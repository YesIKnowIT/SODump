import logging
import signal
import os
import time
import sys
import os.path
import re
import urllib.parse
import queue
from multiprocessing import Process, Queue, JoinableQueue, Lock

import requests

# Number of worker processes
#
# You can be relatively aggressive here since most requests
# to the wayback machine will either ends as a 404 or 302
# response. And if we hammer the server to hard, it will reply
# with a 429, putting one or several of our download processes
# to sleep
WORKERS = 16

# Commands
UNLOCK="UNLOCK"
LOAD = "LOAD"
FOLLOW="FOLLOW"
RETRY = "RETRY"
DONE = "DONE"

REQUESTS_CONNECT_TIMEOUT=3.5
REQUESTS_READ_TIMEOUT=10
REQUESTS_TIMEOUT=(REQUESTS_CONNECT_TIMEOUT,REQUESTS_READ_TIMEOUT)
REQUESTS_COOLDOWN=15
MAX_RETRY=5

def notify(code, *args):
    print("{:6d} {:8s}".format(os.getpid(), code), *args)
    sys.stdout.flush()

PATH_FMT="archive/{timestamp:.4}/{timestamp:.6}/{timestamp:.8}/{timestamp}/{original}"
WAYBACK_URL_FMT="https://web.archive.org/web/{timestamp}/{original}"
def capturetopath(capture):
    url = WAYBACK_URL_FMT.format_map(capture)
    path = PATH_FMT.format_map(capture)

    # Sanitize path
    path = path.replace('/?', '?')
    if path.endswith('/'):
        path = path[:-1:] + '.index'
    path = re.sub(r':80/', '/', path)

    # Skip the protocol at the start of the URL but also in anywhere in the URL because
    # some redirections in the Wayback Machine embeds the protocol after the date
    items = [part for part in path.split('/') if part not in ('http:', 'https:')]
    path = os.path.normpath(os.path.join(*items))

    return (path, url)

def worker(ctrl, queue):
    """ Load an URL and push back links to the queue
    """
    stats = dict(
        sleep=0,
        redirect=0,
        run=0,
        download=0,
        write=0,
        error=0,
        timeout=0,
        connerr=0
    )

    # Cooldown delay in seconds between server requests
    cooldown = 0

    def load(capture):
        nonlocal cooldown
        path, url = capturetopath(capture)

        try:
            os.makedirs(os.path.dirname(path), exist_ok=True)
            stat = os.stat(path)
            notify("STAT", stat)
            if stat.st_size != 0:
                notify("EXISTS", path)
                return

            # else remove the empty file
            os.unlink(path)

        except FileNotFoundError:
            pass # That was expected

        if cooldown > 0:
            notify("SLEEP", cooldown)
            stats['sleep'] += 1
            time.sleep(cooldown)
            cooldown = cooldown // 2

        notify("DOWNLD", url)
        retry = False
        try:
            r = requests.get(url, timeout=REQUESTS_TIMEOUT)
            stats['download'] += 1
            if r.status_code != 200:
                notify("STATUS", r.status_code)
                retry = True

            if r.status_code == 429 or r.status_code >= 500:
                # Too many requests
                # As a quick workaround, just delay the
                # next downloads from this worker
                cooldown = REQUESTS_COOLDOWN

        except requests.Timeout:
            notify("TIMEOUT", url)
            retry = True
            cooldown = REQUESTS_COOLDOWN
            stats['timeout'] += 1
        except requests.exceptions.ConnectionError:
            # Connection refused?
            notify("CONNERR", url)
            retry = True
            cooldown = REQUESTS_COOLDOWN
            stats['connerr'] += 1

        if retry:
            # Retry later
            notify("RETRY", url)
            ctrl.put((RETRY, capture))
            return

        # Store file
        try:
            with open(path, 'xb') as dest:
                notify("WRITE", path)
                dest.write(r.content)
                stats['write'] += 1
        except NotADirectoryError:
            pass
        except FileExistsError:
            notify("DUP", path)
            # A concurrent process has likely downloaded the file
            # unlink and retry to be sure
            os.unlink(path)
            ctrl.put((RETRY, capture))
            pass
        except:
            notify("UNLINK", path)
            os.unlink(path)
            raise


    def _run():
        stats['run'] += 1
        if not stats['run'] % 100:
            notify("STATS", stats)

        capture = queue.get()

        try:
            load(capture)
        finally:
            ctrl.put((DONE,capture))
            notify("DONE")

    while True:
        try:
            _run()
        except Exception as err:
            notify('ERROR', type(err))
            notify('ERROR', err)
            logging.error(type(err))
            logging.error(err, exc_info=True)
            logging.error(err.__traceback__)
            stats['error'] += 1

CDX_API_ENDPOINT="http://web.archive.org/cdx/search/cdx"

def cdx(lck, ctrl, url):
    """ Query the CDX index to retrieve all captures for the `url` prefix
    """

    stats = {
        'run': 0,
        'error': 0,
        'push': 0
    }

    done = False
    params = dict(
        url=url,
        matchType='prefix',
        limit=2000,
        showResumeKey='true',
        resumeKey=None
    )

    fields = ('timestamp', 'original', 'statuscode')
    params['fl'] = ",".join(fields)

    def _run():
        r = requests.get(CDX_API_ENDPOINT, 
            timeout=REQUESTS_TIMEOUT,
            stream=True,
            params=params)
        notify("CDX", r.status_code)
        if r.status_code != 200:
            time.sleep(REQUESTS_COOLDOWN)
            return False

        count = 0
        for line in r.iter_lines():
            if not line:
                # ignore empty lines
                continue

            count += 1
            line = line.decode('utf-8')
            item = line.split()
            if len(item) == 1:
                # resume key
                params['resumeKey'] = urllib.parse.unquote_plus(item[0])
                break

            # else
            item = {
                k: v for k,v in zip(fields, item)
            }
            if item.get("statuscode") == "200":
                notify("PUSH", item['timestamp'], item['original'])
                stats['push'] += 1
                ctrl.put((LOAD,item))

        return count == 0

    while not done:
        stats['run'] += 1
        if not stats['run'] % 1000:
            notify("STATS", stats)

        try:
            lck.acquire()
            done = _run()
        except Exception as err:
            notify('ERROR', type(err))
            notify('ERROR', err)
            logging.error(type(err))
            logging.error(err, exc_info=True)
            logging.error(err.__traceback__)
            stats['error'] += 1
        finally:
            ctrl.put((UNLOCK,))

    notify('QUIT')


def controller(lck, ctrl, queue):
    stats = {
        'run': 0,
        'error': 0,
        'ttl': [0]*MAX_RETRY
    }

    pending = {}
    # Keep track of entries queued, but not downloaded yet

    def load(data):
        key = frozenset(data.items()) # dict are not hashable
        if key not in pending:
            retry(data)

    def retry(data):
        key = frozenset(data.items()) # dict are not hashable
        ttl = pending.get(key, MAX_RETRY)
        ttl -= 1
        stats['ttl'][ttl] += 1

        if ttl == 0:
            del pending[key]
        else:
            pending[key] = ttl
            queue.put(data)

    def done(data):
        key = frozenset(data.items()) # dict are not hashable
        pending.pop(key, None)

        queue.task_done()

    def unlock():
        lck.release()

    CMDS = {
        UNLOCK: unlock,
        LOAD: load,
        RETRY: retry,
        DONE: done
    }

    def _run():
        stats['run'] += 1
        if not stats['run'] % 1000:
            stats['pending'] = len(pending)
            notify("STATS", stats)

        (cmd, *args) = ctrl.get()
        CMDS[cmd](*args)

    while True:
        try:
            _run()
        except Exception as err:
            notify('ERROR', type(err))
            notify('ERROR', err)
            logging.error(type(err))
            logging.error(err, exc_info=True)
            logging.error(err.__traceback__)
            stats['error'] += 1

    queue.join()

if __name__ == '__main__':
    URL_PREFIX = 'http://stackoverflow.com/questions/'
    MAX_QUEUE_LENGTH = 10000

    queue = JoinableQueue(MAX_QUEUE_LENGTH)
    ctrl = Queue(0)
    lck = Lock()

    workers = [
        Process(target=cdx, args=(lck, ctrl, URL_PREFIX)),
        Process(target=controller, args=(lck, ctrl, queue))
    ]
    workers += [Process(target=worker, args=(ctrl, queue)) for _ in range(WORKERS)]

    sigint = signal.getsignal(signal.SIGINT)
    def killall(*args):
        for worker in workers:
            worker.terminate()
        sigint(*args)

    signal.signal(signal.SIGINT, killall)


    for worker in workers:
        worker.start()
        pid = worker.pid
        notify("START", "worker", pid)

    workers[0].join()
    notify("EOF")
