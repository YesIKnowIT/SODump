import logging
import signal
import os
import time
import sys
import os.path
import re
import urllib.parse
import queue
from multiprocessing import Process, Queue, JoinableQueue

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

def worker(queue):
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
            queue.put(capture)
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
            queue.task_done()
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

def cdx(queue, url):
    """ Query the CDX index to retrieve all captures for the `url` prefix
    """

    stats = {
        'run': 0,
        'error': 0,
        'push': 0
    }

    done = False
    params = dict(
        output='json',
        url=url,
        matchType='prefix',
        limit=1000,
        showResumeKey='true',
        resumeKey=None
    )

    def _run():
        r = requests.get(CDX_API_ENDPOINT, params=params)
        notify("CDX", r.status_code)
        if r.status_code != 200:
            time.sleep(REQUESTS_COOLDOWN)
            return

        # else
        result = r.json()

        if result[-2:-1] == [[]]:
            rk = urllib.parse.unquote_plus(result[-1][0])
            notify("RK", rk)
            result = result[:-2]
        else:
            rk = None
            nonlocal done
            done = True

        if result:
            keys = result[0]
            for item in result[1:]:
                item = {
                    k: v for k,v in zip(keys, item)
                }

                if item.get("statuscode") == "200":
                    notify("PUSH", item['timestamp'], item['original'])
                    stats['push'] += 1
                    queue.put(item)

        params['resumeKey'] = rk


    while not done:
        stats['run'] += 1
        if not stats['run'] % 1000:
            notify("STATS", stats)

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
    workers = [
        Process(target=cdx, args=(queue, URL_PREFIX))
    ]
    workers += [Process(target=worker, args=(queue,)) for _ in range(WORKERS)]

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
