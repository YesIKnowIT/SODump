import logging
import signal
import os
import time
import sys
import os.path
import re
import urllib.parse
from multiprocessing import Process, Queue, JoinableQueue



# Commands
LOAD = "LOAD"
RETRY = "RETRY"
DONE = "DONE"

REQUESTS_CONNECT_TIMEOUT=3.5
REQUESTS_READ_TIMEOUT=5
REQUESTS_TIMEOUT=(REQUESTS_CONNECT_TIMEOUT,REQUESTS_READ_TIMEOUT)
REQUESTS_COOLDOWN=15
MAX_RETRY=5

def notify(code, *args):
    print("{:6d} {:8s}".format(os.getpid(), code), *args)
    sys.stdout.flush()

def worker(urls, ctrl):
    """ Load an URL and push back links to the queue
    """
    import requests
    from bs4 import BeautifulSoup

    stats = dict(
        redirect=0,
        run=0,
        download=0,
        write=0,
        parse=0,
        error=0,
        timeout=0,
        connerr=0
    )

    # Cooldown delay in seconds between server requests
    cooldown = 0

    def urltopath(url):
        url = url.replace('/?', '?')
        if url.endswith('/'):
            url = url[:-1:] + '.index'

        items = url.split('/')
        if items[0] in ('http:','https:'):
            del items[0]

        return os.path.normpath(os.path.join(*items))

    ACCEPT_RE = re.compile('/[0-9]+/.*//stackoverflow.com/questions/')

    def accept(url):
        return ACCEPT_RE.search(url)

    def load(url):
        nonlocal cooldown

        path = urltopath(url)
        notify("DEST", path)

        download = True
        try:
            os.makedirs(os.path.dirname(path), exist_ok=True)
            fstat = os.stat(path)
            if fstat.st_size > 0:
                download = False
        except FileExistsError:
            # mkdir with a file of the same name. What do do?
            download = False
        except FileNotFoundError:
            pass # That was expected

        if download:
            if cooldown > 0:
                notify("SLEEP", cooldown)
                time.sleep(cooldown)
                cooldown = cooldown // 2

            notify("DOWNLD", url)
            retry = False
            try:
                r = requests.get(url, allow_redirects=False, timeout=REQUESTS_TIMEOUT)
                stats['download'] += 1
                if r.status_code != 200:
                    notify("STATUS", r.status_code)
                    retry = True

                if r.status_code == 404:
                    return
                elif r.status_code in (301, 302, 303, 307):
                    location = r.headers['Location']
                    notify("REDIRECT", url, location)
                    stats['redirect'] += 1
                    if accept(location):
                        ctrl.put((LOAD, location,), False)
                    return
                elif r.status_code == 429:
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
                ctrl.put((RETRY, url,), False)
                return

            try:
                with open(path, 'xb') as dest:
                    notify("WRITE", path)
                    dest.write(r.content)
                    stats['write'] += 1
            except FileExistsError:
                # A concurrent process has likely downloaded the file
                pass
            except:
                os.unlink(path)
                raise

        parse(url, path)

    def parse(url, path):
        notify("PARSE", path)
        stats['parse'] += 1
        with open(path, "rt", encoding='utf-8', errors='replace') as f:
            soup = BeautifulSoup(f, 'lxml')

            # TODO override `base` with base specified in the html document
            base = url

            for link in soup.find_all('a'):
                href = link.get('href')
                (href, _) = urllib.parse.urldefrag(href)
                href = urllib.parse.urljoin(base, href)
                if accept(href):
                    ctrl.put((LOAD, href), False)

    def _run():
        if not stats['run'] % 100:
            notify("STATS", stats)

        url = urls.get()
        try:
            stats['run'] += 1
            load(url)
        finally:
            ctrl.put((DONE,), False)
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

def controller(urls, ctrl):
    TTL = 5

    cache = {}
    stats = {
        'ttl': [0]*(TTL+1),
        'run': 0,
        'error': 0,
        'drop': 0
    }

    def load(url):
        if url not in cache:
            cache[url] = TTL
            stats['ttl'][TTL] += 1
            urls.put(url, False)

    def retry(url):
        ttl = cache.setdefault(url, TTL+1)
        if ttl > 0:
            ttl -= 1
            cache[url] = ttl
            stats['ttl'][ttl] += 1
            urls.put(url, False)
        else:
            stats['drop'] += 1

    def done():
        urls.task_done()

    CMDS = {
        LOAD: load,
        RETRY: retry,
        DONE: done
    }

    def _run():
        if not stats['run'] % 5000:
            notify("STATS", stats)

        (cmd, *args) = ctrl.get()
        stats['run'] += 1
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

if __name__ == '__main__':
    WORKERS = 8
    ROOTS = (
        'https://web.archive.org/web/20091115094944/http://stackoverflow.com/',
        'https://web.archive.org/web/20101229025739/http://stackoverflow.com/',
        'https://web.archive.org/web/20111231100355/http://stackoverflow.com/',
        'https://web.archive.org/web/20121231231845/http://stackoverflow.com/',
        'https://web.archive.org/web/20131231220201/http://stackoverflow.com/',
        'https://web.archive.org/web/20141231194453/http://stackoverflow.com/',
        'https://web.archive.org/web/20151231215204/http://stackoverflow.com/',
        'https://web.archive.org/web/20161231124253/http://stackoverflow.com/',
        'https://web.archive.org/web/20171231143703/https://stackoverflow.com/',
        'https://web.archive.org/web/20181231223103/https://stackoverflow.com/',
        'https://web.archive.org/web/20191231230732/https://stackoverflow.com/'
    )

    urls = JoinableQueue(0)
    ctrl = Queue(0)
    for url in ROOTS:
        urls.put(url, False)

    workers = [
        Process(target=controller, args=(urls, ctrl))
    ]
    workers += [Process(target=worker, args=(urls, ctrl)) for _ in range(WORKERS)]

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

    urls.join()
    notify("EOF")
