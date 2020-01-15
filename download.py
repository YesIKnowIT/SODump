import logging
import signal
import os
import sys
import os.path
import re
import urllib.parse
from multiprocessing import Process, JoinableQueue



# Commands
LOAD = "LOAD"

def notify(code, *args):
    print("{:6d} {:8s}".format(os.getpid(), code), *args)
    sys.stdout.flush()

def run(queue):
    """ Load an URL and push back links to the queue
    """
    import requests
    from bs4 import BeautifulSoup

    cache = set() # This should be shared among process but overhead shouldn't be that high

    def urltopath(url):
        url = url.replace('/?', '?')
        if url.endswith('/'):
            url = url[:-1:] + '.index'

        items = url.split('/')
        if items[0] in ('http','https'):
            del item[0]

        return os.path.normpath(os.path.join(*items))

    def load(url, ttl=5):
        if ttl <= 0:
            return

        if url in cache:
            return # this process has already handled that url

        path = urltopath(url)
        notify("DEST", path)
        try:
            os.makedirs(os.path.dirname(path), exist_ok=True)
            with open(path, 'xb') as dest:
                notify("DOWNLD", url)
                r = requests.get(url)
                if r.status_code != 200:
                    notify("STATUS", r.status_code, url)
                    # Retry later
                    dest.close()
                    os.unlink(path)
                    queue.put((LOAD, url, ttl-1), False)
                    return
                    
                notify("WRITE", path)
                dest.write(r.content)
        except FileExistsError:
            notify("EXIST", path)
            pass

        parse(url, path)

    ACCEPT_RE = re.compile('/[0-9]+/.*//stackoverflow.com/questions/')

    def parse(url, path):
        notify("PARSE", path)
        with open(path, "rt", encoding='utf-8', errors='replace') as f:
            soup = BeautifulSoup(f, 'lxml')

            # TODO override `base` with base specified in the html document
            base = url

            for link in soup.find_all('a'):
                href = link.get('href')
                href = urllib.parse.urljoin(base, href)
                if ACCEPT_RE.search(href):
                    queue.put((LOAD, href), False)

        cache.add(url)

    def _run(queue):
        (cmd, *args) = queue.get()
        notify(cmd, *args)
        try:
            if cmd == LOAD:
                load(*args)
            elif cmd == PARSE:
                parse(*args)
            else:
                raise Exception("Bad command: %s%s", cmd, args)
        finally:
            notify("DONE")
            queue.task_done()

    while True:
        try:
            _run(queue)
        except Exception as err:
            notify('ERROR', err)
            logging.error(err, exc_info=True)
            logging.error(err.__traceback__)

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
    
    queue = JoinableQueue(0)
    for url in ROOTS:
        queue.put((LOAD, url), False)

    workers = [Process(target=run, args=(queue,)) for _ in range(WORKERS)]

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

    queue.join()

