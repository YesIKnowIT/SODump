import sys
from pathlib import Path
from multiprocessing import Process, Queue, SimpleQueue, JoinableQueue, Lock, Semaphore
from utils.pm import ProcessManager

from utils import notify
from utils.db import Db
from utils.worker import worker
from workers.db import db
from workers.cdx import cdx
from workers.loader  import loader
from workers.parser  import parser
from config.constants import *
from config.commands import *

def controller(ctrl, db_queue, cdx_queue, loader_queue, parser_queue, sem):
    pending = {}
    cache = []
    stats = {
       'ttl': [0]*MAX_RETRY,
       'check': 0,
       'commit': 0,
       'store': 0,
    }

    def _check(path, url):
        key = path
        if key not in pending:
            stats['check'] += 1
            db_queue.put((CHECK, path, url))
        else:
            _discard(path, url)

    def _discard(path, url):
        sem.release()

    def _load(path, url):
        _retry(path, url)

    def _retry(path, url):
        key = path
        ttl = pending.get(key, MAX_RETRY)
        ttl -= 1
        stats['ttl'][ttl] += 1

        if ttl == 0:
            del pending[key]
            sem.release()
        else:
            pending[key] = ttl
            loader_queue.put((path,url))

    def _done(path):
        key = path
        pending.pop(key, None)
        sem.release()

    def _unlock():
        pass

    def _run():
        # notify('DEBUG', sem.get_value(), len(pending), loader_queue.qsize(), parser_queue.qsize())
        cmd, *args = ctrl.get()
        # notify('DO', cmd, *[arg[:10] for arg in args])
        CMDS[cmd](*args)

    def _cdx(resumeKey):
        notify('CDX', resumeKey)
        cdx_queue.put(resumeKey)
        notify('CDX', 'done')

    def _parse(path, text):
        parser_queue.put((path, text))

    def _store(path, status,  items=()):
        notify('STORE', path)
        cache.append((path, status, items))
        stats['store'] += 1

        if len(cache) > CACHE_MAX_SIZE:
            _commit()

    def _commit():
        db_queue.put((COMMIT, cache))
        cache = []
        stats['commit'] += 1

    CMDS = {
        CDX: _cdx,
        CHECK: _check,
        DISCARD: _discard,
        DONE: _done,
        LOAD: _load,
        PARSE: _parse,
        RETRY: _retry,
        STORE: _store,
        UNLOCK: _unlock,
    }

    cdx_queue.put(None)
    worker(_run, "controller", stats)
    _commit()


def stdin():
    for line in sys.stdin:
        yield Path(line.rstrip())

def glob():
    for path in Path(ROOT_DIR).glob('**/questions/*/*'):
        if path.is_file():
            yield path

def parse_args():
    import argparse
    parser = argparse.ArgumentParser()

    parser.add_argument("--stdin", help="Read path from stdin",
            dest='reader',
            default=glob, action='store_const', const=stdin)


    args = parser.parse_args()
    return args

if __name__ == '__main__':
    args = parse_args()

    loader_queue = Queue()
    parser_queue = Queue()
    cdx_queue = Queue()
    db_queue = Queue()
    ctrl = Queue()
    sem = Semaphore(QUEUE_LENGTH)

    pm = ProcessManager(
        Process(target=controller, args=(ctrl, db_queue, cdx_queue, loader_queue, parser_queue, sem)),
        Process(target=db, args=(ctrl, db_queue)),
        *[Process(target=cdx, args=(ctrl,cdx_queue, sem, URL_PREFIX)) for n in range(CDX_PROCESS_COUNT)],
        *[Process(target=loader, args=(ctrl,loader_queue)) for n in range(LOADER_PROCESS_COUNT)],
        *[Process(target=parser, args=(ctrl,parser_queue)) for n in range(PARSER_PROCESS_COUNT)],
    )

    try:
        pm.start()

        pm[0].join()
    finally:
        pm.terminate()
