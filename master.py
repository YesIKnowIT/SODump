import sys
from pathlib import Path
from multiprocessing import Process, Queue, SimpleQueue, JoinableQueue, Lock, Semaphore
from utils.pm import ProcessManager

from utils import notify
from utils.db import Db
from utils.worker import worker
from workers.cdx import cdx
from workers.loader  import loader
from workers.parser  import parser
from utils.constants import *
from utils.commands import *

def controller(ctrl, loader_queue, parser_queue, sem):
    db = Db(DB_URI)
    pending = {}
    cache = []
    stats = {
       'ttl': [0]*MAX_RETRY,
       'skip': 0,
       'commit': 0,
       'store': 0,
    }

    def _load(path, url):
        key = path
        if key not in pending:
            if not db.exists(key):
                _retry(path, url)
                return
            else:
                stats['skip'] += 1

        sem.release()

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
        notify('DEBUG', sem.get_value(), len(pending), loader_queue.qsize(), parser_queue.qsize())
        cmd, *args = ctrl.get()
        CMDS[cmd](*args)

    def _parse(path, text):
        parser_queue.put((path, text))

    def _store(path, items):
        notify('STORE', path)
        cache.append(dict(
            path=path,
            items=items
        ))
        stats['store'] += 1

        if len(cache) > CACHE_MAX_SIZE:
            _commit()

    def _commit():
        """ Commit cached changes to the DB
        """
        db.write(cache)
        stats['commit'] += 1

    CMDS = {
        DONE: _done,
        LOAD: _load,
        PARSE: _parse,
        RETRY: _retry,
        STORE: _store,
        UNLOCK: _unlock,
    }

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
    ctrl = SimpleQueue()
    sem = Semaphore(100)

    pm = ProcessManager(
        Process(target=controller, args=(ctrl, loader_queue, parser_queue, sem)),
        Process(target=cdx, args=(ctrl, sem, URL_PREFIX)),
        *[Process(target=loader, args=(ctrl,loader_queue)) for n in range(LOADER_PROCESS_COUNT)],
        *[Process(target=parser, args=(ctrl,parser_queue)) for n in range(PARSER_PROCESS_COUNT)],
    )

    try:
        pm.start()

        pm[0].join()
    finally:
        pm.terminate()
