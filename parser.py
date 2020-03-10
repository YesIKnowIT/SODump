import logging
import os
import sys
import re
from pathlib import Path
from bs4 import BeautifulSoup

from multiprocessing import JoinableQueue, Process, Semaphore
from utils.pm import ProcessManager

ROOT_DIR = 'archive'


STORE="STORE"
VISIT="VISIT"
DONE="DONE"

class ImpreciseViewCountError(Exception):
    pass

def parser(queue, ctrl):
    def visit(path):
        """ Parse a file with Beautiful Soup and extract data
        """

        def _visit(path):
            with path.open("rt", encoding='utf-8', errors='replace') as f:
                soup = BeautifulSoup(f, 'lxml')

                if path.parts[-2] == 'tagged':
                    return _visit_tagged(soup)
                else:
                    return _visit_question(soup)

        def _visit_tagged(soup):
            questions = soup.find_all('div', attrs={'class': 'question-summary'})
            if questions:
                for question in questions:
                    href = question.find('h3').find('a')['href']
                    date = re.search('/web/([0-9]{14})/', href).group(1)

                    qid = question.get('id')
                    if qid:
                        qid = qid.rsplit('-')[-1]
                    else:
                        qid = re.search('/questions/([0-9]+)/', href).group(1)

                    qid=str(int(qid)) # raise an exception if this is not a numerical id

                    views = question.find('div', attrs={'class':'views'})
                    vc = views = views.get('title') or views.get_text()
                    views = re.search('([0-9]+(,[0-9]{3})*)([k])?\s', views)
                    if views is None:
                        logging.warning("tagged -- Can't understand view count '%s' in %s", vc, path)
                        continue
                    elif views.group(3):
                        logging.warning("tagged -- Imprecise '%s' in %s", vc, path)
                        continue

                    views = int(views.group(1).replace(',',''))

                    tags = question.find_all('a', attrs={'rel': 'tag'})
                    tags = sorted(set([el.get_text() for el in tags]))

                    yield dict(
                        src=path.as_posix(),
                        id=qid,
                        date=date,
                        viewcount=views,
                        tags=tags
                    )

                return

            #raise NotImplementedError

        def _visit_question(soup):
            VIEWED_NNNN_TIMES_RE = re.compile('[Vv]iewed\s+[0-9]+(,[0-9]{3})\s+times?')
            NNNN_TIMES_RE = re.compile('[0-9]+(,[0-9]{3})*\s+times?')
            VIEWED_RE=re.compile('[Vv]iewed')

            def viewcount(soup):
                def asnum(txt):
                    count, suffix = re.search('([0-9]+(?:,[0-9]{3})*)([k]?)', str(txt)).group(1, 2)

                    if suffix:
                        raise ImpreciseViewCountError("View count {}{} is imprecise for {}".format(count, suffix, path))

                    count = count.replace(',','')
                    return int(count)

                # 2019 version
                vc = soup.find('div', attrs={'title': VIEWED_NNNN_TIMES_RE})
                if vc:
                    return asnum(vc.get_text())

                # 2019 version (alternate)
                vc = soup.find('div', attrs={'title': VIEWED_NNNN_TIMES_RE})
                if vc:
                    return asnum(vc.get_text())

                vc = soup.find('div', attrs={'itemprop': 'mainEntity'})
                if vc:
                    vc = vc.find('span', string='Viewed')
                if vc:
                    return asnum(" ".join(vc.parent.strings))

                # 2015 version
                vc = soup.find('div', attrs={'id': 'question-header'})
                if vc:
                    vc = vc.find('div', string=VIEWED_NNNN_TIMES_RE)
                if vc:
                    return asnum(vc.get_text())

                # 2013 version
                vc = soup.find('table', attrs={'id': 'qinfo'})
                if vc:
                    vc = vc.find(string=NNNN_TIMES_RE)
                if vc:
                    return asnum(vc)

                # 2009 version
                vc = soup.find('div', id='sidebar')
                if vc:
                    vc = vc.find('p', attrs={'class': 'label-key'}, string=VIEWED_RE)
                if vc:
                    vc = vc.find_next_sibling('p', string=NNNN_TIMES_RE)
                if vc:
                    return asnum(vc.get_text())

                # 2008 version
                vc = soup.find('div', id='sidebar')
                if vc:
                    vc = vc.find('p', string=VIEWED_RE)
                if vc:
                    vc = vc.find_next_sibling('p', string=NNNN_TIMES_RE)
                if vc:
                    return asnum(vc.get_text())

                # Beta version
                vc = soup.find('div', id='viewcount')
                if vc:
                    vc = vc.b
                if vc:
                    return asnum(vc.get_text())

                # Beta version
                vc = soup.find('div', attrs={'class':'viewcount'})
                if vc:
                    vc = vc.b
                if vc:
                    return asnum(vc.get_text())

                return None

            ATOM_RE = re.compile('/(?P<date>[0-9]{14})/.*/question/(?P<id>[0-9]+)')
            CANONICAL_RE = re.compile('/(?P<date>[0-9]{14})/.*/questions/(?P<id>[0-9]+)')
            OG_URL_RE = re.compile('/(?P<date>[0-9]{14})(?:im_)?/.*/questions/(?P<id>[0-9]+)')

            def coreinfo(soup):
                url = soup.find('link', rel='alternate',type="application/atom+xml")
                if url:
                    m = ATOM_RE.search(url['href'])
                    if m:
                        return m.groupdict()

                url = soup.find('link', rel='canonical')
                if url:
                    m = CANONICAL_RE.search(url['href'])
                    if m:
                        return m.groupdict()

                url = soup.find('meta', dict(name='og:url'))
                if url:
                    m = OG_URL_RE.search(url['content'])
                    if m:
                        return m.groupdict()

                # 2019
                url = soup.find('meta', dict(property='og:url'))
                if url:
                    m = OG_URL_RE.search(url['content'])
                    if m:
                        return m.groupdict()

                return None

            def tags(soup):
                tags = None

                # 2009+
                if tags is None:
                    div = soup.find('div', attrs={'class': 'post-taglist'})
                    if div:
                        tags = soup.find_all('a', attrs={'rel': 'tag'})

                # Public beta
                if tags is None:
                    tags = soup.find_all('a', attrs={'rel': 'tag'})

                return sorted(set([el.get_text() for el in (tags or [])]))


            ci = coreinfo(soup)
            if ci is None:
                logging.warning("coreinfo -- Can't find info for %s", path)
                return

            vc = viewcount(soup)
            if vc is None:
                logging.warning("viewcount -- Can't find view count for %s", path)
                return

            tg = tags(soup)
            if not tg:
                logging.warning("tags -- Can't find tags for %s", path)
                return

            yield dict(
                src=path.as_posix(),
                viewcount=vc,
                tags=tg,
                **ci
            )



        try:
            result = tuple(_visit(path))
            ctrl.put((STORE, path.as_posix(), result))
        except ImpreciseViewCountError as err:
            logging.warning(err)
        except Exception as err:
            logging.error('unexpected -- While processing %s', path)
            logging.error(err, exc_info=True)
            logging.error(err.__traceback__)

    def _run():
        path = queue.get()
        try:
            visit(path)
        finally:
            ctrl.put((DONE,))

    done = False
    while not done:
        try:
            _run()
        except KeyboardInterrupt:
            done = True
        except Exception as err:
            logging.error(type(err))
            logging.error(err, exc_info=True)
            logging.error(err.__traceback__)



def controller(queue, ctrl, sem):
    import sqlite3

    cache = []
    CACHE_MAX_SIZE = 10000

    def _visit(path):
        cursor.execute(DB_SELECT_SOURCE, dict(path=path.as_posix()))
        if not cursor.fetchall():
            queue.put(path)
        else:
            _done()

    def _done():
        sem.release()

    def _commit():
        """ Commit cached changes to the DB
        """
        try:
            cursor.execute("BEGIN DEFERRED TRANSACTION")
            for entry in cache:
                cursor.execute(DB_INSERT_SOURCE, dict(path=entry['path']))
                for item in entry['items']:
                    cursor.execute(DB_INSERT_VIEWCOUNT, dict(
                        question=item['id'],
                        date=item['date'],
                        viewcount=item['viewcount']
                    ))
                    for tag in item['tags']:
                        cursor.execute(DB_INSERT_TAG, dict(
                            question=item['id'],
                            tag=tag
                        ))
            cursor.execute("COMMIT")
            del cache[:]
            print("COMMIT")

        except Exception as e:
            print("ROLLBACK")
            cursor.execute("ROLLBACK")
            raise

    def _store(path, items):
        cache.append(dict(
            path=path,
            items=items
        ))

        if len(cache) > CACHE_MAX_SIZE:
            _commit()

    def _run():
        cmd, *args = ctrl.get()
        print(cmd, *args)
        CMDS[cmd](*args)

    CMDS = {
        VISIT: _visit,
        STORE: _store,
        DONE: _done,
    }


    DB_URI="file:questions.db?mode=rwc"
    DB_TIMEOUT=600
    DB_INIT="""
        CREATE TABLE IF NOT EXISTS sources (
            path TEXT PRIMARY KEY
        );
        CREATE TABLE IF NOT EXISTS tags (
            question INT NOT NULL,
            tag TEXT NOT NULL,
            PRIMARY KEY(question, tag)
        );
        CREATE TABLE IF NOT EXISTS views (
            question INT NOT NULL,
            viewcount INT NOT NULL,
            date TEXT NOT NULL,
            PRIMARY KEY(date, question)
        );
    """
    DB_SELECT_SOURCE="SELECT 1 FROM sources WHERE path = :path"
    DB_INSERT_SOURCE="INSERT OR IGNORE INTO sources(path) VALUES(:path)"
    DB_INSERT_TAG="INSERT OR IGNORE INTO tags(question, tag) VALUES(:question, :tag)"
    DB_INSERT_VIEWCOUNT="INSERT OR IGNORE INTO views(question, date, viewcount) VALUES(:question, :date, :viewcount)"
    db = sqlite3.connect(DB_URI, uri=True, isolation_level=None, timeout=DB_TIMEOUT)
    cursor = db.cursor()

    cursor.executescript(DB_INIT)

    done = False
    while not done:
        try:
            _run()
        except KeyboardInterrupt:
            done = True
        except Exception as err:
            logging.error(type(err))
            logging.error(err, exc_info=True)
            logging.error(err.__traceback__)

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
    QUEUE_LENGTH=10000000

    args = parse_args()

    queue = JoinableQueue()
    ctrl = JoinableQueue()
    sem = Semaphore(QUEUE_LENGTH)

    pm = ProcessManager(
        Process(target=controller, args=(queue, ctrl,sem,)),
        *[Process(target=parser, args=(queue,ctrl,)) for n in range(5)]
    )

    try:
        pm.start()
        for path in args.reader():
            sem.acquire()
            ctrl.put((VISIT,path))

        queue.close()
        queue.join()
    finally:
        pm.terminate()
