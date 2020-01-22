import logging
import os
import re
from bs4 import BeautifulSoup

from multiprocessing import Pool

def foreach_question(action, *, path=os.path.join('https:','web.archive.org', 'web')):
    """ Walk through an archive dump.
    """
    queue = [ path ]

    def find(path):
        """ Find question files
        """
        for entry in os.scandir(path):
            if entry.is_dir():

                try:
                    qid = int(entry.name)
                except ValueError:
                    # Not a question directory. Ignore this entry
                    continue

                qfiles = []
                for root, dirs, files in os.walk(entry.path):
                    qfiles += [os.path.join(root, f) for f in files]

                if len(qfiles) == 1:
                    action(qfiles[0])


    def walk(path):
        """ Recursively walk down the directory tree
        """
        for entry in os.scandir(path):
            if entry.is_dir():
                if entry.name == 'questions':
                    # this is a question directory
                    find(entry.path)
                else:
                    queue.append(entry.path)

    while queue:
        top = queue.pop()
        walk(top)


VIEWED_NNNN_TIMES_RE = re.compile('[V]iewed\s+[0-9]+(,[0-9]{3})*\s+times?')
NNNN_TIMES_RE = re.compile('[0-9]+(,[0-9]{3})* times?')
VIEWED_RE=re.compile('[Vv]iewed')

def viewcount(soup):
    def asnum(txt):
        count = re.search('[0-9]+(?:,[0-9]{3})*', str(txt)).group(0)
        return int(count.replace(',',''))

    # 2019 version
    vc = soup.find('div', attrs={'title': VIEWED_NNNN_TIMES_RE})
    if vc:
        return asnum(vc.get_text())

    # 2019 version
    vc = soup.find('div', attrs={'title': VIEWED_NNNN_TIMES_RE})
    if vc:
        return asnum(vc.get_text())

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

def visit(path):
    """ Parse a file with Beautiful Soup and extract data
    """

    def _visit(path):
        with open(path, "rt", encoding='utf-8', errors='replace') as f:
            soup = BeautifulSoup(f, 'lxml')

            # Ignore redirects
            if soup.find('meta', attrs={'http-equiv':'Refresh'}):
                return


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

            return dict(
                src=path,
                viewcount=vc,
                tags=tg,
                **ci
            )

    try:
        return _visit(path)
    except Exception as err:
        logging.error('unexpected -- While processing %s', path)
        logging.error(err, exc_info=True)
        logging.error(err.__traceback__)


if __name__ == '__main__':
    cache = set()
    with Pool(5) as pool:
        def write(item):
            if item is None:
                return

            key = (item['id'], item['date'], *item['tags'])
            if key in cache:
                return

            cache.add(key)
            print(','.join((
                item['id'], item['date'], str(item['viewcount']), *item['tags']
            )))

        def push(path):
            pool.apply_async(visit, (path,), callback=write)


        try:
            foreach_question(push)
            pool.close()
            pool.join()
        except:
            pool.terminate()
