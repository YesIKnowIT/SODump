import logging
import re

from bs4 import BeautifulSoup

from utils.worker import worker
from utils.commands import *
from utils.constants import *

class ParserError(Exception):
    pass

class ImpreciseViewCountError(ParserError):
    pass

def parser(ctrl, queue):
    stats = {}

    def visit(path, text):
        """ Parse a file with Beautiful Soup and extract data
        """

        def _visit():
            soup = BeautifulSoup(text, 'lxml')

            try:
                if '/tagged/' in path:
                    yield from tuple(_visit_tagged(soup))
                else:
                    yield from tuple(_visit_question(soup))

            except ParserError as e:
                notify('ERROR', e)
                yield dict(src=path, status=PARSER_ERROR)

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
                        src=path,
                        status=PARSER_OK,
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
                src=path,
                status=PARSER_OK,
                viewcount=vc,
                tags=tg,
                **ci
            )



        try:
            result = tuple(_visit())
            ctrl.put((STORE, path, result))
        except ImpreciseViewCountError as err:
            logging.warning(err)

    def _run():
        path, text = queue.get()
        visit(path, text)

    return worker(_run, "parser", stats)
