import logging
import urllib
import re
import os.path

import requests

from utils import Cooldown, notify
from utils.worker import worker
from config.constants import *
from config.commands import *

PATH_FMT="{timestamp}/{original}"
WAYBACK_URL_FMT="https://web.archive.org/web/{timestamp}/{original}"
def capturetopath(capture):
    url = WAYBACK_URL_FMT.format_map(capture)
    path = PATH_FMT.format_map(capture)

    # Sanitize path
    path = path.replace('/?', '?')
    path = re.sub(r':80/', '/', path)

    # Skip the protocol at the start of the URL but also in anywhere in the URL because
    # some redirections in the Wayback Machine embeds the protocol after the date
    items = [part for part in path.split('/') if part not in ('http:', 'https:')]
    path = os.path.normpath(os.path.join(*items))

    return (path, url)

def cdx(ctrl, queue, sem, url):
    """ Query the CDX index to retrieve all captures for the `url` prefix
    """

    stats = {
        'run': 0,
        'error': 0,
        'push': 0,
        'timeout': 0,
        'connerr': 0,
    }

    cooldown = Cooldown()
    params = dict(
        url=url,
        matchType='prefix',
        limit=CDX_LIMIT,
        showResumeKey='true',
        resumeKey=None
    )

    fields = ('timestamp', 'original', 'statuscode')
    params['fl'] = ",".join(fields)

    def _next():
        resumeKey = queue.get()
        cooldown.wait()

        try:
            params['resumeKey'] = resumeKey
            r = requests.get(CDX_API_ENDPOINT,
                    timeout=REQUESTS_TIMEOUT,
                    headers = {
                        'user-agent':REQUESTS_USER_AGENT,
                    },
                    stream=True,
                    params=params)
            notify("CDX", r.status_code)
            if r.status_code != 200:
                cooldown.set()
                return False

            resumeKey = None
            count = 0
            items = []
            for line in r.iter_lines():
                count += 1
                if not line:
                    # ignore empty lines
                    continue

                line = line.decode('utf-8')
                item = line.split()
                if len(item) == 1:
                    # TEST ONLY
                    # break
                    # END TEST ONLY

                    # resume key
                    resumeKey = urllib.parse.unquote_plus(item[0])
                    notify('DEBUG', resumeKey)
                    break

                # else
                item = {
                    k: v for k,v in zip(fields, item)
                }
                if item.get("statuscode") == "200":
                    items.append(item)
        finally:
            ctrl.put((CDX, resumeKey))

        for item in items:
                # notify("PUSH", item['timestamp'], item['original'])
                stats['push'] += 1
                sem.acquire()
                ctrl.put((CHECK,*capturetopath(item)))

        cooldown.clear()
        notify('DEBUG', count)

        return count == 0

    def _run():
        try:
            return _next()
        except requests.Timeout:
            notify("TIMEOUT", params['resumeKey'])
            stats['timeout'] += 1
            cooldown.set(1)
        except requests.exceptions.ConnectionError:
            notify("CONNERR", params['resumeKey'])
            stats['connerr'] += 1
            cooldown.set(1)
        except BrokenPipeError:
            return True
        finally:
            ctrl.put((UNLOCK,))

    return worker(_run, "cdx", stats)
