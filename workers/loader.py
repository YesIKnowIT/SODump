import os

import requests

from utils.worker import worker
from config.constants import *
from config.commands import *
from utils import Cooldown, notify

def loader(ctrl, queue):
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

    cooldown = Cooldown()

    def load(path, url):
        if cooldown.wait():
            stats['sleep'] += 1

        notify("DOWNLD", url)
        retry = False
        try:
            r = requests.get(
                url,
                headers = { 'user-agent': REQUESTS_USER_AGENT, },
                timeout=REQUESTS_TIMEOUT
            )
            stats['download'] += 1
            if r.status_code != 200:
                notify("STATUS", r.status_code)
                retry = True

            if r.status_code == 429 or r.status_code >= 500:
                # Too many requests or server error
                cooldown.set()
            else:
                cooldown.clear()


        except requests.Timeout:
            notify("TIMEOUT", url)
            retry = True
            cooldown.set()
            stats['timeout'] += 1
        except requests.exceptions.ConnectionError:
            # Connection refused?
            notify("CONNERR", url)
            retry = True
            cooldown.set()
            stats['connerr'] += 1

        if retry:
            # Retry later
            notify("RETRY", url)
            ctrl.put((RETRY, path, url))
        else:
            notify("PARSE", path)
            ctrl.put((PARSE,path,r.text))
            notify("DONE")
            ctrl.put((DONE,path))


    def _run():
        path,url = queue.get()
        load(path, url)

    return worker(_run, "loader", stats)

