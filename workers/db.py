import logging
import urllib
import re
import os.path

import requests

from utils import notify
from utils.worker import worker
from utils.db import Db
from config.constants import *
from config.commands import *


def db(ctrl, queue):
    db = Db(DB_URI, mode='rwc', timeout=DB_TIMEOUT)
    notify('DB', 'up')
    stats = {}

    def _commit(cache):
        db.write(cache)

    def _check(path, url):
        if not db.exists(path):
            ctrl.put((LOAD, path, url))
        else:
            ctrl.put((DISCARD, path, url))

    commands = {
        CHECK: _check,
        COMMIT: _commit,
    }

    def _run():
        cmd, *args = queue.get()
        commands[cmd](*args)

    return worker(_run, "db", stats)
