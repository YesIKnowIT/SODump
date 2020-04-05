import time
import logging
import subprocess
import traceback
from datetime import datetime, timedelta

from config.constants import *
from utils.db import Db

SECOND = 1
MINUTE = 60*SECOND
HOUR = 60*MINUTE


MSG="""\
\N{ROBOT FACE} Downloading the full #StackOverflow history from the @waybackmachine for {duration} now

At this point I have read {fcount} files
"""
def timedelta_format(td):
    d = td.days
    s = td.seconds
    h, s = divmod(s, HOUR)
    m, s = divmod(s, MINUTE)

    return "{} days, {} hours and {} minutes".format(d, h, m)

db = Db(DB_URI, timeout=DB_TIMEOUT)
while True:
    try:
        orig = datetime(2020, 2, 1, 4, 17)
        delta = datetime.today() - orig
        ts = timedelta_format(delta)
        
        fcount = db.fcount()
        
        msg = MSG.format(duration=ts, fcount=fcount)
        print()
        print(msg)
        TWEET_CMD=['t', 'update', 'XXXX']
        TWEET_CMD[-1] = msg
        subprocess.run(TWEET_CMD)
    except Exception as e:
        logging.error(traceback.format_exc())
    finally:
        time.sleep(12*HOUR)

