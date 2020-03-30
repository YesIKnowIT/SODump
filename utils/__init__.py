import sys
import os
import time

from utils.constants import *

def notify(code, *args):
    print("{:6d} {:8s}".format(os.getpid(), code), *args)
    sys.stdout.flush()

class Cooldown:
    def __init__(self):
        self.cooldown = 0
        self.end = time.time()

    def set(self, value=REQUESTS_COOLDOWN):
        self.cooldown = max(self.cooldown, value)
        self.end = self.cooldown + time.time()

    def wait(self):
        delay = int(self.end - time.time())

        if delay > 0:
            notify("SLEEP", delay)
            time.sleep(delay)
            self.cooldown = min(self.cooldown*2, MAX_SLEEP_TIME)
            return True

        return False

    def clear(self):
        self.cooldown = 0
        self.end = time.time()
