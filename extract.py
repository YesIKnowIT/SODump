import sys
import csv

from utils.db import Db
from config.constants import *

if __name__ == "__main__":
    db = Db(DB_URI, timeout=DB_TIMEOUT)
    write = csv.writer(sys.stdout).writerow

    db.forEachQuestion(write)

