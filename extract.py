import os
import sys
import csv
import time

from collections import defaultdict

from utils.db import Db
from config.constants import *

if __name__ == "__main__":
    manifest=os.path.join(EXTRACT_DIR, EXTRACT_MANIFEST)
    db = Db(DB_URI, timeout=DB_TIMEOUT)

    stats={
        'date': time.strftime('%Y-%m-%dT%H:%M:%SZ', time.gmtime()),
        'by-year': defaultdict(lambda: dict(count=0, filename=None, file=None)),
    }

    def writer():
        def _writer(year):
            entry = stats['by-year'][year]
            entry['filename'] = filename = str(year)+'.csv'

            filepath=os.path.join(EXTRACT_DIR, filename)
            entry['file'] = file = open(filepath, 'wt')
            output = csv.writer(file)
                
            def _write(row):
                date, *tail = row
                new_year = date[0:4]
                
                if new_year != year:
                    file.close()
                    return _writer(new_year)(row)


                entry['count'] +=1
                output.writerow(row)

                return _write
            return _write
        
        def _first(row):
            date, *tail = row
            year = date[0:4]

            return _writer(year)(row)
            
        return _first;


    db.forEachQuestion(writer())

    with open(manifest, "wt") as  f:
        f.write('{date}\n'.format(date=stats['date']))
        f.write('\n')

        byyear = stats['by-year']
        for year in sorted(byyear.keys()):
            entry = stats['by-year'][year]

            entry['file'].close()
            f.write('{filename}: {count:10d}\n'.format(year=year, **entry))

