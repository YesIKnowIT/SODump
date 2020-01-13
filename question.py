import logging
import os
from bs4 import BeautifulSoup


def foreach_question(action, *, path=os.path.join('web.archive.org', 'web')):
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

                if len(qfiles) != 1:
                    logging.warning("Ignoring %s (contains %d files)", entry.path, len(qfiles))
                else:
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

foreach_question(print)
