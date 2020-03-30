import logging

from utils import notify

def worker(fct, name, stats):
    done = False
    stats['run'] = 0
    stats['error'] = 0

    notify('START', name)
    while not done:
        stats['run'] += 1
        if not stats['run'] % 1000:
            notify("STATS", stats)

        try:
            done = fct()
        except BrokenPipeError:
            done = True
        except Exception as err:
            notify('ERROR', type(err))
            notify('ERROR', err)
            logging.error(type(err))
            logging.error(err, exc_info=True)
            logging.error(err.__traceback__)
            stats['error'] += 1

    notify('EXIT', name)
