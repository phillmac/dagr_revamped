import logging
import sys
import threading
from logging.handlers import RotatingFileHandler
from pathlib import Path

__logging_ready = threading.Event()
__buffered_records = {}


def logging_ready():
    return __logging_ready.is_set()


def buffer_record(lname, record):
    if not lname in __buffered_records:
        __buffered_records[lname] = []
    __buffered_records[lname].append(record)


def log(lname, *args, **kwargs):
    if logging_ready():
        logger = logging.getLogger(lname)
        logger.log(*args, **kwargs)
    else:
        buffer_record(lname, (args, kwargs))


def init_logging(config):
    frmt = config.get('logging', 'format')
    logging.basicConfig(format=frmt,
                        stream=sys.stdout, level=config.map_log_level() or logging.WARN)
    for fn in config.get('logging.files.locations').values():
        fp = Path(fn).expanduser().resolve()
        log(lname=__name__, level=logging.INFO,
            msg=f"Creating logging file handler {fp}")
        fh = RobustRFileHandler(filename=fp,
                                maxBytes=config.get(
                                    'logging.files', 'maxbytes'),
                                backupCount=config.get(
                                    'logging.files', 'backupcount')
                                )
        fh.setFormatter(logging.Formatter(frmt))
        logging.getLogger().addHandler(fh)

    for k, v in config.get('logging.extra').items():
        logging.addLevelName(int(k), v)
    __logging_ready.set()
    flush_buffer()


def flush_buffer():
    for lname, records in __buffered_records.items():
        logger = logging.getLogger(lname)
        for args, kwargs in records:
            logger.log(*args, **kwargs)


class RobustRFileHandler(RotatingFileHandler):

    def shouldRollover(self, record):
        try:
            return super().shouldRollover(record)
        except (OSError, ValueError):
            try:
                self.stream.close()
                self.stream = None
                self.stream = self._open()
                return super().shouldRollover(record)
            except:
                print(f"Unable to handle logging error:")
                raise
