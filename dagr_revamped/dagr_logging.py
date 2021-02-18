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


def determine_path(config, lname, lvalue):
    if lvalue == 'NUL:':
        return Path('NUL:')
    if isinstance(lvalue, str):
        placeholders = {
            'outputdirectory': str(config.output_dir)
        }
        lvalue = lvalue.format(**placeholders)
    prefix = config.get('logging.files.names.prefixes', lname)
    fname = config.get('logging.files.names', lname)
    return Path(lvalue, prefix+fname).expanduser().resolve()


def get_logging_paths(config):
    return set([determine_path(config, k, v) for k, v in config.get('logging.files.locations').items()])


def init_logging(config, level=None):
    frmt = config.get('logging', 'format')
    log_level = level or config.map_log_level() or logging.WARN
    logging.basicConfig(format=frmt,
                        stream=sys.stdout, level=log_level)
    for fp in get_logging_paths(config):
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
    logging.log(level=15, msg=f"Log level set to {log_level}")
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
                if not self.stream is None:
                    self.stream.close()
                    self.stream = None
                self.stream = self._open()
                return super().shouldRollover(record)
            except:
                print(f"Unable to handle logging error:")
                raise
