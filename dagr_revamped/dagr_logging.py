import sys
import logging
import threading

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
    logging.basicConfig(format=config.get('logging', 'format'), stream=sys.stdout, level=config.map_log_level() or logging.WARN)
    for k,v in config.get('logging.extra').items():
        logging.addLevelName(int(k),v)
    __logging_ready.set()
    flush_buffer()

def flush_buffer():
    for lname, records in __buffered_records.items():
        logger = logging.getLogger(lname)
        for args, kwargs in records:
            logger.log(*args, **kwargs)

