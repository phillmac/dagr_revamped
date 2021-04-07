import logging
import sys
import threading
from logging.handlers import RotatingFileHandler
from pathlib import Path

import requests
from requests.adapters import HTTPAdapter
from requests.packages.urllib3.util.retry import Retry

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
    return set([determine_path(config, k, v) for k, v in config.get('logging.files.locations').items() if not v == ''])


def init_logging(config, level=None, host_mode=None):
    frmt = config.get('logging', 'format')
    log_level = level or config.map_log_level() or logging.WARN
    logging.basicConfig(format=frmt,
                        stream=sys.stdout, level=log_level)

    maxBytes = config.get('logging.files', 'maxbytes')
    backupCount = config.get('logging.files', 'backupcount')

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

    http_handler_hosts = config.get('logging.http.hosts').items()
    filtered_modules = config.get('logging.http', 'filteredmodules').split(',')
    if len(http_handler_hosts) > 0 and not host_mode is None:
        for _n, h in http_handler_hosts:
            log(lname=__name__, level=logging.INFO,
                msg=f"Creating logging http handler {h}")
            httphandler = DagrHTTPHandler(
                h, host_mode, maxBytes, backupCount, frmt, filtered_modules)
            logging.getLogger().addHandler(httphandler)
    else:
        log(lname=__name__, level=logging.WARN,
            msg='Skipping http handlers: missing host_mode param')

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


class DagrHTTPHandler(logging.Handler):
    def __init__(self, host, host_mode, max_bytes, backup_count, frmt, filtered_modules):
        self.__host = host
        self.__host_mode = host_mode
        self.MAX_POOLSIZE = 100
        self.__session = requests.Session()
        self.__filtered_modules = filtered_modules

        self.__session.headers.update({
            'Content-Type': 'application/json'
        })

        self.__session.mount('https://', HTTPAdapter(
            max_retries=Retry(
                total=5,
                backoff_factor=0.5,
                status_forcelist=[403, 500]
            ),
            pool_connections=self.MAX_POOLSIZE,
            pool_maxsize=self.MAX_POOLSIZE
        ))

        self.__session.mount('http://', HTTPAdapter(
            max_retries=Retry(
                total=5,
                backoff_factor=0.5,
                status_forcelist=[403, 500]
            ),
            pool_connections=self.MAX_POOLSIZE,
            pool_maxsize=self.MAX_POOLSIZE
        ))

        super().__init__()

        resp = self.__session.post(f"{self.__host}/logger/create",
                                   json={'hostMode': self.__host_mode, 'maxBytes': max_bytes, 'backupCount': backup_count, 'frmt': frmt})

    def close(self):
        resp = self.__session.post(f"{self.__host}/logger/remove",
                                   json={'hostMode': self.__host_mode})
        super().close()

    def emit(self, record):
        if not record.name in self.__filtered_modules:
            #print(record.name, record.module)
            print(record.__dict__)
            resp = self.__session.post(
                f"{self.__host}/logger/append", json={'hostMode': self.__host_mode, 'record': record.__dict__})
