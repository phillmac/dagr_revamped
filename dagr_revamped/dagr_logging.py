import logging
import sys
import threading
from logging.handlers import QueueHandler, QueueListener, RotatingFileHandler
from pathlib import Path
from queue import Queue

from requests.exceptions import ConnectionError, ReadTimeout, RetryError

from dagr_revamped.utils import sleep

from .TCPKeepAliveSession import TCPKeepAliveSession

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
    logging_paths = set()
    for k, v in config.get('logging.files.locations').items():
        if not v == '':
            logging_paths.add((determine_path(config, k, v),
                      config.get('logging.files.levels').get(k, None)))
    return logging_paths



def init_logging(config, level=None, host_mode=None):
    frmt = config.get('logging', 'format')
    log_level = level or config.map_log_level() or logging.WARN
    logging.basicConfig(format=frmt,
                        stream=sys.stdout, level=log_level)

    maxBytes = config.get('logging.files', 'maxbytes')
    backupCount = config.get('logging.files', 'backupcount')

    for fp, ll in get_logging_paths(config):
        log(lname=__name__, level=logging.INFO,
            msg=f"Creating logging file handler {fp}")
        fh = RobustRFileHandler(filename=fp,
                                maxBytes=config.get(
                                    'logging.files', 'maxbytes'),
                                backupCount=config.get(
                                    'logging.files', 'backupcount')
                                )
        if ll is not None:
            fh.setLevel(ll)
            log(lname=__name__, level=logging.INFO,
                msg=f"Set file logger {fp} level to {ll}")
        fh.setFormatter(logging.Formatter(frmt))
        logging.getLogger().addHandler(fh)

    http_handler_hosts = config.get('logging.http.hosts').items()
    filtered_modules = config.get('logging.http', 'filteredmodules').split(',')
    filtered_keys = config.get('logging.http', 'filteredkeys').split(',')
    if len(http_handler_hosts) > 0 and not host_mode is None:
        http_handlers=[]
        queue = Queue()
        queuehandler = QueueHandler(queue)
        queuelistener = QueueListener(queue,handlers=http_handlers)
        for n, h in http_handler_hosts:
            log(lname=__name__, level=logging.INFO,
                msg=f"Creating logging http handler {n} {h}")
            http_handlers.append(DagrHTTPHandler(
                h, host_mode, maxBytes, backupCount, frmt, filtered_modules, filtered_keys))
        queuelistener.start()
        logging.getLogger().addHandler(queuehandler)
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
    def __init__(self, host, host_mode, max_bytes, backup_count, frmt, filtered_modules, filtered_keys):
        self.__host = host
        self.__host_mode = host_mode
        self.__max_bytes = max_bytes
        self.__backup_count = backup_count
        self.__frmt = frmt
        self.MAX_POOLSIZE = 100
        self.__session = TCPKeepAliveSession()
        self.__filtered_modules = filtered_modules
        self.__filtered_keys = filtered_keys

        self.__session.headers.update({
            'Content-Type': 'application/json'
        })

        super().__init__()
        self.create_remote()

    def create_remote(self):
        _resp = self.__session.post(f"{self.__host}/logger/create",
                                    json={'hostMode': self.__host_mode, 'maxBytes': self.__max_bytes, 'backupCount': self.__backup_count, 'frmt': self.__frmt})

    def close(self):
        _resp = self.__session.post(f"{self.__host}/logger/remove",
                                    json={'hostMode': self.__host_mode})
        super().close()

    def emit(self, record):
        self.post_record(record)

    def post_record(self, record, retry=False, connection_retries=0):
        if not record.name in self.__filtered_modules:
            print(record.name, record.module)
            # print(record.__dict__)
            try:
                resp = self.__session.post(
                    f"{self.__host}/logger/append", timeout=30, json={'hostMode': self.__host_mode, 'record':
                                                                      {
                                                                          kn: record.__dict__[kn] for kn in record.__dict__.keys() if not kn in self.__filtered_keys}
                                                                      })
                if resp.status_code == 400 and retry is False:
                    check_resp = self.__session.get(
                        f"{self.__host}/logger/exists", json={'hostMode': self.__host_mode})
                    if check_resp.json()['exists'] is False:
                        self.create_remote()
                        self.post_record(record, retry=True)
            except TypeError as ex:
                print(ex, record.__dict__)

            except (ConnectionError, ReadTimeout):
                disconnected = True
                while disconnected:
                    try:
                        self.__session.get(
                            f"{self.__host}/ping"
                        )
                        disconnected = False
                    except (ConnectionError, ReadTimeout, RetryError):
                        sleep(30)
                if connection_retries <= 10:
                    raise
                sleep(30)
                print(f"HTTP logger connection retries {connection_retries}")
                self.post_record(record, connection_retries=connection_retries+1)
