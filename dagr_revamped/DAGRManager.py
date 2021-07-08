import logging
from platform import node as get_hostname

from dagr_revamped.config import DAGRConfig

from .dagr_logging import init_logging
from .lib import DAGR

logger = logging.getLogger(__name__)

from threading import Event

class DAGRManager():
    def __init__(self, config=None):
        if config is None:
            config = DAGRConfig()
        self.__config = config
        self.__dagr = None
        self.__cache = None
        self.__mode = None
        self.__stop_check = None
        self.__hostname = get_hostname().lower()
        self.__session_bad = Event()

    @property
    def session_ok(self):
        return not self.__session_bad.is_set()

    @property
    def mode(self):
        return self.__mode

    def session_bad(self):
        self.__session_bad.set()

    def get_config(self):
        return self.__config

    def init_logging(self, level=None):
        host_mode = self.get_host_mode()
        self.__config.set_section('logging.files.names.prefixes', {
            "remote": f"{host_mode}.",
            "local": f"{self.__mode}."
        })
        init_logging(self.__config, level=level, host_mode=host_mode)
        logger.info(f"Host Mode: {host_mode}")

    def get_browser(self):
        return self.get_dagr().browser

    def get_crawler(self):
        return self.get_dagr().devation_crawler

    def get_dagr(self, **kwargs) -> DAGR:
        if self.__dagr is None:
            self.__dagr = DAGR(
                config=self.__config,
                stop_check=self.__stop_check, **kwargs)
        return self.__dagr

    def get_cache(self):
        if not self.__cache:
            self.__cache = self.get_dagr().crawler_cache
        return self.__cache

    def get_host_mode(self):
        if self.__mode is None:
            return self.__hostname
        return f"{self.__hostname}.{self.__mode}"

    def set_mode(self, mode):
        self.__mode = mode

    def set_stop_check(self, func):
        self.__stop_check = func
