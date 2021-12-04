import logging
from pathlib import Path
from pprint import pprint

import pybreaker
from dagr_revamped.builtin_plugins.classes.SeleniumBrowser import \
    SeleniumBrowser as Browser
from dagr_revamped.builtin_plugins.classes.SeleniumCache import \
    SeleniumCache as Cache
from dagr_revamped.builtin_plugins.classes.SeleniumCrawler import \
    SeleniumCrawler as Crawler
from dagr_revamped.DAGRIo import DAGRIo
from dagr_revamped.plugin import DagrPluginConfigError, DagrPluginDisabledError

logger = logging.getLogger(__name__)


class SeleniumPlugin():
    def __init__(self, manager):
        self.__config_key = 'dagr.plugins.selenium'
        self.__app_config = manager.app_config
        self.__manager = manager
        self.__config = self.__app_config.get(self.__config_key, None)
        self.__browser = None
        self.__cache = None
        self.__crawler = None

        if self.__config is None:
            raise DagrPluginConfigError('Selenium plugin config missing')
        if not self.__config.get('enabled', False):
            raise DagrPluginDisabledError('Selenium plugin is not enabled')
        webdriver_mode = self.__config.get('webdriver_mode')
        if webdriver_mode == 'local':
            pass
        elif webdriver_mode == 'remote':
            if self.__config.get('webdriver_url', None) is None:
                raise DagrPluginConfigError(
                    "Selenium remote mode requires the 'webdriver_url' option to be configured")
        manager.register_browser('selenium', self.create_browser)
        manager.register_crawler('selenium', self.create_crawler)
        manager.register_shutdown('selenium', self.shutdown)
        manager.register_crawler_cache('selenium', self.create_cache)

    def create_cache(self, cache_io_class):
        if self.__cache is None:
            local_cache_path = Path(self.__config.get(
                'local_cache_path', '~/.cache/dagr_selenium')).expanduser().resolve()

            rel_dir = self.__config.get('remote_cache_path', '.selenium')

            fail_max = self.__config.get('remote_breaker_fail_max', 1)
            reset_timeout = self.__config.get(
                'remote_breaker_reset_timeout', 10)
            remote_breaker = pybreaker.CircuitBreaker(
                fail_max=fail_max, reset_timeout=reset_timeout)
            logger.log(
                level=15, msg=f"Remote cache cb - fail_max: {fail_max} reset_timeout: {reset_timeout}")

            local_io = DAGRIo.create(local_cache_path, '', self.__app_config)
            remote_io = cache_io_class.create(self.__app_config.output_dir.joinpath(
                rel_dir), rel_dir, self.__app_config)

            if not local_io.dir_exists():
                local_io.mkdir()
            if not remote_io.dir_exists():
                remote_io.mkdir()

            self.__cache = Cache(local_io, remote_io, remote_breaker)

        return self.__cache

    def create_browser(self, mature):
        self.__browser = Browser(self.__app_config, self.__config, mature)
        return self.__browser

    def create_crawler(self, *args, **kwargs):
        if self.__crawler is None:
            if self.__browser is None:
                raise Exception('Cannot init crawler before browser')
            if self.__cache is None:
                raise Exception('Cannot init crawler before cache')
            self.__crawler = Crawler(
                self.__app_config, self.__config, self.__browser, self.__cache)
        return self.__crawler

    def shutdown(self):
        self.__cache.close()
        self.__cache = None
        self.__crawler = None
        self.__browser = None


def setup(manager):
    return SeleniumPlugin(manager)
