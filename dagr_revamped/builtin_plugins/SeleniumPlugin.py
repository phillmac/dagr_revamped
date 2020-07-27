from logging import exception
from pathlib import Path
from pprint import pprint

from dagr_revamped.builtin_plugins.classes.SeleniumBrowser import \
    SeleniumBrowser as Browser
from dagr_revamped.builtin_plugins.classes.SeleniumCrawler import \
    SeleniumCrawler as Crawler
from dagr_revamped.plugin import DagrPluginConfigError, DagrPluginDisabledError


class SeleniumPlugin():
    def __init__(self, manager):
        self.__config_key = 'dagr.plugins.selenium'
        self.__manager = manager
        self.__app_config = manager.app_config
        self.__config = self.__app_config.get(self.__config_key, None)
        self.__output_dir = manager.output_dir
        self.__browser = None
        self.__cache = self.__output_dir.joinpath(self.__config.get('cachepath','.selenium'))

        if not self.__cache.exists():
            self.__cache.mkdir()

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

    def create_browser(self, mature):
        self.__browser = Browser(self.__app_config, self.__config, mature)
        return self.__browser

    def create_crawler(self, *args, **kwargs):
        if self.__browser is None:
            raise Exception('Cannot init crawler before browser')
        return lambda ripper: Crawler(self.__app_config, self.__config, self.__browser, self.__cache)


def setup(manager):
    return SeleniumPlugin(manager)
