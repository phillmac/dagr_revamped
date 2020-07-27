import os

from dagr_revamped.plugin import DagrImportError, DagrPluginConfigError

from .classes.SeleniumBrowser import SeleniumBrowser as Browser

config_key = 'dagr.plugins.selenium'


def setup(manager):
    app_config = manager.app_config
    config = app_config.get(
        config_key, key_errors=False)
    if not config or not config.get('enabled', False):
        return False
    webdriver_mode = config.get('webdriver_mode')
    if webdriver_mode == 'local':
        pass
    elif webdriver_mode == 'remote':
        if config.get('webdriver_url', None) is None:
            raise DagrPluginConfigError(
                "Selenium remote mode requires the 'webdriver_url' option to be configured")
    manager.register_browser(
        'selenium', lambda mature: create_browser(app_config, config, mature))


def create_browser(app_config, config, mature):
    return Browser(app_config, config, mature)
