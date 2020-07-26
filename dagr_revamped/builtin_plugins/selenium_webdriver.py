import os

from dagr_revamped.plugin import DagrImportError, DagrPluginConfigError

from .classes.SeleniumBrowser import SeleniumBrowser as Browser

def setup(manager):
    app_config = manager.app_config
    config = app_config.get('dagr.plugins.selenium.webdriver', key_errors=False)
    if not config or not config.get('enabled', False): return False
    manager.register_browser('selenium.webdriver', lambda mature: create_browser(app_config, config, mature))

def create_browser(app_config, config, mature):
    if not config.get('webdriver_url', os.environ.get('dagr.plugins.selenium.webdriver.webdriver_url')):
        raise DagrPluginConfigError("Selenium webdriver plugin requires the 'webdriver_url' option to be configured")
    return Browser(app_config, config, mature)
