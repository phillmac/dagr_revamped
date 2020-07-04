import os
import pickle
import re
import sys
import urllib
from pprint import pprint

import requests
from bs4 import BeautifulSoup

from dagr_revamped.plugin import DagrImportError, DagrPluginConfigError
from dagr_revamped.utils import create_browser as utils_create_browser

try:
    from selenium import webdriver
    from selenium.webdriver.common.keys import Keys
    from selenium.webdriver.support.ui import WebDriverWait
except ModuleNotFoundError:
    raise DagrImportError('Required package selenium not available')


def setup(manager):
    app_config = manager.app_config
    config = app_config.get('dagr.plugins.selenium.webdriver', key_errors=False)
    if not config or not config.get('enabled', False): return False
    manager.register_browser('selenium.webdriver', lambda mature: create_browser(app_config, config, mature))

def create_browser(app_config, config, mature):
    if not config.get('webdriver_url', os.environ.get('dagr.plugins.selenium.webdriver.webdriver_url')):
        raise DagrPluginConfigError("Selenium webdriver plugin requires the 'webdriver_url' option to be configured")
    return Browser(app_config, config, mature)


class Browser():
    def __init__(self, app_config, config, mature, driver = None):
        self.__app_config = app_config
        self.__config = config
        self.__mature = mature
        self.__login_url = self.__config.get('login_url', 'https://deviantart.com/users/login')
        if driver:
            self.__driver = driver
        else:
            options = webdriver.ChromeOptions()
            options.add_argument('--disable-web-security')
            capabilities = {**options.to_capabilities(), **self.__config.get('capabilities', {})}
            config_ce_url = self.__config.get('webdriver_url', '')
            ce_url = os.environ.get('dagr.plugins.selenium.webdriver.webdriver_url', config_ce_url)
            self.__driver = webdriver.Remote(
                command_executor=ce_url,
                desired_capabilities=capabilities)
        if self.__mature:
            self.__driver.get('https://deviantart.com')
            self.__driver.add_cookie({
                    'name':'agegate_state',
                    'value':'1',
                    "domain": 'deviantart.com',
                    "expires": '',
                    'path': '/',
                    'httpOnly': False,
                    'HostOnly': False,
                    'Secure': False
                })
        self.do_login()

        self.__browser = utils_create_browser(
            mature = self.__mature,
            user_agent = self.__driver.execute_script("return navigator.userAgent;")
        )

    def wait_ready(self):
        WebDriverWait(self.__driver, 60).until(lambda d: d.execute_script('return document.readyState') == 'complete')

    def do_login(self):
        if self.__dirver.current_url != self.__login_url:
            self.__driver.get('https://deviantart.com/users/login')

        config_user = self.__config.get('deviantart.username', '')
        user = os.environ.get('deviantart.username', config_user)

        config_pass = self.__config.get('deviantart.password', '')
        passwd = os.environ.get('deviantart.password', config_pass)

        self.__driver.find_element_by_id('username').send_keys(user)
        self.__driver.find_element_by_id('password').send_keys(passwd)
        self.__driver.find_element_by_id('loginbutton').send_keys(Keys.RETURN)
        self.wait_ready()




    @property
    def session(self):
        for cookie in self.__driver.get_cookies():
            if 'httpOnly' in cookie: del cookie['httpOnly']
            if 'expiry' in cookie: del cookie['expiry']
            self.__browser.session.cookies.set(**cookie)
        return self.__browser.session

    @property
    def title(self):
        return self.__driver.title

    def absolute_url(self, url):
        return urllib.parse.urljoin(self.__driver.current_url, url)

    def get_url(self):
        return self.__driver.current_url

    def __open(self, url):
        self.__driver.get(url)
        self.wait_ready()

    def open_do_login(self, url):
        self.__open(url)
        if self.__dirver.current_url == self.__login_url:
            self.do_login()
        elif self.get_current_page().find('a', {'href':'https://www.deviantart.com/users/login'}):
            self.do_login()
        if self.__dirver.current_url != url:
            self.__open(url)

    def open(self, url):
        self.__driver.get(url)
        self.wait_ready()
        page_title = self.title
        if '404 Not Found' in page_title or 'DeviantArt: 404' in page_title:
            return Response(content=self.__driver.page_source, status=404)

        return Response(content=self.__driver.page_source)

    def get(self, url, timeout=30, *args, **kwargs):
        cookies = dict((c['name'], c['value']) for c in self.__driver.get_cookies())
        return  self.__browser.get(url, timeout=timeout, *args, **kwargs, cookies=cookies)

    def get_current_page(self):
        soup_config = self.__app_config.get('dagr.bs4.config')
        return BeautifulSoup(self.__driver.page_source, **soup_config)

    def links(self, url_regex=None, link_text=None, *args, **kwargs):
        all_links = self.get_current_page().find_all(
            'a', href=True, *args, **kwargs)
        if url_regex is not None:
            all_links = [a for a in all_links
                        if re.search(url_regex, a['href'])]
        if link_text is not None:
            all_links = [a for a in all_links
                        if a.text == link_text]
        return all_links
    def quit(self):
        self.__driver.quit()



class Response():
    def __init__(self, content='', headers={}, status=200):
        self.__status = status
        self.__headers = headers
        self.__content = content

    @property
    def status_code(self):
        return self.__status

    @property
    def text(self):
        if isinstance(self.__content, str):
            return self.__content
        try:
            if 'ISO-8859-1' in self.headers.get('content-type', ''):
                return self.content.decode('ISO-8859-1')
            return self.content.decode('utf8')
        except UnicodeDecodeError:
            pprint(self.headers)
            raise

    @property
    def headers(self):
        return dict(h.split('\u003a\u0020') for h in self.__headers.split('\u000d\u000a') if len(h.split('\u003a\u0020')) == 2 )

    @property
    def content(self):
        return bytearray(self.__content)
