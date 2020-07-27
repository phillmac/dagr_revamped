import logging
import os
import re
import urllib

from bs4 import BeautifulSoup
from docopt import parse_seq

from dagr_revamped.plugin import DagrImportError
from dagr_revamped.utils import create_browser as utils_create_browser

from .Response import Response

try:
    from selenium import webdriver
    from selenium.webdriver.common.keys import Keys
    from selenium.webdriver.support.ui import WebDriverWait
    from selenium.webdriver.support.expected_conditions import staleness_of

except ModuleNotFoundError:
    raise DagrImportError('Required package selenium not available')


class SeleniumBrowser():
    def __init__(self, app_config, config, mature, driver=None):
        self.__app_config = app_config
        self.__config = config
        self.__logger = logging.getLogger(__name__)
        self.__mature = mature
        self.__login_url = self.__config.get(
            'login_url', 'https://deviantart.com/users/login')
        if driver:
            self.__driver = driver
        else:
            options = webdriver.ChromeOptions()
            options.add_argument('--disable-web-security')
            options.add_argument("--start-maximized")
            capabilities = {**options.to_capabilities(), **
                            self.__config.get('capabilities', {})}
            ce_url = self.__config.get('webdriver_url', None)
            webdriver_mode = config.get('webdriver_mode')
            if webdriver_mode == 'local':
                self.__logger.info('Starting selenium in local mode')
                driver_path = config.get('driver_path', None)
                params = {'desired_capabilities': capabilities}
                if driver_path:
                    params['executable_path'] = driver_path
                self.__driver = webdriver.Chrome(**params)
            elif webdriver_mode == 'remote':
                self.__logger.info('Starting selenium in remote mode')
                self.__driver = webdriver.Remote(
                    command_executor=ce_url,
                    desired_capabilities=capabilities)
        if self.__mature:
            self.__driver.get('https://deviantart.com')
            self.__driver.add_cookie({
                'name': 'agegate_state',
                'value': '1',
                "domain": 'deviantart.com',
                "expires": '',
                'path': '/',
                'httpOnly': False,
                'HostOnly': False,
                'Secure': False
            })

        self.__browser = utils_create_browser(
            mature=self.__mature,
            user_agent=self.__driver.execute_script(
                "return navigator.userAgent;")
        )

    def wait_ready(self):
        WebDriverWait(self.__driver, 60).until(
            lambda d: d.execute_script('return document.readyState') == 'complete')

    def wait_stale(self, element, message='Timed out while waiting for staleness', delay=3):
        WebDriverWait(self.__driver, delay).until(
            staleness_of(element), message=message)

    def do_login(self):
        if self.__driver.current_url != self.__login_url:
            self.__driver.get('https://deviantart.com/users/login')

        user = self.__app_config.get(
            'deviantart', 'username', key_errors=False)

        passwd = self.__app_config.get(
            'deviantart', 'password', key_errors=False)

        if (not user) or (not passwd):
            raise Exception('Username or password not configured')

        self.__driver.find_element_by_id('username').send_keys(user)
        self.__driver.find_element_by_id('password').send_keys(passwd)
        self.__driver.find_element_by_id('loginbutton').send_keys(Keys.RETURN)
        self.wait_ready()

    @property
    def session(self):
        for cookie in self.__driver.get_cookies():
            if 'httpOnly' in cookie:
                del cookie['httpOnly']
            if 'expiry' in cookie:
                del cookie['expiry']
            if 'sameSite' in cookie:
                del cookie['sameSite']
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
        if self.__driver.current_url == self.__login_url:
            self.do_login()

    def open_do_login(self, url):
        self.__open(url)
        if self.get_current_page().find('a', {'href': 'https://www.deviantart.com/users/login'}):
            self.do_login()
        if self.__driver.current_url != url:
            self.__open(url)

    def open(self, url):
        self.__open(url)
        page_title = self.title
        page_source = self.__driver.page_source

        if '404 Not Found' in page_title or 'DeviantArt: 404' in page_title:
            return Response(content=page_source, status=404)

        if '403 ERROR' in page_source:
            return Response(content=page_source, status=403)

        return Response(content=page_source)

    def get(self, url, timeout=30, *args, **kwargs):
        cookies = dict((c['name'], c['value'])
                       for c in self.__driver.get_cookies())
        return self.__browser.get(url, timeout=timeout, *args, **kwargs, cookies=cookies)

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

    def find_element_by_tag_name(self, *args, **kwargs):
        return self.__driver.find_element_by_tag_name(*args, **kwargs)

    def execute_async_script(self, *args, **kwargs):
        return self.__driver.execute_async_script(*args, **kwargs)

    def quit(self):
        self.__driver.quit()
