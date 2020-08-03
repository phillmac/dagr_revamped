import logging
import re
import urllib

from bs4 import BeautifulSoup
from selenium.webdriver import ActionChains

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

logger = logging.getLogger(__name__)


def create_driver(config):
    driver = None
    options = webdriver.ChromeOptions()
    options.add_argument('--disable-web-security')
    options.add_argument("--start-maximized")
    # options.add_argument("--no-sandbox") #See: https://bugs.chromium.org/p/chromedriver/issues/detail?id=2473
    # options.add_argument("--disable-dev-shm-usage")
    # options.add_argument("--remote-debugging-port=9222")
    capabilities = {**options.to_capabilities(), **
                    config.get('capabilities', {})}
    ce_url = config.get('webdriver_url', None)
    webdriver_mode = config.get('webdriver_mode')
    if webdriver_mode == 'local':
        logger.info('Starting selenium in local mode')
        driver_path = config.get('driver_path', None)
        params = {'desired_capabilities': capabilities}
        if driver_path:
            params['executable_path'] = driver_path
        driver = webdriver.Chrome(**params)
    elif webdriver_mode == 'remote':
        logger.info('Starting selenium in remote mode')
        driver = webdriver.Remote(
            command_executor=ce_url,
            desired_capabilities=capabilities)
    return driver


class SeleniumBrowser():
    def __init__(self, app_config, config, mature, driver=None):
        self.__app_config = app_config
        self.__config = config
        self.__mature = mature
        self.__disable_login = self.__config.get('disable_login')
        self.__login_url = self.__config.get(
            'login_url', [
                'https://deviantart.com/users/login',
                'https://www.deviantart.com/users/login'
            ])
        if driver:
            self.__driver = driver
        else:
            self.__driver = create_driver(self.__config)
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

    def wait_stale(self, element, message='Timed out while waiting for staleness', delay=None):
        if delay is None:
            delay = self.__config.get('stale_delay', 30)
            logger.log(level=15, msg=f"Stale delay: {delay}")
        WebDriverWait(self.__driver, delay).until(
            staleness_of(element), message=message)

    def do_login(self):
        if self.__disable_login:
            logger.warning('Ignoring login request')
            return
        if not self.__driver.current_url in self.__login_url:
            self.__driver.get(next(iter(self.__login_url)))

        user = self.__app_config.get(
            'deviantart', 'username', key_errors=False)

        passwd = self.__app_config.get(
            'deviantart', 'password', key_errors=False)

        if (not user) or (not passwd):
            raise Exception('Username or password not configured')

        self.__driver.find_element_by_id('username').send_keys(user)
        self.__driver.find_element_by_id('password').send_keys(passwd)
        self.__driver.find_element_by_id('loginbutton').send_keys(Keys.RETURN)
        while self.__driver.current_url in self.__login_url:
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
        if self.__driver.current_url in self.__login_url:
            logger.info('Detected login required')
            self.do_login()
            if self.__driver.current_url != url:
                self.__driver.get(url)

    def open_do_login(self, url):
        self.__open(url)
        if self.get_current_page().find('a', {'href': self.__login_url}):
            self.do_login()
        if self.__driver.current_url != url:
                self.__driver.get(url)

    def open(self, url):
        self.__open(url)
        page_title = self.title
        page_source = self.__driver.page_source

        if '404 Not Found' in page_title or 'DeviantArt: 404' in page_title:
            return Response(content=page_source, status=404)

        if '403 ERROR' in page_source:
            return Response(content=page_source, status=403)

        if '504 Gateway Time-out' in page_source:
            return Response(content=page_source, status=504)

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

    def refresh(self):
        self.__driver.refresh()

    def find_element_by_css_selector(self, *args, **kwargs):
        return self.__driver.find_element_by_css_selector(*args, **kwargs)

    def find_element_by_tag_name(self, *args, **kwargs):
        return self.__driver.find_element_by_tag_name(*args, **kwargs)

    def execute_async_script(self, *args, **kwargs):
        return self.__driver.execute_async_script(*args, **kwargs)

    def click_element(self, elem):
        ActionChains(self.__driver).move_to_element(elem).click().perform()

    def quit(self):
        self.__driver.quit()
