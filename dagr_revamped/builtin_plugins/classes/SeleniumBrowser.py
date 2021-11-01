import logging
import re
import urllib
from contextlib import contextmanager
from pprint import pprint

from bs4 import BeautifulSoup
from dagr_revamped.exceptions import DagrException
from dagr_revamped.plugin import DagrImportError
from dagr_revamped.utils import create_browser as utils_create_browser
from dagr_revamped.utils import sleep

from .Response import Response

try:
    from selenium import webdriver
    from selenium.common.exceptions import (NoSuchElementException,
                                            WebDriverException)
    from selenium.webdriver.common.keys import Keys
    from selenium.webdriver.support.expected_conditions import staleness_of
    from selenium.webdriver.support.ui import WebDriverWait
except ModuleNotFoundError:
    raise DagrImportError('Required package selenium not available')

logger = logging.getLogger(__name__)



class SeleniumBrowser():
    def __init__(self, app_config, config, mature, driver=None):
        self.__app_config = app_config
        self.__config = config
        self.__mature = mature
        self.__driver = None
        self.__browser = None
        self.__login_policy = self.__config.get('login_policy')
        self.__login_url = self.__config.get(
            'login_url', [
                'https://deviantart.com/users/login',
                'https://www.deviantart.com/users/login'
            ])
        self.__create_driver_policy = self.__config.get(
            'create_driver_policy', False)

        logger.log(level=15, msg=f"Login policy: {self.__login_policy}")
        logger.log(
            level=15, msg=f"Create driver policy: {self.__create_driver_policy}")

        if driver:
            self.__driver = driver
        elif self.__create_driver_policy not in [
            'disabled',
            'prohibit',
            'on-demand-only'
        ]:
            self.__create_driver()

        # if self.__mature:
        #     self.__driver.get('https://deviantart.com')
        #     self.__driver.add_cookie({
        #         'name': 'agegate_state',
        #         'value': '1',
        #         "domain": 'deviantart.com',
        #         "expires": '',
        #         'path': '/',
        #         'httpOnly': False,
        #         'HostOnly': False,
        #         'Secure': False
        #     })

    def __enter__(self):
        if self.__driver is None:
            if self.__create_driver_policy in ['prohibit']:
                raise DagrException('Policy disallows creating driver')
            self.__create_driver()
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        if not self.__create_driver_policy in [None, 'keep']:
            self.quit()

    def __create_driver(self):
        options = webdriver.ChromeOptions()
        options.add_argument('--disable-web-security')
        options.add_argument("--start-maximized")
        options.add_argument("--disable-gpu")
        # options.add_argument("--no-sandbox") #See: https://bugs.chromium.org/p/chromedriver/issues/detail?id=2473
        # options.add_argument("--disable-dev-shm-usage")
        # options.add_argument("--remote-debugging-port=9222")
        capabilities = {**options.to_capabilities(), **
                        self.__config.get('capabilities', {})}
        ce_url = self.__config.get('webdriver_url', None)
        webdriver_mode = self.__config.get('webdriver_mode')
        if webdriver_mode == 'local':
            logger.info('Starting selenium in local mode')
            driver_path = self.__config.get('driver_path', None)
            params = {'desired_capabilities': capabilities}
            if driver_path:
                params['executable_path'] = driver_path
            self.__driver = webdriver.Chrome(**params)
        elif webdriver_mode == 'remote':
            max_tries = self.__config.get('webdriver_max_tries', 1)
            logger.info('Starting selenium in remote mode')
            tries = 0
            while self.__driver is None:
                try:
                    self.__driver = webdriver.Remote(
                        command_executor=ce_url,
                        desired_capabilities=capabilities)
                except:
                    tries += 1
                    if tries >= max_tries:
                        raise
                    sleep(5)
        script_timeout = self.__config.get('script_timeout', 45)
        self.__driver.set_script_timeout(script_timeout)
        logger.info(f"Async script timeout: {script_timeout}")

        self.__browser = utils_create_browser(
            mature=self.__mature,
            user_agent=self.__driver.execute_script(
                "return navigator.userAgent;")
        )


    @contextmanager
    def get_r_context(self):
        if self.__driver:
            yield
            return

        if self.__create_driver_policy in ['prohibit']:
                raise DagrException('Policy disallows creating driver')
        self.__create_driver()
        try:
            yield
        finally:
            if not self.__create_driver_policy in [None, 'keep']:
                self.quit()

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
        if self.__login_policy == 'prohibit':
            raise LoginDisabledError('Login policy set to prohibit')
        if not self.__driver.current_url in self.__login_url:
            url = next(iter(self.__login_url))
            logger.info(f"Navigating to {url}")
            self.__driver.get(url)

        user = self.__app_config.get(
            'deviantart', 'username', key_errors=False)

        passwd = self.__app_config.get(
            'deviantart', 'password', key_errors=False)

        if (not user) or (not passwd):
            raise Exception('Username or password not configured')
        try:
            self.__driver.find_element_by_name('username').send_keys(user)
            self.__driver.find_element_by_name('password').send_keys(passwd)
            self.__driver.find_element_by_id(
                'loginbutton').send_keys(Keys.RETURN)
        except NoSuchElementException:
            logger.debug(self.get_current_page().prettify())
            ss_output=str(self.__app_config.output_dir.joinpath('login-fail.png'))
            logger.info(f"Dumping ss to {ss_output}")
            self.__driver.save_screenshot(ss_output)
            logger.info(f"current url is {self.__driver.current_url}")
            raise
        while self.__driver.current_url in self.__login_url:
            self.wait_ready()

    @property
    def login_policy(self):
        return self.__login_policy

    @property
    def create_driver_policy(self):
        return self.__create_driver_policy

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

    @property
    def current_url(self):
        return self.__driver.current_url

    def absolute_url(self, url):
        return urllib.parse.urljoin(self.__driver.current_url, url)

    def get_url(self):
        return self.__driver.current_url

    def __open(self, url):
        self.__driver.get(url)
        self.wait_ready()
        if self.__driver.current_url in self.__login_url:
            if self.__login_policy in ['disable', 'prohibit']:
                raise LoginDisabledError('Automatic login disabled')
            logger.info('Detected login required. Reason: current url')
            logger.info(f"Cuurent url: {self.__driver.current_url}")
            self.do_login()
            if self.__driver.current_url != url:
                self.__driver.get(url)

    def open_do_login(self, url):
        self.__open(url)

        if self.__login_policy not in ['disable', 'prohibit']:
            current_page = self.get_current_page()
            user_link = None
            data_username = None

            top_nav = current_page.find('header', {'data-hook': 'top_nav'})
            if top_nav:
                logger.log(level=10, msg='Found top_nav')
                user_link = top_nav.find('a', {'data-hook': 'user_link'})

            if user_link:
                data_username = user_link.get('data-username')
                logger.log(level=10, msg=f'Detected data-username "{data_username}"')
            if data_username and data_username.lower() == self.__app_config.get(
            'deviantart', 'username').lower():
                logger.log(level=10, msg='Detected already logged in: user link')
            else:
                found = current_page.find('a', {'href': self.__login_url})
                if found and found.text.lower() == 'sign in':
                    logger.info('Detected login required. reason: hyperlink')
                    logger.info(found.prettify())
                    self.do_login()

        if self.__driver.current_url != url:
            self.__driver.get(url)

    def open(self, url):
        if self.__login_policy == 'force':
            self.open_do_login(url)
        else:
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

    def find_elements_by_tag_name(self, *args, **kwargs):
        return self.__driver.find_elements_by_tag_name(*args, **kwargs)

    def find_element_by_link_text(self, *args, **kwargs):
        return self.__driver.find_element_by_link_text(*args, **kwargs)

    def execute_async_script(self, *args, **kwargs):
        return self.__driver.execute_async_script(*args, **kwargs)

    def execute_script(self, *args, **kwargs):
        return self.__driver.execute_script(*args, **kwargs)

    def move_to_element(self, elem):
        webdriver.ActionChains(self.__driver).move_to_element(elem).perform()

    def click_element(self, elem):
        webdriver.ActionChains(self.__driver).move_to_element(
            elem).click().perform()

    def quit(self):
        try:
            if self.__driver is not None:
                self.__driver.quit()
                self.__driver = None
        except WebDriverException:
            logger.exception('Unable to close browser session')


class LoginDisabledError(DagrException):
    pass
