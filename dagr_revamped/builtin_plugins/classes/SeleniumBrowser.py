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
        self.__page_title = None
        self.__page_source = None
        self.__bs4 = None
        self.__default_script_timeout = self.__config.get('script_timeout', 45)
        self.__login_policy = self.__config.get('login_policy')
        self.__login_urls = self.__config.get(
            'login_url', [
                'https://deviantart.com/users/login',
                'https://www.deviantart.com/users/login'
            ])
        self.__deviantart_username = self.__app_config.get(
                    'deviantart', 'username')
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
        self.__driver.set_script_timeout(self.__default_script_timeout)
        logger.info('Default async script timeout: %s',
                    self.__default_script_timeout)

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
        is_ready = False
        count = 0
        while not is_ready and count <= 6:
            count += 1
            result = self.__driver.execute_async_script(
                """
const done = arguments[0];
(async () => {
  var count=1;
  while(count <= 20 && document.readyState !== 'complete'){
    count++;
    await new Promise(r => setTimeout(r, 500));
  }
  return { readyState: document.readyState };
})().then((result) => {
    console.log({result, done});
    done(result);
    })
""")
            logger.debug('Got page ready result %s', result)
            is_ready = (result.get('readyState') == 'complete')
        if not is_ready:
            raise DagrException('Page ready timeout')

    def wait_stale(self, element, message='Timed out while waiting for staleness', delay=None):
        if delay is None:
            delay = self.__config.get('stale_delay', 30)
            logger.log(level=15, msg=f"Stale delay: {delay}")
        WebDriverWait(self.__driver, delay).until(
            staleness_of(element), message=message)

    def do_login(self):
        if self.__login_policy == 'prohibit':
            raise LoginDisabledError('Login policy set to prohibit')
        if not self.get_url() in self.__login_urls:
            url = next(iter(self.__login_urls))
            logger.info('Navigating to %s', url)
            self.__driver_get(url)

        user = self.__deviantart_username

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
            ss_output = str(
                self.__app_config.output_dir.joinpath('login-fail.png'))
            logger.info('Dumping ss to %s', ss_output)
            self.__driver.save_screenshot(ss_output)
            logger.info('current url is %s', self.get_url())
            raise
        while self.get_url() in self.__login_urls:
            logger.debug('Waiting for page other than login')
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
        if  self.__page_title is None:
            self.__page_title = self.__driver.title

        return self.__page_title

    @property
    def page_source(self):
        if self.__page_source is None:
            self.__page_source = self.__driver.page_source
        return self.__page_source

    @property
    def current_url(self):
        return self.get_url()

    def absolute_url(self, url):
        return urllib.parse.urljoin(self.get_url(), url)

    def get_url(self):
        logger.debug('Fetching url')
        result = self.__driver.current_url
        logger.debug('Url is %s', result)
        return result

    def __driver_get(self, url):
        self.__page_title = None
        self.__page_source = None
        self.__driver.get(url)

    def __open(self, url):
        self.__driver_get(url)
        self.wait_ready()

        current_url = self.get_url()
        if current_url in self.__login_urls:
            if self.__login_policy in ['disable', 'prohibit']:
                raise LoginDisabledError('Automatic login disabled')
            logger.info('Detected login required. Reason: current url: %s', current_url)
            self.do_login()
            if self.get_url() != url:
                self.__driver_get(url)

    def open_do_login(self, url):
        self.__open(url)

        if self.__login_policy not in ['disable', 'prohibit']:
            data_username = (self.__driver.execute_async_script(
                """
const done = arguments[0];
let dataUsername = '';

const getUsername = () => {
    const topNav = document.querySelector('header[data-hook=top_nav]');
    const userLink = topNav?.querySelector('a[data-hook=user_link]')
    return userLink?.dataset?.username;;
};

(async () => {
  var count=1;
  while(count <= 20 && ! (dataUsername = getUsername())){
    count++;
    await new Promise(r => setTimeout(r, 500));
  }
  return { dataUsername };
})().then((result) => {
    done(result);
    })
""").get('dataUsername') or '').lower()


            if data_username:
                conf_uname = self.__deviantart_username.lower()
                if data_username == conf_uname:
                    logger.log(10, 'Detected already logged in: user link')
                    return
                else:
                    logger.warning('data-username mismatch. %s != %s', data_username, conf_uname)
            elif self.__login_policy == 'force':
                self.do_login()
            else:
                current_page = self.get_current_page()
                found = current_page.find('a', {'href': self.__login_urls})
                if found and found.text.lower() =='log in':
                    logger.info('Detected login required. reason: hyperlink')
                    logger.info(found.prettify())
                    self.do_login()

        if self.get_url() != url:
           self.__driver_get(url)

    def open(self, url):
        self.__bs4 = None
        if self.__login_policy == 'force':
            self.open_do_login(url)
        else:
            self.__open(url)

        if '404 Not Found' in self.title or 'DeviantArt: 404' in self.title:
            return Response(content=self.page_source, status=404)

        if '403 ERROR' in self.page_source:
            return Response(content=self.page_source, status=403)

        if '504 Gateway Time-out' in self.page_source:
            return Response(content=self.page_source, status=504)
        return Response(content=self.page_source)

    def get(self, url, timeout=30, *args, **kwargs):
        cookies = dict((c['name'], c['value'])
                       for c in self.__driver.get_cookies())
        return self.__browser.get(url, timeout=timeout, *args, **kwargs, cookies=cookies)

    def get_current_page(self):
        if self.__bs4 is None:
            soup_config = self.__app_config.get('dagr.bs4.config')
            self.__bs4 = BeautifulSoup(self.page_source, **soup_config)
        return self.__bs4

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

    def execute_async_script(self, *args, timeout=None, **kwargs):
        if timeout:
            self.__driver.set_script_timeout(timeout)
        try:
            return self.__driver.execute_async_script(*args, **kwargs)
        finally:
            if timeout:
                self.__driver.set_script_timeout(self.__default_script_timeout)

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
