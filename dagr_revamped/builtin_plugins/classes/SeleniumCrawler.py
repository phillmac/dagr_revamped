

import logging
from time import sleep, time

from selenium.webdriver.common.keys import Keys

from dagr_revamped.utils import load_json, save_json


class SeleniumCrawler():
    def __init__(self, app_config, config, browser, cache):
        self.__app_config = app_config
        self.__logger = logging.getLogger(__name__)
        self.__config = config
        self.__browser = browser
        self.__cache = cache

    def collect_pages(self):
        st = time()
        pages = set()
        try:
            pages.update(self.__browser.execute_async_script(
                """
        const done = arguments[0];
        const links = document.querySelectorAll("a[data-hook=deviation_link]")
        const pages = new Set()
        for (const l of links) {
            pages.add(l.getAttribute("href"))
        }
        done([...pages])
        """
            ))
        except:
            self.__logger.exception('Error while collecting pages')
        self.__logger.log(
            level=15, msg=f"Collect pages took {time() - st} seconds")
        return pages

    def crawl_action(self, save_file, pages=set(), history=set()):
        pcount = None
        sleep_time = 0
        body = self.__browser.find_element_by_tag_name('body')
        while (pcount is None) or (pcount < len(pages)):
            pcount = len(pages)
            for _pd in range(self.__config.get('page_down_count', 7)):
                st = time()
                collected = self.collect_pages()
                if len(collected - pages) > 0:
                    sleep_time = self.__config.get('page_sleep_time', 15)
                    pages.update(collected)
                else:
                    sleep_time = self.__config.get('page_sleep_time', 5)
                self.__logger.info(f"URL count {len(pages)}")
                body.send_keys(Keys.PAGE_DOWN)
                self.__logger.log(level=15, msg='Sent page down key')
                new_pages = pages - history
                if len(new_pages) > 0:
                    history.update(pages)
                    try:
                        try:
                            history.update(load_json(save_file))
                        except:
                            self.__logger.exception('Unable to load history')
                        save_json(save_file, history)
                    except:
                        self.__logger.exception('Unable to save history')
                while time() - st < sleep_time:
                    sleep(1)
                self.__logger.log(
                    level=15, msg=f"Crawl took {time() - st} seconds")
        return pages

    def crawl(self, url_fmt, mode, deviant, mval=None, msg=None, full_crawl=False):
        save_file = self.__cache.joinpath(f"{deviant}_{mode}.json")
        pages = set()
        history = set()
        try:
            history.update(load_json(save_file))
        except:
            pass
        if not full_crawl:
            pages.update(history)
        self.__browser.open_do_login(
            f"https://www.deviantart.com/{deviant}/{mode}/all")
        return self.crawl_action(save_file, pages, history)
