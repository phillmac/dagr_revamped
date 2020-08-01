

import logging
from time import sleep, time

from selenium.webdriver.common.keys import Keys

class SeleniumCrawler():
    def __init__(self, app_config, config, browser, cache):
        self.__logger = logging.getLogger(__name__)
        self.__config = config
        self.__browser = browser
        self.__cache = cache

    def collect_pages(self):
        collect_st = time()
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
            level=15, msg=f"Collect pages took {'{:.4f}'.format(time() - collect_st)} seconds")
        return pages

    def crawl_action(self, slug, pages=None, history=None):
        pcount = None
        sleep_time = 0
        if pages is None: pages = set()
        if isinstance(pages, list): pages = set(pages)
        if history is None: history = set()
        body = self.__browser.find_element_by_tag_name('body')
        while (pcount is None) or (pcount < len(pages)):
            pcount = len(pages)
            for _pd in range(self.__config.get('page_down_count', 7)):
                crawl_st = time()
                collected = self.collect_pages()
                if len(collected - pages) > 0:
                    sleep_time = self.__config.get('page_sleep_time', 15)
                    pages.update(collected)
                else:
                    sleep_time = self.__config.get('page_sleep_time', 5)
                self.__logger.info(f"URL count {len(pages)}")
                pd_st = time()
                try:
                    body.send_keys(Keys.PAGE_DOWN)
                except:
                    self.__logger.exception('Error while sending page down keypress')
                self.__logger.log(level=15, msg=f"Sending page down keypress took {'{:.4f}'.format(time() - pd_st )} seconds")
                new_pages = pages - history
                if len(new_pages) > 0:
                    hlen = len(history)
                    history.update(pages)
                    save_st = time()
                    if len(history) > hlen:
                        self.__cache.update(slug, history)
                    else:
                        self.__logger.info('History unchanged')
                    self.__logger.log(
                        level=15, msg=f"Save took {'{:.4f}'.format(time() - save_st)} seconds")
                while time() - crawl_st < sleep_time:
                    sleep(1)
                self.__logger.log(
                    level=15, msg=f"Crawl took {'{:.4f}'.format(time() - crawl_st)} seconds")
                self.__cache.flush(slug)
        return pages

    def crawl(self, url_fmt, mode, deviant, mval=None, msg=None, full_crawl=False):
        full_crawl = full_crawl or self.__config.get('full_crawl', '').lower() == 'force'
        slug = f"{deviant}_{mode}"
        pages = set()
        history = set()
        history.update(self.__cache.query(slug))
        if not full_crawl:
            pages.update(history)
        else:
            self.__logger.info('Performing full crawl, no history loaded')
        url = {
            'gallery': f"https://www.deviantart.com/{deviant}/gallery/all",
            'favs': f"https://www.deviantart.com/{deviant}/favourites/all"
            }.get(mode)

        self.__browser.open_do_login(url)
        return self.crawl_action(slug, pages, history)
