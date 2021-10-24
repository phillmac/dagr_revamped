

import logging
from pathlib import PurePosixPath
from time import time
from dagr_revamped.utils import sleep


from selenium.webdriver.common.keys import Keys
from selenium.common.exceptions import NoSuchElementException

logger = logging.getLogger(__name__)


class SeleniumCrawler():
    def __init__(self, app_config, config, browser, cache):
        self.__config = config
        self.__browser = browser
        self.__cache = cache
        self.__page_count = 1
        self.__oom_max_pages = self.__config.get('oom_max_pages', 13000)
        self.__collect_mval_id = self.__config.get('collect_mval_id', True)
        logger.log(15, 'OOM max pages set to %s', self.__oom_max_pages)
        logger.log(15, 'Collect using mvalid elem set to %s',
                   self.__collect_mval_id)

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
        """))
        except:
            logger.exception('Error while collecting pages')
        logger.log(15, 'Collect pages took %.4f seconds', time() - collect_st)
        return pages

    def collect_pages_mval_id(self, mval_id):
        collect_st = time()
        pages = set()
        try:
            result = (self.__browser.execute_async_script(
                """
const collect_links = async (mvalID) => {
  const done = arguments[arguments.length - 1]
  if (mvalID) {
    const mvalDiv = document.getElementById(mvalID)
    if (!mvalDiv) {
        done({ iserror: true, message: `Element with id ${mvalID} not found.` })
    }
    const links = mvalDiv.querySelectorAll("a[data-hook=deviation_link]")
    const pages = new Set()
    for (const l of links) {
      pages.add(l.getAttribute("href"))
    }
    done([...pages])
  } else {
    done({ iserror: true, message: 'Missing required mvalID param' })
  }
}
collect_links(arguments[0])
        """, mval_id))
            if(isinstance(result, dict) and result.get('iserror', False)):
                logger.error('Error while collecting pages: %s',
                             result.get('message'))
            else:
                pages.update(result)
        except:
            logger.exception('Error while collecting pages')
        logger.log(15, 'Collect pages took %.4f seconds', time() - collect_st)
        return pages

    def update_history(self, slug, pages, history):
        new_pages = pages - history
        if len(new_pages) > 0:
            hlen = len(history)
            history.update(pages)
            save_st = time()
            if len(history) > hlen:
                self.__cache.update(slug, history)
            else:
                logger.info('History unchanged')
            logger.log(15, "Save took %.4f seconds", time() - save_st)

    def load_more(self, slug, pages, history, mval_id=None):
        body = self.__browser.find_element_by_tag_name('body')
        next_page = None
        try:
            next_page = body.find_element_by_link_text('Next')
        except NoSuchElementException:
            pass

        if next_page:
            logger.log(15, 'Found next page element. Count: %s',
                       self.__page_count)
            self.__page_count += 1
            crawl_st = time()
            collected = self.collect_pages_mval_id(
                mval_id) if mval_id and self.__collect_mval_id else self.collect_pages()

            pages.update(collected)

            logger.info('URL count %s', len(pages))

            self.update_history(slug, pages, history)
            sleep_time = self.__config.get('page_sleep_time', 7)
            delay_needed = sleep_time - (time() - crawl_st)

            if delay_needed > 0:
                logger.log(15, 'Need to sleep for %.2f seconds', delay_needed)
                sleep(delay_needed)

            self.__browser.click_element(next_page)
            return True

        else:
            sleep_time = 0
            for _pd in range(self.__config.get('page_down_count', 7)):
                crawl_st = time()
                collected = self.collect_pages_mval_id(
                    mval_id) if mval_id and self.__collect_mval_id else self.collect_pages()
                if len(collected - pages) > 0:
                    sleep_time = self.__config.get(
                        'collect_sleep_time_long', 15)
                    pages.update(collected)
                else:
                    sleep_time = self.__config.get(
                        'collect_sleep_time_short', 5)
                logger.info('URL count %s', len(pages))
                pd_st = time()
                try:
                    body.send_keys(Keys.PAGE_DOWN)
                except:
                    logger.exception(
                        'Error while sending page down keypress')
                logger.log(
                    15, 'Sending page down keypress took %.4f seconds', time() - pd_st)

                self.update_history(slug, pages, history)

                while time() - crawl_st < sleep_time:
                    sleep(1)
                logger.log(15, 'Crawl took %.4f seconds', time() - crawl_st)
        return False

    def crawl_action(self, slug, mval_id=None, pages=None, history=None):
        pcount = None
        if pages is None:
            pages = set()
        if isinstance(pages, list):
            pages = set(pages)
        if history is None:
            history = set()
        while (pcount is None) or (pcount < len(pages)):
            pcount = len(pages)
            is_paginated = self.load_more(
                slug, pages, history, mval_id=mval_id)
            if not is_paginated and len(pages) > self.__oom_max_pages:
                break  # Prevent chrome from crashing with 'Out of Memory'
        self.__cache.flush(slug)
        return pages

    def crawl(self, url_fmt, mode, deviant, mval=None, msg=None, full_crawl=False, crawl_offset=None, no_crawl=None):
        if not full_crawl:
            conf_fc = self.__config.get('full_crawl', '')
            if conf_fc is True or conf_fc.lower() == 'force':
                full_crawl = True
        slug = None
        mval_id = None
        deviant_lower = deviant.lower() if deviant else None
        url = crawl_offset if isinstance(crawl_offset, str) else {
            'gallery': f"https://www.deviantart.com/{deviant_lower}/gallery/all",
            'scraps': f"https://www.deviantart.com/{deviant_lower}/gallery/scraps",
            'favs': f"https://www.deviantart.com/{deviant_lower}/favourites/all",
            'album': f"https://www.deviantart.com/{deviant_lower}/gallery/{mval}",
            'collection': f"https://www.deviantart.com/{deviant_lower}/favourites/{mval}",
            'favs_featured': f"https://www.deviantart.com/{deviant_lower}/favourites",
            'gallery_featured': f"https://www.deviantart.com/{deviant_lower}/gallery",
            'search': f"https://www.deviantart.com/search?q={mval}",
            'tag': f"https://www.deviantart.com/tag/{mval}?order=most-recent",
            'query': f"https://www.deviantart.com/{deviant_lower}/gallery?q={mval}"
        }.get(mode)

        if not url:
            raise Exception(f"Unable to get url for mode {mode}")

        if mval:
            mval_path = PurePosixPath(mval)
            slug = f"{deviant}_{mode}_{'_'.join(mval_path.parts)}"
            mval_id = mval_path.parent.name
        else:
            slug = f"{deviant}_{mode}"
        pages = set()
        history = set()
        history.update(self.__cache.query(slug))
        if no_crawl:
            logger.info('Skiping crawl')
            pages.update(history)
            return pages
        if not full_crawl:
            pages.update(history)
        else:
            logger.info('Performing full crawl, no history loaded')

        logger.info('Crawl url: %s', url)

        self.__browser.open_do_login(url)
        result = self.crawl_action(slug, mval_id, pages=pages, history=history)
        if self.__config.get('unload_cache_policy', '') == 'always':
            self.__cache.unload(slug)
            logger.log(15, 'Unloaded %s cache', slug)
        return result
