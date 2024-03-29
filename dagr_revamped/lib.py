import json
import logging
import random
import re
import string
import sys
import threading
from copy import deepcopy
from datetime import datetime
from mimetypes import add_type as add_mimetype
from mimetypes import guess_extension
from mimetypes import init as mimetypes_init
from pathlib import Path, PurePosixPath
from pprint import pformat
from time import time

import deviantart
from bs4 import BeautifulSoup
from bs4.element import Tag
from dateutil.parser import parse as date_parse
from requests import codes as req_codes

from .config import DAGRConfig
from .DAGRCache import DAGRCache
from .DAGRIo import DAGRIo
from .exceptions import (DagrCacheLockException, DagrException,
                         DagrHTTPException, DagrPremiumUnavailable)
from .plugin import PluginManager
from .utils import (StatefulBrowser, compare_size, convert_queue,
                    create_browser, dump_html, filter_deviants, get_base_dir,
                    get_html_name, load_bulk_files, make_dirs, shorten_url,
                    sleep, update_d)

logger = logging.getLogger(__name__)


class DAGR():
    def __init__(self, **kwargs):
        self.__work_queue = {}
        self.error_report = []
        self.__kwargs = kwargs
        self.config = kwargs.get('config') or DAGRConfig(kwargs)
        self.deviants = kwargs.get('deviants')
        self.filenames = kwargs.get('filenames')
        self.filter = None if kwargs.get('filter') is None else [
            s.strip().lower() for s in kwargs.get('filter').split(',')]
        self.modes = kwargs.get('modes')
        self.mode_vals = kwargs.get('mode_val')
        self.maxpages = kwargs.get('maxpages')
        self.refresh_only = kwargs.get('refreshonly')
        self.refresh_only_days = kwargs.get('refreshonlydays')
        self.stop_check = kwargs.get('stop_check')
        self.bulk = bool(kwargs.get('bulk'))
        self.test = bool(kwargs.get('test'))
        self.isdeviant = bool(kwargs.get('isdeviant'))
        self.isgroup = bool(kwargs.get('isgroup'))
        self.fixmissing = bool(kwargs.get('fixmissing'))
        self.fixartists = bool(kwargs.get('fixartists'))
        self.nocrawl = bool(kwargs.get('nocrawl'))
        self.fullcrawl = bool(kwargs.get('fullcrawl'))
        self.verifybest = bool(kwargs.get('verifybest'))
        self.verifyexists = bool(kwargs.get('verifyexists'))
        self.unfindable = bool(kwargs.get('unfindable'))
        self.show_queue = bool(kwargs.get('showqueue'))
        self.use_api = bool(kwargs.get('useapi'))
        self.base_url = lambda: self.config.get('deviantart', 'baseurl')
        self.fallbackorder = lambda: list(s.strip() for s in self.config.get(
            'dagr.findlink', 'fallbackorder').split(','))
        self.mature = lambda: self.config.get('deviantart', 'maturecontent')
        self.antisocial = lambda: self.config.get('deviantart', 'antisocial')
        self.outdir = lambda: self.config.output_dir
        self.overwrite = lambda: self.config.get('dagr', 'overwrite')
        self.progress = lambda: self.config.get('dagr', 'saveprogress')
        self.download_delay = lambda: self.config.get('dagr', 'downloaddelay')
        self.resolve_rate_limit = lambda: self.config.get(
            'dagr', 'resolveratelimit')
        self.retry_exception_names = lambda: (
            k for k, v in self.config.get('dagr.retry.exceptionnames').items() if v)
        self.retry_sleep_duration = lambda: self.config.get(
            'dagr.retry', 'sleepduration')
        self.reverse = lambda: self.config.get('dagr', 'reverse') or False
        self.ripper = None
        self.browser = None
        self.crawler_cache = None
        self.deviation_crawler = None
        self.deviation_processor = None
        self.deviant_resolver = None
        self.cache = None
        self.io = None
        self.stop_running = threading.Event()
        self.pl_manager = (kwargs.get('pl_manager') or PluginManager)(self)
        self.total_dl_count = 0
        self.__last_resolved = None
        self.init_mimetypes()
        self.init_classes()

        if self.deviants or (self.bulk and self.filenames) or (self.modes and 'search' in self.modes):
            self.__work_queue = self.__build_queue()

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        self.pl_manager.shutdown()
        if self.browser and hasattr(self.browser, 'quit'):
            self.browser.quit()

        self.cache = None
        self.deviant_resolver = None
        self.deviation_processor = None
        self.ripper = None
        self.deviation_crawler = None
        self.crawler_cache = None
        self.browser = None
        self.io = None

    def init_mimetypes(self):
        mimetypes_init()
        for k, v in self.config.get('dagr.mimetypes').items():
            add_mimetype(k, v)

    def init_classes(self):
        self.io_init()
        self.browser_init()
        self.crawler_cache_init()
        self.crawler_init()
        self.ripper_init()
        self.processor_init()
        self.resolver_init()
        self.cache_init()

    def plugin_class_init(self, class_name, default=None):
        plugin_name = self.config.get('dagr.plugins.classes', class_name)
        if (plugin_name is None) or (plugin_name.lower() == 'default'):
            logger.info('Using default %s', class_name)
            return default

        funcs = self.pl_manager.get_funcs(class_name)
        if not plugin_name in funcs:
            raise Exception(
                f"Could not find {class_name} plugin {plugin_name}")
        logger.info('Using %s %s plugin', plugin_name, class_name)
        return funcs.get(plugin_name)

    def browser_init(self):
        if not self.browser:
            self.browser = self.__kwargs.get('browser') or self.plugin_class_init(
                'browser', create_browser)(self.mature)

    def crawler_cache_init(self):
        if not self.crawler_cache:
            create_cache = self.__kwargs.get(
                'crawler_cache') or self.plugin_class_init('crawler_cache', None)
            if create_cache:
                self.crawler_cache = create_cache(self.io)

    def crawler_init(self):
        if not self.deviation_crawler:
            create_crawler = self.__kwargs.get(
                'crawler') or self.plugin_class_init('crawler', DAGRCrawler)
            self.deviation_crawler = create_crawler(self)

    def ripper_init(self):
        if not self.ripper:
            self.ripper = self.__kwargs.get(
                'ripper') or self.plugin_class_init('ripper', None)

    def processor_init(self):
        if not self.deviation_processor:
            self.deviation_processor = self.__kwargs.get(
                'processor') or self.plugin_class_init('processor', DAGRDeviationProcessor)

    def resolver_init(self):
        if not self.deviant_resolver:
            self.deviant_resolver = self.__kwargs.get(
                'resolver') or self.plugin_class_init('resolver', DAGRDeviantResolver)

    def cache_init(self):
        self.cache = self.__kwargs.get(
            'cache') or self.plugin_class_init('cache', DAGRCache)

    def io_init(self):
        self.io = self.__kwargs.get(
            'io') or self.plugin_class_init('io', DAGRIo)

    def get_queue(self):
        return self.__work_queue

    def set_queue(self, queue):
        self.__work_queue = queue

    def __build_queue(self):
        if self.bulk:
            wq = load_bulk_files(self.filenames)
            wq = convert_queue(self.config, wq)
            wq = filter_deviants(self.filter, wq)
            wq = self.find_refresh(wq)
        else:
            wq = {}
            logger.log(5, 'Deviants: %s', self.deviants)
            logger.log(5, 'Modes: %s', self.modes)
            logger.log(5, 'Mode vals: %s', self.mode_vals)
            if self.deviants:
                for deviant in self.deviants:
                    for mode in self.modes:
                        if self.mode_vals:
                            update_d(wq, {deviant: {mode: [self.mode_vals]}})
                        else:
                            update_d(wq, {deviant: {mode: []}})
            else:
                for mode in self.modes:
                    if self.mode_vals:
                        update_d(wq, {None: {mode: [self.mode_vals]}})
                    else:
                        update_d(wq, {None: {mode: []}})
        logger.log(logging.INFO if self.show_queue else 4,
                   'Work queue: %s', wq)
        return wq

    def find_refresh(self, queue):
        if not (self.refresh_only or self.refresh_only_days):
            return queue
        sq = {**queue}
        seconds = None
        if self.refresh_only_days:
            seconds = int(self.refresh_only_days) * 86400
        elif self.refresh_only:
            now = datetime.now().timestamp()
            refresh_ts = date_parse(self.refresh_only).timestamp()
            seconds = now - refresh_ts
        if not seconds > 0:
            logger.warning(
                'Refresh-only seconds must be greater then 0')
            return queue
        logger.info('Refresh seconds: %s', seconds)
        if None in sq.keys():
            sq.pop(None)
        while sq:
            try:
                deviant = next(iter(sorted(sq.keys(), reverse=self.reverse())))
                modes = sq.pop(deviant)
                deviant, group = self.resolve_deviant(deviant)
                if group:
                    logger.info('Skipping unsupported group %s', deviant)
                    continue
                for mode, mode_vals in modes.items():
                    if mode_vals:
                        for mval in mode_vals:
                            logger.debug('Checking %s: %s: %s',
                                         deviant, mode, mval)
                            if self.check_lastcrawl(seconds, mode, deviant, mval):
                                return {deviant: {mode: [mval]}}
                    else:
                        logger.debug('Checking %s: %s', deviant, mode)
                        if self.check_lastcrawl(seconds, mode, deviant):
                            return {deviant: {mode: None}}
            except DagrException:
                logger.debug(
                    'Exception while finding refresh: ', exc_info=True)
        return {}

    def check_lastcrawl(self, seconds, mode, deviant=None, mval=None):
        base_dir, rel_dir = get_base_dir(self.config, mode, deviant, mval)
        crawl_mode = 'full' if self.maxpages is None else 'short'
        if base_dir.exists():
            cache = self.cache(self.config, base_dir, cache_io=self.io(
                base_dir, rel_dir, self.config))
            last_crawled = cache.last_crawled.get(crawl_mode)
            if last_crawled == 'never':
                logger.debug('%s: never crawled', base_dir)
                return True
            compare_seconds = datetime.now().timestamp() - last_crawled
            if compare_seconds > seconds:
                logger.debug('%s: comp: %s, seconds: %s, compare_seconds > seconds:%s',
                             base_dir, compare_seconds, seconds, compare_seconds > seconds)
                return True
        else:
            logger.warning('Skipping missing dir %s', base_dir)
        return False

    def save_queue(self, path='.queue'):
        with open(path, 'w') as fh:
            json.dump(self.get_queue(), fh)

    def load_queue(self, path='.queue'):
        with open(path, 'r') as fh:
            self.set_queue(json.load(fh))

    def queue_add(self, work):
        return update_d(self.get_queue(), work)

    def keep_running(self, check_stop=False):
        if check_stop and type(self.stop_check).__name__ == 'function':
            return not self.stop_check()
        return not self.stop_running.is_set()

    def run(self):
        if not self.get_queue():
            raise ValueError('Empty work queue')
        wq = self.get_queue()
        logger.info('Mature mode: %s', self.mature())
        logger.info('Antisocial check enabled: %s', self.antisocial())
        logger.info('Fix missing mode: %s', self.fixmissing)
        logger.info('Fix artists mode: %s', self.fixartists)
        logger.info('No crawl mode: %s', self.nocrawl)
        logger.info('Reverse mode: %s', self.reverse())
        logger.info('Test mode: %s', self.test)
        logger.info('Verify mode: %s',
                    self.verifybest or self.verifyexists)
        logger.info('Unfindable mode: %s', self.unfindable)
        logger.info('Loaded plugins: %s',
                    pformat(self.pl_manager.loaded_plugins))
        logger.info('Enabled plugins: %s',
                    pformat(self.pl_manager.enabled_plugins))

        while self.keep_running():
            if None in wq.keys():
                nd = wq.pop(None)
                self.rip(nd, None)
            try:
                deviant = next(iter(sorted(wq.keys(), reverse=self.reverse())))
                modes = wq.pop(deviant)
            except StopIteration:
                break
            self.rip(modes, deviant)
            logger.info('Finished %s', deviant)

    def rip(self, modes, deviant=None):
        group = None
        if deviant:
            try:
                deviant, group = self.resolve_deviant(deviant)
            except DagrException as ex:
                logger.warning(
                    'Deviant %s not found or deactivated!: %s', deviant, str(ex))
                self.handle_download_error(deviant, ex)
                return
        logger.log(5, 'Ripping %s : %s', deviant, modes)
        directory = self.outdir()
        if deviant:
            try:
                make_dirs(directory.joinpath(deviant))
            except OSError:
                logger.warning(
                    'Failed to create deviant directory %s', deviant, exc_info=True)
                return
        for mode, mode_vals in modes.items():
            if mode_vals:
                for mval in mode_vals:
                    self._rip(mode, deviant, mval, group)
            else:
                self._rip(mode, deviant, group=group)
            if not self.keep_running():
                return

    def _rip(self, mode, deviant=None, mval=None, group=False):
        mode_section = f"deviantart.modes.{mode}"
        if group:
            group_url_fmt = self.config.get(mode_section, 'group_url_fmt')
            if not group_url_fmt:
                logger.warning(
                    'Unsuported mode %s ignored for group %s', mode, deviant)
                return
            folder_url_fmt = self.config.get(mode_section, 'folder_url_fmt')
            folder_regex = self.config.get(mode_section, 'folder_regex')
            folders = self.get_folders(group_url_fmt, folder_regex, deviant)
            for folder in folders:
                self.rip_pages(folder_url_fmt, mode, deviant, folder)
        else:
            url_fmt = self.config.get(mode_section, 'url_fmt')
            if mode == 'page':
                self.rip_single(url_fmt, deviant, PurePosixPath(mval).name)
            else:
                self.rip_pages(url_fmt, mode, deviant, mval)

    def rip_pages(self, url_fmt, mode, deviant=None, mval=None):
        msg_formatted = ''
        if deviant:
            msg_formatted += f"{deviant} : "
        msg_formatted += mode
        if mval:
            msg_formatted += f" : {mval}"
        logger.log(15, 'Ripping %s', msg_formatted)
        logger.log(3, pformat(locals()))
        if deviant:
            deviant_lower = deviant.lower()
        try:
            with self.cache.get_cache(self.config, mode, deviant, mval, dagr_io=self.io) as cache:
                pages = self.crawl_pages(
                    url_fmt, mode, deviant, mval, msg_formatted)
                if not self.keep_running():
                    return
                if not pages and not self.nocrawl:
                    logger.log(15, '%s had no deviations', msg_formatted)
                    if self.test:
                        return
                    cache.save_crawled(self.maxpages is None)
                    # cache.save_nolink()
                    # cache.save_queue()
                    return
                logger.log(15, 'Total deviations in %s found: %s',
                           msg_formatted, len(pages))
                self.process_deviations(cache, pages)
                if not self.nocrawl and not self.test:
                    cache.save_extras(self.maxpages is None)
        except (DagrCacheLockException):
            pass

    def rip_single(self, url_fmt, deviant, mval):
        base_url = self.base_url()
        deviant_lower = deviant.lower()
        try:
            with self.cache.get_cache(self.config, 'gallery', deviant, mval, dagr_io=self.io) as cache:
                self.process_deviations(cache, [url_fmt.format(**locals())])
        except DagrCacheLockException:
            pass

    def crawl_pages(self, url_fmt, mode, deviant=None, mval=None, msg_formatted=None):
        if self.nocrawl:
            logger.debug('No crawl mode, skipping pages crawl')
            return []
        return self.deviation_crawler.crawl(url_fmt, mode, deviant, mval, msg_formatted, full_crawl=self.fullcrawl)

    def get_folders(self, url_fmt, folder_regex, deviant):
        deviant_lower = deviant.lower()
        base_url = self.base_url()
        regex = folder_regex.format(**locals())
        folders = []
        offset = 0
        while True:
            url = url_fmt.format(**locals())
            html = self.get(url).text
            logger.log(4, pformat(dict(**locals())))
            k = re.findall(regex, html, re.IGNORECASE)
            if k == []:
                break
            new_folder = False
            for match in k:
                if match not in folders:
                    folders.append(match)
                    new_folder = True
            if not new_folder:
                break
            offset += self.config.get('deviantart.offsets', 'folder')
        if self.reverse():
            folders.reverse()
        logger.debug('Found folders %s', pformat(folders))
        return folders

    def resolve_deviant(self, deviant):
        if self.__last_resolved is not None:
            delay_needed = self.resolve_rate_limit() - \
                (time() - self.__last_resolved)
            if delay_needed > 0:
                logger.log(15, 'Need to sleep for %.4f seconds', delay_needed)
                sleep(delay_needed)

        resolver = self.deviant_resolver(self)
        self.__last_resolved = time()
        result = resolver.resolve(deviant)
        return result

    def process_deviations(self, cache, pages, **kwargs):
        logger.log(level=4, msg=pformat(kwargs))
        dl_delay = self.download_delay()
        logger.info('Download delay: %s', dl_delay)
        disable_filter = kwargs.get('disable_filter', False)
        verify_exists = kwargs.get(
            'verify_exists', False) is True or self.verifyexists is True
        fix_missing = kwargs.get(
            'fix_missing', False) is True or self.fixmissing is True
        verify_best = kwargs.get(
            'verify_best', False) is True or self.verifybest is True
        overwrite = kwargs.get(
            'overwrite', False) is True or self.overwrite() is True

        callback = kwargs.get('callback', None)
        if self.nocrawl:
            pages = cache.existing_pages
            if kwargs.get('reverse', False) is not True and self.reverse() is not True:
                pages.reverse()

        if not any([
            overwrite,
            fix_missing,
            verify_best,
            verify_exists,
            disable_filter
        ]):
            logger.log(level=15, msg='Filtering links')
            pages = cache.filter_links(pages)
        else:
            logger.log(level=5, msg=pformat({
                'overwrite': overwrite,
                'fixmissing': fix_missing,
                'verifybest': verify_best,
                'disable_filter': disable_filter
            }))

        page_count = len(pages)
        logger.log(15, 'Total deviations to download: %s', page_count)
        fileslist_preload_threshold = self.config.get(
            'dagr.cache', 'fileslist_preload_threshold')
        logger.log(
            level=15, msg=f"fileslist preload threshold: {fileslist_preload_threshold}")
        if isinstance(fileslist_preload_threshold, int) and fileslist_preload_threshold > 0:
            if page_count < fileslist_preload_threshold:
                cache.preload_fileslist_policy = 'disable'
                logger.log(
                    level=15, msg='Deviations count below fileslist preload threshold')
            else:
                logger.log(
                    level=15, msg='Deviations count meets fileslist preload threshold')
                cache.preload_fileslist_policy = 'enable'
        progress = self.progress()
        for count, link in enumerate(pages, start=1):
            pstart = time()
            if (not verify_best) and progress > 0 and count % progress == 0:
                cache.save()
            if not self.keep_running(check_stop=count % progress == 0):
                return
            logger.info(
                'Processing deviation %s of %s ( %s )', count, len(pages), link)
            dp = self.deviation_processor(
                self, cache, link, verify_exists=verify_exists)
            downloaded = dp.process_deviation()
            try:
                if callback:
                    callback(page_type=dp.found_type, page_link=link, current_page=dp.get_current_page(
                    ), page_content=dp.get_page_content().content)
            except DagrHTTPException as ex:
                cache.add_httperror(link, ex)
                self.handle_download_error(link, ex)
            delay_needed = dl_delay - (time() - pstart)
            if downloaded and delay_needed > 0:
                logger.log(15, 'Need to sleep for %.4f seconds', delay_needed)
                sleep(delay_needed)
        cache.save('force' if self.fixartists else True)

    def handle_download_error(self, link, link_error):
        logger.warning('Download error (%s) : %s', link, str(link_error))
        self.error_report.append(link_error)

    def get(self, url):
        tries = {}
        response = None
        while True:
            try:
                response = self.get_response(url)
                break
            except Exception as ex:
                except_name = type(ex).__name__.lower()
                if [re for re in self.retry_exception_names() if except_name in re]:
                    logger.warning('Get exception', exc_info=True)
                    if not except_name in tries:
                        tries[except_name] = 0
                    tries[except_name] += 1
                    if tries[except_name] < 3:
                        sleep(self.retry_sleep_duration())
                        continue
                    raise DagrException(
                        f'Failed to get url: {url} {except_name}')
                else:
                    raise DagrException(
                        f'Failed to get url: {url} {except_name}')
        if not response.status_code == req_codes.ok:
            raise DagrException(
                f"Incorrect status code : {response.status_code}")
        return response

    def get_response(self, url, *args, **kwargs):
        if isinstance(url, Tag):
            if hasattr(url, 'attrs') and 'href' in url.attrs:
                url = self.browser.absolute_url(url['href'])
        return self.browser.session.get(url, *args, timeout=150, **kwargs)

    def print_dl_total(self):
        logger.info(f"Download total: {self.total_dl_count}")

    def print_errors(self):
        errors_formatted = {}
        if self.error_report:
            for err in self.error_report:
                error_string = str(err)
                if error_string in errors_formatted:
                    errors_formatted[error_string] += 1
                else:
                    errors_formatted[error_string] = 1
            logger.warning("Download errors:")
            for error_text, error_count in errors_formatted.items():
                logger.warning(
                    f"* {error_text} : {error_count}")

    def report_http_errors(self):
        count = {}
        def err_filter(err): return isinstance(err, DagrHTTPException)
        for err in filter(err_filter, self.error_report):
            if err.http_code in count:
                count[err.http_code] += 1
            else:
                count[err.http_code] = 1
        return count

    def reset_stats(self):
        self.error_report = []
        self.total_dl_count = 0


class APIDeviantResolver():
    def __init__(self, ripper):
        self.ripper = ripper

    def resolve(self, deviant):
        try:
            return self.ripper.da_api.get_user(deviant, True, True), False
        except:
            logger.log(
                level=5, msg='Unable to get deviant info', exc_info=True)
        raise DagrException('Unable to get deviant info')


class APICrawler():
    def __init__(self, ripper):
        self.ripper = ripper
        self.config = ripper.config
        self.da = self.ripper.da_api

    def crawl(self, url_fmt, mode, deviant=None, mval=None, msg_formatted=None):
        mode_action = {
            'favs': self.fetch_favs_deviations,
            'gallery': self.fetch_gallery_deviations
        }.get(mode)
        if(mode_action is None):
            raise DagrException(f"Unkown mode {mode}")
        return mode_action(deviant)

    def fetch_gallery_deviations(self, deviant):
        if isinstance(deviant, deviantart.user.User):
            deviant = str(deviant)
        return self.da.get_gallery_all(deviant)

    def fetch_deviant_collections(self, deviant):
        offset = 0
        has_more = True
        collections = {}
        while has_more:
            try:
                logger.info('Fetching collections offset: %s', offset)
                fetched_collections = self.da.get_collections(
                    username=deviant,
                    offset=offset,
                    ext_preload=False,
                    calculate_size=False,
                )
                results = fetched_collections['results']
                collections.update(dict((v[1], k[1]) for k, v in [
                                   c.items() for c in fetched_collections['results']]))
                offset = fetched_collections['next_offset']
                has_more = fetched_collections['has_more']
            except deviantart.api.DeviantartError as e:
                print(e)
                has_more = False
        return collections

    def fetch_collection_deviations(self, deviant, folder_id):
        offset = 0
        has_more = True
        # while there are more deviations to fetch
        while has_more:
            try:
                logger.info('Fetching collection items offset: %s', offset)
            except deviantart.api.DeviantartError as e:
                print(e)

    def fetch_favs_deviations(self, deviant):
        deviations = []
        if isinstance(deviant,  deviantart.user.User):
            return
        for name, folder_id in self.fetch_collections(deviant).items():
            for deviation in self.fetch_collection_deviations(deviant, folder_id):
                pass


class DAGRCrawler():
    def __init__(self, ripper):
        self.ripper = ripper
        self.config = ripper.config

    def crawl(self, url_fmt, mode, deviant=None, mval=None, msg_formatted=None, **kwargs):
        base_url = self.ripper.base_url()
        pages = []
        pages_offset = (self.config.get('deviantart.offsets', 'search')
                        if mode == 'search'
                        else self.config.get('deviantart.offsets', 'page'))
        art_regex = self.config.get('deviantart.regexes', 'art')
        if deviant:
            deviant_lower = deviant.lower()
        logger.log(level=3, msg=pformat(locals()))
        for page_no in range(0, self.config.get('deviantart', 'maxpages')):
            offset = page_no * pages_offset
            url = url_fmt.format(**locals())
            if msg:
                logger.log(15, 'Crawling %s page %s',
                           msg_formatted, page_no)
            try:
                html = self.ripper.get(url).text
            except DagrException:
                logger.warning(
                    'Could not find %s', url, exc_info=True)
                return pages
            if self.ripper.unfindable:
                return
            matches = re.findall(art_regex, html,
                                 re.IGNORECASE | re.DOTALL)
            for match in matches:
                if match not in pages:
                    pages.append(match)
            done = re.findall("(This section has no deviations yet!|"
                              "This collection has no items yet!|"
                              "Sorry, we found no relevant results.|"
                              "Sorry, we don't have that many results.)",
                              html, re.IGNORECASE | re.S)
            if done:
                break
        if not self.ripper.reverse():
            pages.reverse()
        return pages


class DAGRDeviantResolver():
    def __init__(self, ripper):
        self.__id = ''.join(random.choices(
            string.ascii_uppercase + string.digits, k=5))
        logger.debug('Created DAGRDeviantResolver %s', self.__id)
        self.ripper = ripper

    def __del__(self):
        logger.debug('Destroying DAGRDeviantResolver %s', self.__id)

    def resolve(self, deviant):
        if self.ripper.isdeviant:
            return deviant, False
        if self.ripper.isgroup:
            return deviant, True
        group = False
        try:
            resp = self.ripper.browser.open(
                f"https://www.deviantart.com/{deviant}/")
            if hasattr(self.ripper.browser, 'title'):
                if not deviant.lower() in self.ripper.browser.title.lower():
                    raise DagrException('Unable to get deviant info')
            if not resp.status_code == req_codes.ok:
                raise DagrException(
                    f"Incorrect status code: {resp.status_code}")
            current_page = self.ripper.browser.get_current_page()
            page_title = re.search(
                r'[A-Za-z0-9-]*', current_page.title.string).group(0)
            deviant = re.sub('[^a-zA-Z0-9_-]+', '', page_title)

            if re.search('<dt class="f h">Group</dt>', resp.text):
                group = True
            return deviant, group
        except:
            logger.log(
                level=15, msg='Unable to get deviant info', exc_info=True)
        raise DagrException('Unable to get deviant info')


class DAGRDeviationProcessor():
    def __init__(self, ripper, cache, page_link, **kwargs):
        self.__id = ''.join(random.choices(
            string.ascii_uppercase + string.digits, k=5))
        logger.debug('Created DAGRDeviationProcessor %s', self.__id)
        self.ripper = ripper
        self.config = ripper.config
        self.browser = ripper.browser
        self.base_dir = cache.base_dir
        self.cache = cache
        self.page_link = page_link
        self.__file_link = kwargs.get('file_link')
        self.__filename = kwargs.get('filename')
        self.__found_type = kwargs.get('found_type')
        dest = kwargs.get('dest')

        # if dest and not isinstance(dest, Path):
        #     dest = Path(dest)

        self.__dest = dest
        force_verify_exists = kwargs.get('verify_exists', None)
        self.__force_verify_exists = self.ripper.verifyexists if force_verify_exists is None else force_verify_exists
        self.__response = kwargs.get('response')
        self.__file_ext = kwargs.get('file_ext')
        # self.__html_dump_loc = self.config.get(
        #     'dagr.html', 'dumplocation')
        self.__verify_debug_loc = self.config.get(
            'dagr.verify', 'debuglocation')
        self.__findlink_debug_loc = self.config.get(
            'dagr.findlink', 'debuglocation')
        self.__content_type = None
        self.__mature_error = None
        self.__page_content = None
        self.__current_page = None
        self.__files_list = None

    def __del__(self):
        logger.debug('Destroying DAGRDeviationProcessor %s', self.__id)

    @property
    def force_verify_exists(self):
        return self.__force_verify_exists

    @property
    def found_type(self):
        return self.__found_type

    def get_files_list(self):
        if self.__files_list is None:
            self.__files_list = self.cache.files_list
        return self.__files_list

    def get_current_page(self):
        if self.__current_page is None:
            self.__current_page = self.browser.get_current_page()
        return self.__current_page

    def get_response(self):
        if self.__response:
            return self.__response
        logger.log(4, 'get_response no resonse')
        flink, _ltype = self.find_link()
        self.__response = self.ripper.get(flink)
        return self.__response

    def get_rheaders(self):
        r = self.get_response()
        return r.headers

    def get_fext(self):
        if self.__file_ext:
            return self.__file_ext
        logger.log(4, 'get_fext no file_ext')
        try:
            content_type = self.response_content_type()
            self.__file_ext = guess_extension(content_type)
            if not self.__file_ext:
                raise DagrException(
                    f"Unknown content-type: {content_type}")
        except DagrHTTPException:
            raise
        except:
            ctdp = self.get_rheaders().get('Content-Disposition')
            if ctdp:
                self.__file_ext = Path(re.sub(
                    r'filename(\*)?=(UTF-8\'\')?', '', re.search('filename(.+)', ctdp)[0])).suffix
            if not self.__file_ext:
                raise
        return self.__file_ext

    def get_fname(self):
        if self.__dest:
            return self.__dest.name
        logger.log(4, 'get_fname no dest')
        if self.__filename:
            return self.__filename
        logger.log(4, 'get_fname no filename')
        shortname = PurePosixPath(self.page_link).name
        try:
            self.__filename = self.cache.real_filename(shortname)
            if not self.__filename is None:
                return self.__filename
        except StopIteration:
            pass
        logger.log(4, '%s not in filenames cache', shortname)
        fext = self.get_fext()
        self.__filename = Path(shortname).with_suffix(fext).name
        return self.__filename

    def get_dest(self):
        if self.__dest:
            return self.__dest
        fname = self.get_fname()
        self.__dest = self.base_dir.joinpath(fname)
        return self.__dest

    def process_deviation(self):
        try:
            if self.ripper.test:
                flink, _ltype = self.find_link()
                print(flink)
                return
            if self.download_needed():
                self.download_link()
        except KeyboardInterrupt:
            try:
                inp = input('Do you want to quit? : ').lower()
                if inp.startswith('y'):
                    self.cache.save()
                    self.ripper.stop_running.set()
                elif inp.startswith('q'):
                    sys.exit()
                else:
                    return self.process_deviation()
            except EOFError:
                sys.exit()
        except SystemExit:
            self.cache.save()
            self.ripper.stop_running.set()
        except DagrPremiumUnavailable as ex:
            self.cache.add_premium(self.page_link)
            self.ripper.handle_download_error(self.page_link, ex)
        except DagrHTTPException as ex:
            self.cache.add_httperror(self.page_link, ex)
            self.ripper.handle_download_error(self.page_link, ex)
        except DagrException as ex:
            self.ripper.handle_download_error(self.page_link, ex)
        else:
            self.cache.add_link(self.page_link)
        return not (self.__page_content is None)

    def download_link(self):
        fname = self.get_fname()
        self.save_content()
        self.cache.add_filename(fname)
        self.ripper.total_dl_count += 1

    def download_needed(self):
        if self.ripper.overwrite():
            return True
        if self.checks_fail():
            return True
        return False

    def checks_fail(self):
        if self.verify_exists(warn_on_existing=not self.ripper.verifybest):
            return True
        if self.ripper.verifybest and self.verify_best():
            logger.log(15, 'Verify best fail')
            return True
        return False

    def verify_best(self):
        fullimg_ft = next(iter(self.ripper.fallbackorder()))
        best_res = ['download', 'art_stage', fullimg_ft]
        logger.info('Verifying %s', self.page_link)
        _flink, ltype = self.find_link()
        if not ltype in best_res:
            self.__logger.info('Not a full image, found type is %s', ltype)
            return False

        if self.get_fext() in ['.htm', '.html']:
            self.__logger.info('Skipping html file')
            return False
        fname = self.get_fname()
        response = self.get_response()
        if compare_size(self.cache.cache_io, fname, response.content):
            self.__logger.info('Sizes match, found type is %s', ltype)
            return False
        if self.__verify_debug_loc:
            if not self.cache.cache_io.dir_exists(dir_name=self.__verify_debug_loc):
                self.cache.cache_io.mkdir(dir_name=self.__verify_debug_loc)

            self.cache.cache_io.replace(dest_fname=fname, dest_subdir=self.__verify_debug_loc, src_fname=fname)
            self.__logger.debug('Debug file %s/%s/%s', self.cache.rel_dir, self.__verify_debug_loc, fname)
        return True

    def verify_exists(self, warn_on_existing=True):
        fname = self.get_fname()
        if not self.force_verify_exists:
            if fname in self.get_files_list():
                if warn_on_existing:
                    logger.warning("Cache entry %s exists - skipping", fname)
                return False
        #dest = self.get_dest()
        if self.force_verify_exists:
            logger.log(5, 'Verifying %s really exists', fname)
        if self.cache.cache_io.exists(fname=fname):
            self.cache.add_filename(fname)
            logger.warning("FS entry %s exists - skipping", fname)
            return False
        return True

    def save_content(self):
        dest = self.get_dest()
        tries = {}
        while True:
            try:
                response = self.get_response()
                self.cache.cache_io.write_bytes(
                    self.__response.content, dest=dest)
                break
            except Exception as ex:
                except_name = type(ex).__name__.lower()
                logger.debug('Exception while saving link', exc_info=True)
                if [re for re in self.ripper.retry_exception_names() if except_name in re]:
                    if not except_name in tries:
                        tries[except_name] = 0
                    tries[except_name] += 1
                    if tries[except_name] < 3:
                        sleep(self.ripper.retry_sleep_duration())
                        continue
                    raise DagrException(
                        f"Failed to save content: {except_name}")
                else:
                    raise DagrException(
                        f"Failed to save content: {except_name}")
        if mtime := response.headers.get('last-modified'):
            # Set file dates to last modified time
            self.cache.cache_io.utime(mtime, dest=dest)

    def response_content_type(self):
        if self.__content_type:
            return self.__content_type
        rheaders = self.get_rheaders()
        if "content-type" in rheaders:
            self.__content_type = next(
                iter(rheaders.get("content-type").split(";")), None)
        logger.debug(self.__content_type)
        if not self.__content_type:
            raise DagrException('Missing content-type')
        return self.__content_type

    def get_page_content(self):
        if self.__page_content:
            return self.__page_content
        self.__page_content = self.browser.open(self.page_link)

        if not self.__page_content.status_code == req_codes.ok:
            raise DagrHTTPException(self.__page_content.status_code)

        return self.__page_content

    def find_link(self):
        if self.__file_link:
            return self.__file_link, self.__found_type
        logger.log(4, 'find_link no file_link')
        filelink = None
        resp = self.get_page_content()
        current_url = self.browser.get_url()
        current_page = self.get_current_page()
        # Full image link (via download link)
        link_text = re.compile('Download( (Image|File))?')
        img_link = None
        for candidate in self.browser.links('a'):
            if link_text.search(candidate.text) and candidate.get('href'):
                img_link = candidate
                break
        if img_link and img_link.get('data-download_url'):
            logger.log(5, 'Found download button')
            self.__file_link, self.__found_type = img_link, 'download'
            return self.__file_link, self.__found_type
        img_link = current_page.find(
            'a', {'href': re.compile(r'.*deviantart.com/download/.*')})
        if img_link:
            logger.log(5, 'Found eclipse download button')
            self.__file_link, self.__found_type = img_link, 'download'
            return self.__file_link, self.__found_type
        logger.log(
            15, 'Download link not found, falling back to alternate methods')
        stage = current_page.find('div', {'data-hook': 'art_stage'})
        if stage:
            if stage.find('div', string='Premium Deviation'):
                raise DagrPremiumUnavailable()
            img_tag = stage.find('img')
            if img_tag and hasattr(img_tag, 'src'):
                logger.log(5, 'Found eclipse art stage')
                self.__file_link, self.__found_type = img_tag.get(
                    'src'), 'art_stage'
                return self.__file_link, self.__found_type
            pdf_object = stage.find('object', {'type': 'application/pdf'})
            if pdf_object:
                self.__file_link, self.__found_type = pdf_object.get(
                    'data'), 'pdf_object'
                return self.__file_link, self.__found_type
        page_title = current_page.find('span', {'itemprop': 'title'})
        if page_title and page_title.text == 'Literature':
            logger.log(level=5, msg='Found literature')
            self.__file_link, self.__found_type = current_url, 'literature'
            return self.__file_link, self.__found_type
        lit_h2 = current_page.find('h2', string='Literature Text')
        if lit_h2:
            logger.log(level=5, msg='Found eclipse literature')
            self.__file_link, self.__found_type = current_url, 'literature'
            return self.__file_link, self.__found_type

        # lit_class = current_page.find('div', {'class': '_2JHSA'})
        # if lit_class and lit_class.text.lower() == 'literature':
        #     logger.log(level=5, msg='Found eclipse literature')
        #     self.__file_link, self.__found_type = current_url, 'literature'
        #     return self.__file_link, self.__found_type
        journal = current_page.find('div', {'class': 'journal-wrapper'})
        if 'journal' in current_url or journal:
            logger.log(level=5, msg='Found journal')
            self.__file_link, self.__found_type = current_url, 'journal'
            return self.__file_link, self.__found_type
        # Check for antisocial
        if current_page.find('div', {'class': 'antisocial'}):
            self.cache.add_nolink(self.page_link)
            raise DagrException('deviation not available without login')
        search_tags = {}
        # Fallback 1: try collect_rid, full
        search_tags['img full'] = current_page.find('img',
                                                    {'collect_rid': True,
                                                     'class': re.compile('.*full.*|dev-content-full')})
        # Fallback 2: try meta (filtering blocked meta)
        search_tags['meta'] = current_page.find(
            'meta', {'property': 'og:image'})
        # Fallback 3: try collect_rid, normal
        search_tags['img normal'] = current_page.find('img',
                                                      {'collect_rid': True,
                                                       'class': re.compile('.*normal.*|dev-content-normal')})

        def meta(tag):
            if tag:
                fl = tag.get('content')
                if Path(fl).name.startswith('noentrythumb-'):
                    self.__mature_error = True
                elif 'st.deviantart.net' in fl:
                    logger.log(5, 'Ignoring static meta: %s', fl)
                    return None
                else:
                    return fl
        # Default: 'img full,meta,img normal'
        fbo = self.ripper.fallbackorder()
        logger.log(5, 'Fallback order: %s', fbo)
        searches = {
            'img full': lambda tag: tag.get('src') if tag else None,
            'img normal': lambda tag: tag.get('src') if tag else None,
            'meta': meta
        }
        for si in fbo:
            filelink = searches.get(si)(search_tags.get(si))
            if filelink:
                logger.log(5, 'Found %s', si)
                self.__file_link, self.__found_type = filelink, si
                return self.__file_link, self.__found_type
            else:
                logger.log(5, '%s not found', si)

        for pl_name, pl_func in [*self.ripper.pl_manager.get_funcs('findlink').items()]:
            filelink = pl_func(current_page)
            if filelink:
                logger.log(5, 'Found %s', pl_name)
                self.__file_link, self.__found_type = filelink, pl_name
                return self.__file_link, self.__found_type
        for pl_name, pl_func in [*self.ripper.pl_manager.get_funcs('findlink_b').items()]:
            temp_browser = StatefulBrowser(
                session=deepcopy(self.browser.session))
            temp_browser.open_fake_page(
                deepcopy(resp.content), current_url)
            filelink = pl_func(temp_browser)
            if filelink:
                logger.log(5, 'Found %s', pl_name)
                self.__file_link, self.__found_type = filelink, pl_name
                return self.__file_link, self.__found_type
        self.cache.add_nolink(self.page_link)
        # Check for antisocial
        if self.ripper.antisocial() and current_page.find('div', {'class': 'antisocial'}):
            raise DagrException('deviation not available without login')
        if self.__mature_error:
            if self.ripper.mature():
                raise DagrException('unable to find downloadable deviation')
            else:
                raise DagrException('maybe a mature deviation/' +
                                    'unable to find downloadable deviation')
        if self.__findlink_debug_loc:
            html_name = get_html_name(self.page_link).name
            dump_html(self.cache.cache_io, self.__findlink_debug_loc,
                      html_name,  resp.content)
        raise DagrException('all attemps to find a link failed')
