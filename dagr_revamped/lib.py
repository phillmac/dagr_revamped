import json
import logging
import pathlib
import re
import sys
import threading
from copy import deepcopy
from datetime import datetime
from email.utils import parsedate
from io import StringIO
from mimetypes import add_type as add_mimetype
from mimetypes import guess_extension
from mimetypes import init as mimetypes_init
from os import utime
from pathlib import Path, PurePosixPath
from pprint import pformat
from time import mktime, sleep, time

import deviantart
import portalocker
from bs4 import BeautifulSoup
from bs4.element import Tag
from dateutil.parser import parse as date_parse
from requests import Response
from requests import codes as req_codes

from .config import DAGRConfig
from .plugin import PluginManager
from .utils import (StatefulBrowser, artist_from_url, compare_size,
                    convert_queue, create_browser, filter_deviants,
                    get_base_dir, load_bulk_files, make_dirs, shorten_url,
                    unlink_lockfile, update_d)


class DAGR():
    def __init__(self, **kwargs):
        self.__logger = logging.getLogger(__name__)
        self.__work_queue = {}
        self.errors_count = {}
        self.kwargs = kwargs
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
        self.bulk = bool(kwargs.get('bulk'))
        self.test = bool(kwargs.get('test'))
        self.isdeviant = bool(kwargs.get('isdeviant'))
        self.isgroup = bool(kwargs.get('isgroup'))
        self.fixmissing = bool(kwargs.get('fixmissing'))
        self.fixartists = bool(kwargs.get('fixartists'))
        self.nocrawl = bool(kwargs.get('nocrawl'))
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
        self.outdir = lambda: self.config.get('dagr', 'outputdirectory')
        self.overwrite = lambda: self.config.get('dagr', 'overwrite')
        self.progress = lambda: self.config.get('dagr', 'saveprogress')
        self.download_delay = lambda: self.config.get('dagr', 'downloaddelay')
        self.retry_exception_names = lambda: (
            k for k, v in self.config.get('dagr.retry.exceptionnames').items() if v)
        self.retry_sleep_duration = lambda: self.config.get(
            'dagr.retry', 'sleepduration')
        self.reverse = lambda: self.config.get('dagr', 'reverse') or False
        self.ripper = None
        self.browser = None
        self.devation_crawler = None
        self.deviantion_pocessor = None
        self.deviant_resolver = None
        self.stop_running = threading.Event()
        self.pl_manager = (kwargs.get('pl_manager') or PluginManager)(self)
        self.cache = kwargs.get('cache') or DAGRCache
        self.total_dl_count = 0
        self.init_mimetypes()
        self.init_classes()

        if self.deviants or (self.bulk and self.filenames) or (self.modes and 'search' in self.modes):
            self.__work_queue = self.__build_queue()

    def init_mimetypes(self):
        mimetypes_init()
        for k, v in self.config.get('dagr.mimetypes').items():
            add_mimetype(k, v)

    # def init_classes(self):
    #     if not self.use_api:
    #         self.deviantion_pocessor = self.kwargs.get(
    #             'processor') or DAGRDeviantionProcessor
    #         self.deviant_resolver = self.kwargs.get(
    #             'resolver') or DAGRDeviantResolver
    #         self.devation_crawler = self.kwargs.get('crawler') or DAGRCrawler
    #     else:
    #         self.da_api = deviantart.Api(
    #             self.kwargs.get('clientid'),
    #             self.kwargs.get('clientsecret'),
    #             mature_content=self.mature
    #         )
    #         self.deviantion_pocessor = self.kwargs.get(
    #             'processor') or DAGRDeviantionProcessor
    #         self.deviant_resolver = self.kwargs.get(
    #             'resolver') or APIDeviantResolver
    #         self.devation_crawler = self.kwargs.get('crawler') or APICrawler

    def init_classes(self):
        self.browser_init()
        self.crawler_init()
        self.ripper_init()
        self.processor_init()
        self.resolver_init()

    def plugin_class_init(self, class_name, default=None):
        plugin_name = self.config.get('dagr.plugins.classes', class_name)
        if (plugin_name is None) or (plugin_name.lower() == 'default'):
            self.__logger.info(f"Using default {class_name}")
            return default

        funcs = self.pl_manager.get_funcs(class_name)
        if not plugin_name in funcs:
            raise Exception(
                f"Could not find {class_name} plugin {plugin_name}")
        self.__logger.info(f"Using {plugin_name} {class_name} plugin")
        return funcs.get(plugin_name)

    def browser_init(self):
        if not self.browser:
            self.browser = self.kwargs.get('browser') or self.plugin_class_init(
                'browser', create_browser)(self.mature)

    def crawler_init(self):
        if not self.devation_crawler:
            self.devation_crawler = self.kwargs.get(
                'crawler') or self.plugin_class_init('crawler', DAGRCrawler)(self)

    def ripper_init(self):
        if not self.ripper:
            self.ripper = self.kwargs.get(
                'ripper') or self.plugin_class_init('ripper', None)

    def processor_init(self):
        if not self.deviantion_pocessor:
            self.deviantion_pocessor = self.kwargs.get(
                'processor') or self.plugin_class_init('processor', DAGRDeviantionProcessor)

    def resolver_init(self):
        if not self.deviant_resolver:
            self.deviant_resolver = self.kwargs.get(
                'resolver') or self.plugin_class_init('resolver', DAGRDeviantResolver)

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
            self.__logger.log(5, f'Deviants: {self.deviants}')
            self.__logger.log(5, f'Modes: {self.modes}')
            self.__logger.log(5, f'Mode vals: {self.mode_vals}')
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
        self.__logger.log(level=logging.INFO if self.show_queue else 4,
                          msg='Work queue: {}'.format(pformat(wq)))
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
            self.__logger.warning(
                'Refresh-only seconds must be greater then 0')
            return queue
        self.__logger.info('Refresh seconds: {}'.format(seconds))
        if None in sq.keys():
            sq.pop(None)
        while sq:
            try:
                deviant = next(iter(sorted(sq.keys(), reverse=self.reverse())))
                modes = sq.pop(deviant)
                deviant, group = self.resolve_deviant(deviant)
                if group:
                    self.__logger.info(
                        'Skipping unsupported group {}'.format(deviant))
                    continue
                for mode, mode_vals in modes.items():
                    if mode_vals:
                        for mval in mode_vals:
                            self.__logger.debug(
                                'Checking {}: {}: {}'.format(deviant, mode, mval))
                            if self.check_lastcrawl(seconds, mode, deviant, mval):
                                return {deviant: {mode: [mval]}}
                    else:
                        self.__logger.debug(
                            'Checking {}: {}'.format(deviant, mode))
                        if self.check_lastcrawl(seconds, mode, deviant):
                            return {deviant: {mode: None}}
            except DagrException:
                self.__logger.debug(
                    'Exception while finding refresh: ', exc_info=True)
        return {}

    def check_lastcrawl(self, seconds, mode, deviant=None, mval=None):
        base_dir = get_base_dir(self.config, mode, deviant, mval)
        crawl_mode = 'full' if self.maxpages is None else 'short'
        if base_dir.exists():
            cache = self.cache(self.config, base_dir)
            last_crawled = cache.last_crawled.get(crawl_mode)
            if last_crawled == 'never':
                self.__logger.debug('{}: never crawled'.format(base_dir))
                return True
            compare_seconds = datetime.now().timestamp() - last_crawled
            if compare_seconds > seconds:
                self.__logger.debug('{}: comp: {}, seconds: {}, compare_seconds > seconds:{}'.format(
                    base_dir, compare_seconds, seconds, compare_seconds > seconds))
                return True
        else:
            self.__logger.warning('Skipping missing dir {}'.format(base_dir))
        return False

    def save_queue(self, path='.queue'):
        with open(path, 'w') as fh:
            json.dump(self.get_queue(), fh)

    def load_queue(self, path='.queue'):
        with open(path, 'r') as fh:
            self.set_queue(json.load(fh))

    def queue_add(self, work):
        return update_d(self.get_queue(), work)

    def keep_running(self):
        return not self.stop_running.is_set()

    def run(self):
        if not self.get_queue():
            raise ValueError('Empty work queue')
        wq = self.get_queue()
        self.__logger.info('Mature mode: {}' .format(self.mature()))
        self.__logger.info(f'Antisocial check enabled: {self.antisocial()}')
        self.__logger.info('Fix missing mode: {}'.format(self.fixmissing))
        self.__logger.info('Fix artists mode: {}'.format(self.fixartists))
        self.__logger.info('No crawl mode: {}'.format(self.nocrawl))
        self.__logger.info('Reverse mode: {}'.format(self.reverse()))
        self.__logger.info('Test mode: {}'.format(self.test))
        self.__logger.info('Verify mode: {}'.format(
            self.verifybest or self.verifyexists))
        self.__logger.info('Unfindable mode: {}'.format(self.unfindable))
        self.__logger.info('Loaded plugins: {}'.format(
            pformat(self.pl_manager.loaded_plugins)))
        self.__logger.info('Enabled plugins: {}'.format(
            pformat(self.pl_manager.enabled_plugins)))

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
            self.__logger.info('Finished {}'.format(deviant))

    def rip(self, modes, deviant=None):
        group = None
        if deviant:
            try:
                deviant, group = self.resolve_deviant(deviant)
            except DagrException as ex:
                self.__logger.warning(
                    'Deviant {} not found or deactivated!: {}'.format(deviant, ex))
                self.handle_download_error(deviant, ex)
                return
        self.__logger.log(
            level=5, msg='Ripping {} : {}'.format(deviant or '', modes))
        directory = Path(self.outdir()).expanduser().resolve()
        if deviant:
            try:
                make_dirs(directory.joinpath(deviant))
            except OSError:
                self.__logger.warning(
                    'Failed to create deviant directory {}'.format(deviant), exc_info=True)
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
        mode_section = 'deviantart.modes.{}'.format(mode)
        if group:
            group_url_fmt = self.config.get(mode_section, 'group_url_fmt')
            if not group_url_fmt:
                self.__logger.warning(
                    'Unsuported mode {} ignored for group {}'.format(mode, deviant))
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
        msg = ''
        if deviant:
            msg += '{deviant} : '
        msg += '{mode}'
        if mval:
            msg += ' : {mval}'
        msg_formatted = msg.format(**locals())
        self.__logger.log(level=15, msg='Ripping {}'.format(msg_formatted))
        self.__logger.log(level=3, msg=pformat(locals()))
        if deviant:
            deviant_lower = deviant.lower()
        try:
            with DAGRCache.get_cache(self.config, mode, deviant, mval) as cache:
                pages = self.crawl_pages(url_fmt, mode, deviant, mval, msg)
                if not self.keep_running():
                    return
                if not pages and not self.nocrawl:
                    self.__logger.log(
                        level=15, msg='{} had no deviations'.format(msg_formatted))
                    if self.test:
                        return
                    cache.save_crawled(self.maxpages is None)
                    # cache.save_nolink()
                    # cache.save_queue()
                    return
                self.__logger.log(level=15, msg='Total deviations in {} found: {}'.format(
                    msg_formatted, len(pages)))
                self.process_deviations(cache, pages)
                if not self.nocrawl and not self.test:
                    cache.save_crawled(self.maxpages is None)
                    cache.save_nolink()
                    cache.save_queue()
                    cache.save_premium()
        except (portalocker.exceptions.LockException, portalocker.exceptions.AlreadyLocked):
            pass

    def rip_single(self, url_fmt, deviant, mval):
        base_url = self.base_url()
        deviant_lower = deviant.lower()
        try:
            with DAGRCache.get_cache(self.config, 'gallery', deviant, mval) as cache:
                self.process_deviations(cache, [url_fmt.format(**locals())])
        except (portalocker.exceptions.LockException, portalocker.exceptions.AlreadyLocked):
            pass

    def crawl_pages(self, url_fmt, mode, deviant=None, mval=None, msg=None):
        if self.nocrawl:
            self.__logger.debug('No crawl mode, skipping pages crawl')
            return []
        return self.devation_crawler(self).crawl(url_fmt, mode, deviant, mval, msg)

    def get_folders(self, url_fmt, folder_regex, deviant):
        deviant_lower = deviant.lower()
        base_url = self.base_url()
        regex = folder_regex.format(**locals())
        folders = []
        offset = 0
        while True:
            url = url_fmt.format(**locals())
            html = self.get(url).text
            self.__logger.log(level=4, msg='{}'.format(dict(**locals())))
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
        self.__logger.debug('Found folders {}'.format(pformat(folders)))
        return folders

    def resolve_deviant(self, deviant):
        resolver = self.deviant_resolver(self)
        return resolver.resolve(deviant)

    def process_deviations(self, cache, pages):
        if self.nocrawl:
            pages = cache.existing_pages
        if not (self.overwrite() or self.fixmissing or self.verifybest):
            self.__logger.log(level=5, msg='Filtering links')
            pages = cache.filter_links(pages)
        page_count = len(pages)
        self.__logger.log(
            level=15, msg='Total deviations to download: {}'.format(page_count))
        progress = self.progress()
        for count, link in enumerate(pages, start=1):
            if not self.verifybest and progress > 0 and count % progress == 0:
                cache.save()
            self.__logger.info(
                'Processing deviation {} of {} ( {} )'.format(count, len(pages), link))
            dp = self.deviantion_pocessor(self, cache, link)
            dp.process_deviation()
            if not self.keep_running():
                return
            dl_delay = self.download_delay()
            if not dl_delay == 0:
                sleep(dl_delay)
        cache.save('force' if self.fixartists else True)

    def handle_download_error(self, link, link_error):
        error_string = str(link_error)
        self.__logger.warning(
            "Download error ({}) : {}".format(link, error_string))
        if error_string in self.errors_count:
            self.errors_count[error_string] += 1
        else:
            self.errors_count[error_string] = 1

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
                    self.__logger.warning('Get exception', exc_info=True)
                    if not except_name in tries:
                        tries[except_name] = 0
                    tries[except_name] += 1
                    if tries[except_name] < 3:
                        sleep(self.retry_sleep_duration())
                        continue
                    raise DagrException(
                        f'Failed to get url: {url} {except_name}')
                else:
                    # self.__logger.critical(f'Get exception: {except_name}')
                    raise DagrException(
                        f'Failed to get url: {url} {except_name}')
        if not response.status_code == req_codes.ok:
            raise DagrException(
                'incorrect status code - {}'.format(response.status_code))
        return response

    def get_response(self, url, *args, **kwargs):
        if isinstance(url, Tag):
            if hasattr(url, 'attrs') and 'href' in url.attrs:
                url = self.browser.absolute_url(url['href'])
        return self.browser.session.get(url, *args, timeout=150, **kwargs)

    def print_dl_total(self):
        self.__logger.info(f"Download total: {self.total_dl_count}")

    def print_errors(self):
        if self.errors_count:
            self.__logger.warning("Download errors:")
            for error in self.errors_count:
                self.__logger.warning(
                    '* {} : {}'.format(error, self.errors_count[error]))


class APIDeviantResolver():
    def __init__(self, ripper):
        self.ripper = ripper
        self.__logger = logging.getLogger(__name__)

    def resolve(self, deviant):
        try:
            return self.ripper.da_api.get_user(deviant, True, True), False
        except:
            self.__logger.log(
                level=5, msg='Unable to get deviant info', exc_info=True)
        raise DagrException('Unable to get deviant info')


class APICrawler():
    def __init__(self, ripper):
        self.ripper = ripper
        self.config = ripper.config
        self.da = self.ripper.da_api
        self.__logger = logging.getLogger(__name__)

    def crawl(self, url_fmt, mode, deviant=None, mval=None, msg=None):
        mode_action = {
            'favs': self.fetch_favs_deviations,
            'gallery': self.fetch_gallery_deviations
        }.get(mode)
        if(mode_action is None):
            raise DagrException(f'Unkown mode {mode}')
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
                print('fetching {}'.format(offset))
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
                print('fetching {}'.format(offset))
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
        self.__logger = logging.getLogger(__name__)

    def crawl(self, url_fmt, mode, deviant=None, mval=None, msg=None):
        base_url = self.ripper.base_url()
        pages = []
        pages_offset = (self.config.get('deviantart.offsets', 'search')
                        if mode == 'search'
                        else self.config.get('deviantart.offsets', 'page'))
        art_regex = self.config.get('deviantart', 'artregex')
        if deviant:
            deviant_lower = deviant.lower()
        self.__logger.log(level=3, msg=pformat(locals()))
        for page_no in range(0, self.config.get('deviantart', 'maxpages')):
            offset = page_no * pages_offset
            url = url_fmt.format(**locals())
            if msg:
                self.__logger.log(level=15, msg='Crawling {} page {}'.format(
                    msg.format(**locals()), page_no))
            try:
                html = self.ripper.get(url).text
            except DagrException:
                self.__logger.warning(
                    'Could not find {}'.format(msg.format(**locals())), exc_info=True)
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
        self.ripper = ripper
        self.__logger = logging.getLogger(__name__)

    def resolve(self, deviant):
        if self.ripper.isdeviant:
            return deviant, False
        if self.ripper.isgroup:
            return deviant, True
        group = False
        try:
            resp = self.ripper.browser.open(
                'https://www.deviantart.com/{}/'.format(deviant))
            if not deviant.lower() in self.ripper.browser.title.lower():
                raise DagrException('Unable to get deviant info')
            if not resp.status_code == req_codes.ok:
                raise DagrException(
                    'incorrect status code - {}'.format(resp.status_code))
            current_page = self.ripper.browser.get_current_page()
            page_title = re.search(
                r'[A-Za-z0-9-]*', current_page.title.string).group(0)
            deviant = re.sub('[^a-zA-Z0-9_-]+', '', page_title)

            if re.search('<dt class="f h">Group</dt>', resp.text):
                group = True
            return deviant, group
        except:
            self.__logger.log(
                level=5, msg='Unable to get deviant info', exc_info=True)
        raise DagrException('Unable to get deviant info')


class DAGRDeviantionProcessor():
    def __init__(self, ripper, cache, page_link, **kwargs):
        self.__logger = logging.getLogger(__name__)
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
        if dest and not isinstance(dest, Path):
            dest = Path(dest)
        self.__dest = dest
        self.__response = kwargs.get('response')
        self.__file_ext = kwargs.get('file_ext')
        self.__verify_debug_loc = self.config.get(
            'dagr.verify', 'debuglocation')
        self.__findlink_debug_loc = self.config.get(
            'dagr.findlink', 'debuglocation')
        self.__content_type = None
        self.__mature_error = None

    def get_response(self):
        if isinstance(self.__response, Response):
            return self.__response
        self.__logger.log(level=4, msg='get_response no resonse')
        flink, _ltype = self.find_link()
        self.__response = self.ripper.get(flink)
        return self.__response

    def get_rheaders(self):
        r = self.get_response()
        return r.headers

    def get_fext(self):
        if self.__file_ext:
            return self.__file_ext
        self.__logger.log(level=4, msg='get_fext no file_ext')
        try:
            content_type = self.response_content_type()
            self.__file_ext = guess_extension(content_type)
            if not self.__file_ext:
                raise DagrException(
                    'unknown content-type - {}'.format(content_type))
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
        self.__logger.log(level=4, msg='get_fname no dest')
        if self.__filename:
            return self.__filename
        self.__logger.log(level=4, msg='get_fname no filename')
        shortname = PurePosixPath(self.page_link).name
        try:
            self.__filename = self.cache.real_filename(shortname)
            return self.__filename
        except StopIteration:
            pass
        self.__logger.log(
            level=4, msg='{} not in filenames cache'.format(shortname))
        fext = self.get_fext()
        self.__filename = Path(shortname).with_suffix(fext).name
        return self.__filename

    def get_dest(self):
        if self.__dest:
            return self.__dest
        fname = self.get_fname()
        self.__dest = self.base_dir.joinpath(fname).expanduser().resolve()
        return self.__dest

    def process_deviation(self):
        try:
            if self.ripper.test:
                flink, _ltype = self.find_link()
                print(flink)
                return
            #self.__logger.debug('File link: {}'.format(flink))
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
                    self.process_deviation()
            except EOFError:
                sys.exit()
        except SystemExit:
            self.cache.save()
            self.ripper.stop_running.set()
        except DagrPremiumUnavailable as ex:
            self.cache.add_premium(self.page_link)
            self.ripper.handle_download_error(self.page_link, ex)
        except DagrException as ex:
            self.ripper.handle_download_error(self.page_link, ex)
            return
        else:
            self.cache.add_link(self.page_link)

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
        if self.verify_exists():
            return True
        if self.ripper.verifybest and self.verify_best():
            return True
        return False

    def verify_best(self):
        fullimg_ft = next(iter(self.ripper.fallbackorder()))
        self.__logger.log(level=15, msg='Verifying {}'.format(self.page_link))
        _flink, ltype = self.find_link()
        if not ltype == fullimg_ft:
            self.__logger.log(level=15, msg='Not a full image')
            return False
        if self.get_fext() == '.htm':
            self.__logger.log(level=15, msg='Skipping htm file')
            return False
        dest = self.get_dest()
        response = self.get_response()
        if compare_size(dest, response.content):
            self.__logger.log(level=15, msg='Sizes match')
            return False
        if self.__verify_debug_loc:
            debug = self.base_dir.joinpath(
                self.__verify_debug_loc).expanduser().resolve()
            make_dirs(debug)
            debug_file = debug.joinpath(dest.name)
            debug_file.write_bytes(dest.read_bytes())
            self.__logger.debug('Debug file {}'.format(debug_file))
        return True

    def verify_exists(self):
        fname = self.get_fname()
        if not self.ripper.verifyexists:
            if fname in self.cache.files_list:
                self.__logger.warning(
                    "Cache entry {} exists - skipping".format(fname))
                return False
        dest = self.get_dest()
        if self.ripper.verifyexists:
            self.__logger.log(
                level=5, msg='Verifying {} really esists'.format(dest.name))
        if dest.exists():
            self.cache.add_filename(fname)
            self.__logger.warning(
                "FS entry {} exists - skipping".format(fname))
            return False
        return True

    def save_content(self):
        dest = self.get_dest()
        tries = {}
        while True:
            try:
                response = self.get_response()
                dest.write_bytes(self.__response.content)
                self.__logger.log(
                    level=4, msg='Wrote devation to {}'.format(dest))
                break
            except Exception as ex:
                except_name = type(ex).__name__.lower()
                if [re for re in self.ripper.retry_exception_names() if except_name in re]:
                    self.__logger.debug(
                        'Exception while saving link', exc_info=True)
                    if not except_name in tries:
                        tries[except_name] = 0
                    tries[except_name] += 1
                    if tries[except_name] < 3:
                        sleep(self.ripper.retry_sleep_duration())
                        continue
                    raise DagrException(f'Failed to get url: {except_name}')
                else:
                    # self.__logger.error('Exception name {}'.format(except_name))
                    raise DagrException(f'Failed to get url: {except_name}')
        if response.headers.get('last-modified'):
            # Set file dates to last modified time
            mod_time = mktime(parsedate(response.headers.get('last-modified')))
            utime(dest, (mod_time, mod_time))

    def response_content_type(self):
        if self.__content_type:
            return self.__content_type
        rheaders = self.get_rheaders()
        if "content-type" in rheaders:
            self.__content_type = next(
                iter(rheaders.get("content-type").split(";")), None)
        self.__logger.debug(self.__content_type)
        if not self.__content_type:
            raise DagrException('missing content-type')
        return self.__content_type

    def find_link(self):
        if self.__file_link:
            return self.__file_link, self.__found_type
        self.__logger.log(level=4, msg='find_link no file_link')
        filelink = None
        soup_config = self.config.get('dagr.bs4.config')
        resp = self.browser.open(self.page_link)
        if not resp.status_code == req_codes.ok:
            raise DagrException(
                'incorrect status code - {}'.format(resp.status_code))
        current_page = self.browser.get_current_page()
        # Full image link (via download link)
        link_text = re.compile('Download( (Image|File))?')
        img_link = None
        for candidate in self.browser.links('a'):
            if link_text.search(candidate.text) and candidate.get('href'):
                img_link = candidate
                break
        if img_link and img_link.get('data-download_url'):
            self.__logger.log(level=5, msg='Found download button')
            self.__file_link, self.__found_type = img_link, 'download'
            return self.__file_link, self.__found_type
        img_link = current_page.find(
            'a', {'href': re.compile('deviantart.com/download')})
        if img_link:
            self.__logger.log(level=5, msg='Found eclipse download button')
            self.__file_link, self.__found_type = img_link, 'download'
            return self.__file_link, self.__found_type
        self.__logger.log(
            level=15, msg='Download link not found, falling back to alternate methods')
        stage = current_page.find('div', {'data-hook': 'art_stage'})
        if stage:
            if stage.find('div', string='Premium Deviation'):
                raise DagrPremiumUnavailable()
            img_tag = stage.find('img')
            if img_tag and hasattr(img_tag, 'src'):
                self.__logger.log(level=5, msg='Found eclipse art stage')
                self.__file_link, self.__found_type = img_tag.get(
                    'src'), 'art_stage'
                return self.__file_link, self.__found_type
        page_title = current_page.find('span', {'itemprop': 'title'})
        if page_title and page_title.text == 'Literature':
            self.__logger.log(level=5, msg='Found literature')
            self.__file_link, self.__found_type = self.browser.get_url(), 'literature'
            return self.__file_link, self.__found_type
        lit_class = current_page.find('div', {'class': '_2JHSA'})
        if lit_class and lit_class.text.lower() == 'literature':
            self.__logger.log(level=5, msg='Found eclipse literature')
            self.__file_link, self.__found_type = self.browser.get_url(), 'literature'
            return self.__file_link, self.__found_type
        journal = current_page.find('div', {'class': 'journal-wrapper'})
        if journal:
            self.__logger.log(level=5, msg='Found journal')
            self.__file_link, self.__found_type = self.browser.get_url(), 'journal'
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
                    self.__logger.log(
                        level=5, msg='ignoring static meta: {}'.format(fl))
                    return None
                else:
                    return fl
        # Default: 'img full,meta,img normal'
        fbo = self.ripper.fallbackorder()
        self.__logger.log(level=5, msg='Fallback order: {}'.format(fbo))
        searches = {
            'img full': lambda tag: tag.get('src') if tag else None,
            'img normal': lambda tag: tag.get('src') if tag else None,
            'meta': meta
        }
        for si in fbo:
            filelink = searches.get(si)(search_tags.get(si))
            if filelink:
                self.__logger.log(level=5, msg='Found {}'.format(si))
                self.__file_link, self.__found_type = filelink, si
                return self.__file_link, self.__found_type
            else:
                self.__logger.log(level=5, msg='{} not found'.format(si))

        for found, pl_func in [*self.ripper.pl_manager.get_funcs('findlink').items()]:
            filelink = pl_func(BeautifulSoup(
                deepcopy(resp.content), **soup_config))
            if filelink:
                self.__logger.log(level=5, msg='Found {}'.format(found))
                self.__file_link, self.__found_type = filelink, found
                return self.__file_link, self.__found_type
        for found, pl_func in [*self.ripper.pl_manager.get_funcs('findlink_b').items()]:
            temp_browser = StatefulBrowser(
                session=deepcopy(self.browser.session))
            temp_browser.open_fake_page(
                deepcopy(resp.content), self.browser.get_url())
            filelink = pl_func(temp_browser)
            if filelink:
                self.__logger.log(level=5, msg='Found {}'.format(found))
                self.__file_link, self.__found_type = filelink, found
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
            debug_folder = self.base_dir.joinpath(
                self.__findlink_debug_loc).expanduser().resolve()
            if not debug_folder.exists():
                debug_folder.mkdir(parents=True)
            debug_output = (debug_folder
                            .joinpath(re.sub('[^a-zA-Z0-9_-]+', '_', shorten_url(self.page_link)))
                            .with_suffix('.html'))
            self.__logger.info('Dumping html to {}'.format(debug_output))
            debug_output.write_bytes(resp.content)
        raise DagrException('all attemps to find a link failed')


class DagrException(Exception):
    def __init__(self, value):
        super(DagrException, self).__init__(value)
        self.parameter = value

    def __str__(self):
        return str(self.parameter)


class DagrPremiumUnavailable(DagrException):
    def __init__(self):
        super(DagrPremiumUnavailable, self).__init__(
            'Premium content unavailable')


class DAGRCache():

    @staticmethod
    def get_cache(config, mode, deviant, mval=None):
        base_dir = get_base_dir(config, "gallery", deviant, None)
        return DAGRCache(config, base_dir)

    def __init__(self, dagr_config, base_dir):
        self.__logger = logging.getLogger(__name__)
        if not isinstance(base_dir, Path):
            base_dir = Path(base_dir)
        self.base_dir = base_dir
        self.dagr_config = dagr_config
        self.__lock = None
        self.__lock_path = None
        self.settings_name = self.dagr_config.get(
            'dagr.cache', 'settings') or '.settings'
        self.settings = next(self.__load_cache(
            use_backup=False, settings=self.settings_name))
        self.fn_name = self.settings.get('filenames', '.filenames')
        self.ep_name = self.settings.get(
            'downloadedpages', '.dagr_downloaded_pages')
        self.artists_name = self.settings.get('artists', '.artists')
        self.crawled_name = self.settings.get('crawled', '.crawled')
        self.nolink_name = self.settings.get('nolink', '.nolink')
        self.queue_name = self.settings.get('queue', '.queue')
        self.premium_name = self.settings.get('premium', '.premium')
        self.__files_list = next(self.__load_cache(filenames=self.fn_name))
        self.existing_pages = next(
            self.__load_cache(existing_pages=self.ep_name))
        self.artists = next(self.__load_cache(artists=self.artists_name))
        self.last_crawled = next(self.__load_cache(
            last_crawled=self.crawled_name))
        self.no_link = next(self.__load_cache(
            no_link=self.nolink_name, warn_not_found=False))
        self.queue = next(self.__load_cache(
            queue=self.queue_name, warn_not_found=False))
        self.premium = next(self.__load_cache(
            premium=self.premium_name, warn_not_found=False))
        self.__excluded_fnames = [
            '.lock',
            self.settings_name,
            self.fn_name,
            self.ep_name,
            self.artists_name,
            self.crawled_name,
            self.nolink_name,
            self.queue_name,
            self.premium_name
        ]
        self.downloaded_pages = []
        if not self.settings.get('shorturls') == self.dagr_config.get('dagr.cache', 'shorturls'):
            self.__convert_urls()

    def __enter__(self):
        try:
            if not self.__lock:
                self.__lock_path = self.base_dir.joinpath('.lock')
                self.__lock = portalocker.RLock(
                    self.__lock_path, fail_when_locked=True)
            self.__lock.acquire()
            return self
        except (portalocker.exceptions.LockException, portalocker.exceptions.AlreadyLocked):
            self.__logger.warning(f"Skipping locked directory {self.base_dir}")
            raise

    def __exit__(self, exc_type, exc_val, exc_tb):
        self.__lock.release()
        if self.__lock._acquire_count == 0:
            unlink_lockfile(self.__lock_path)

    @property
    def files_list(self):
        return [f for f in self.__files_list if not f in self.__excluded_fnames]

    def __load_cache_file(self, cache_file, use_backup=True, warn_not_found=True):
        full_path = self.base_dir.joinpath(cache_file)
        backup = full_path.with_suffix('.bak')
        try:
            if full_path.exists():
                with full_path.open('r') as fh:
                    return json.load(fh)
            elif warn_not_found:
                self.__logger.log(
                    level=15, msg='Primary {} cache not found'.format(cache_file))
        except:
            self.__logger.warning(
                'Unable to load primary {} cache:'.format(cache_file), exc_info=True)
            full_path.replace(full_path.with_suffix('.bad'))
        try:
            if use_backup and backup.exists():
                with backup.open('r') as fh:
                    return json.load(fh)
            elif warn_not_found:
                self.__logger.log(
                    level=15, msg='Backup {} cache not found'.format(cache_file))
        except:
            self.__logger.warning(
                'Unable to load backup {} cache:'.format(cache_file), exc_info=True)

    def __load_cache(self, use_backup=True, warn_not_found=True, **kwargs):
        def filenames():
            self.__logger.log(level=15, msg='Building filenames cache')
            files_list_raw = self.base_dir.glob('*')
            return [fn.name for fn in files_list_raw]
        cache_defaults = {
            'settings': lambda: self.dagr_config.get('dagr.cache'),
            'filenames': filenames,
            'existing_pages': lambda: [],
            'artists': lambda: {},
            'last_crawled': lambda: {'short': 'never', 'full': 'never'},
            'no_link': lambda: [],
            'queue': lambda: [],
            'premium': lambda: []
        }
        for cache_type, cache_file in kwargs.items():
            cache_contents = self.__load_cache_file(
                cache_file, use_backup=use_backup, warn_not_found=warn_not_found)
            if cache_contents:
                yield cache_contents
            else:
                if not cache_type in cache_defaults:
                    raise ValueError(
                        'Unkown cache type: {}'.format(cache_type))
                yield cache_defaults[cache_type]()
        self.__logger.log(level=5, msg=pformat(locals()))

    def __ep_exists(self):
        return self.base_dir.joinpath(self.ep_name).exists()

    def __fn_exists(self):
        return self.base_dir.joinpath(self.fn_name).exists()

    def __artists_exists(self):
        return self.base_dir.joinpath(self.artists_name).exists()

    def __settings_exists(self):
        return self.base_dir.joinpath(self.settings_name).exists()

    def __backup_cache_file(self, f):
        backup = f.with_suffix('.bak')
        if f.exists():
            if backup.exists():
                backup.unlink()
            f.rename(backup)

    def __update_cache(self, cache_file, cache_contents, do_backup=True):
        full_path = self.base_dir.joinpath(cache_file)
        if do_backup:
            self.__backup_cache_file(full_path)
        self.__logger.log(level=15, msg='Updating {} cache'.format(cache_file))
        buffer = StringIO()
        json.dump(cache_contents, buffer, indent=4, sort_keys=True)
        buffer.seek(0)
        full_path.write_text(buffer.read())

    def __convert_urls(self):
        self.__logger.warning(
            'Converting cache {} url format'.format(self.base_dir))
        short = self.dagr_config.get('dagr.cache', 'shorturls')
        base_url = self.dagr_config.get('deviantart', 'baseurl')
        self.existing_pages = (
            [shorten_url(p) for p in self.existing_pages] if short else
            ['{}/{}'.format(base_url, p) for p in self.existing_pages]
        )
        self.settings['shorturls'] = short
        self.__update_cache(self.ep_name, self.existing_pages)
        self.__update_cache(self.settings_name, self.settings, False)
        self.update_artists(True)

    def update_artists(self, force=False):
        base_url = self.dagr_config.get('deviantart', 'baseurl')
        updated_pages = self.existing_pages if force else self.downloaded_pages
        self.__logger.log(
            15, 'Sorting {} artist pages'.format(len(updated_pages)))
        for page in updated_pages:
            artist_url_p, artist_name, shortname = artist_from_url(page)
            try:
                rfn = self.real_filename(shortname)
            except StopIteration:
                self.__logger.error('Cache entry not found {} : {} : {}'.format(
                    self.base_dir, page, shortname), exc_info=True)
                raise
            if not artist_name in self.artists:
                self.artists[artist_name] = {
                    'Home Page': '{}/{}'.format(base_url, artist_url_p), 'Artworks': {}}
            self.artists[artist_name]['Artworks'][rfn] = page
        self.__update_cache(self.artists_name, self.artists)

    def rename_deviant(self, old, new):
        rn_count = 0
        for pcount in range(0, len(self.existing_pages)):
            ep = self.existing_pages[pcount]
            artist_url_p = PurePosixPath(ep).parent.parent
            if artist_url_p.name == old:
                result = ep.replace(old, new)
                self.__logger.log(4, 'Changing {} to {}'.format(ep, result))
                self.existing_pages[pcount] = result
                rn_count += 1
        if rn_count > 0:
            self.downloaded_pages = True
        return rn_count

    def save(self, save_artists=False):
        fn_missing = not self.__fn_exists()
        ep_missing = not self.__ep_exists()
        artists_missing = not self.__artists_exists()
        settings_missing = not self.__settings_exists()
        fix_fn = fn_missing and bool(self.files_list)
        fix_ep = ep_missing and bool(self.existing_pages)
        fix_artists = artists_missing and bool(
            self.files_list) and bool(self.existing_pages)
        if settings_missing:
            self.__update_cache(self.settings_name, self.settings, False)
        if self.downloaded_pages or fix_fn:
            self.__update_cache(self.fn_name, self.files_list)
        if self.downloaded_pages or fix_ep:
            self.__update_cache(self.ep_name, self.existing_pages)
        if save_artists:
            if self.downloaded_pages or fix_artists or save_artists == 'force':
                self.update_artists(save_artists == 'force')
        self.__logger.log(level=5, msg=pformat(locals()))

    def save_nolink(self):
        if not self.no_link is None:
            self.__update_cache(self.nolink_name, self.no_link)

    def save_queue(self):
        if not self.queue is None:
            self.__update_cache(self.queue_name, self.queue)

    def save_premium(self):
        if not self.premium is None:
            self.__update_cache(self.premium_name, self.premium)

    def save_crawled(self, full_crawl=False):
        if full_crawl:
            self.last_crawled['full'] = time()
        else:
            self.last_crawled['short'] = time()
        self.__update_cache(self.crawled_name, self.last_crawled)

    def add_premium(self, page):
        if page in self.premium:
            return
        self.dequeue_page(page)
        self.premium.append(page)

    @property
    def nl_exclude(self):
        return set([*self.downloaded_pages, *self.existing_pages, *self.no_link, *self.premium])

    def add_nolink(self, page):
        if page in self.nl_exclude:
            return
        if page in self.queue:
            self.queue.remove(page)
        self.no_link.append(page)

    @property
    def q_exclude(self):
        return set([*self.downloaded_pages, *self.existing_pages, *self.no_link, *self.queue, *self.premium])

    def add_queue(self, page):
        if page in self.q_exclude:
            return
        self.queue.append(page)

    def dequeue_page(self, page):
        if page in self.queue:
            self.queue.remove(page)
            self.__logger.log(level=5, msg=f"Removed {page} from queue")
        if page in self.no_link:
            self.no_link.remove(page)
            self.__logger.log(level=5, msg=f"Removed {page} from no-link list")

    def add_link(self, page):
        self.dequeue_page(page)
        if self.settings.get('shorturls'):
            page = shorten_url(page)
        if page not in self.existing_pages:
            self.downloaded_pages.append(page)
            self.existing_pages.append(page)
            if page in self.queue:
                self.queue.re
        elif self.dagr_config.get('dagr', 'overwrite'):
            self.downloaded_pages.append(page)

    def check_link(self, page):
        if self.settings.get('shorturls'):
            page = shorten_url(page)
        if page in self.existing_pages:
            return True
        self.__logger.log(
            level=5, msg='Checking for lowercase link {}'.format(page))
        return page.lower() in (l.lower() for l in self.existing_pages)

    def filter_links(self, links):
        return [l for l in links if not self.check_link(l)]

    def add_filename(self, fn):
        if fn in self.__files_list:
            self.__logger.log(
                level=5, msg='{} allready in filenames cache'.format(fn))
        else:
            self.__files_list.append(fn)

    def real_filename(self, shortname):
        return next(fn for fn in self.files_list if shortname.lower() in fn.lower())
