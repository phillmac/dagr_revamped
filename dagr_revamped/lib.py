import re
import sys
import json
import pickle
import logging
import threading
import portalocker
import collections.abc
from glob import glob
from time import mktime
from random import choice
from pprint import pformat
from bs4.element import Tag
from shutil import copyfileobj
from .config import DAGRConfig
from email.utils import parsedate
from .plugin import PluginManager
from mechanicalsoup import StatefulBrowser
from os import getcwd, makedirs, rename, utime, remove, rmdir
from os.path import (
    abspath, basename, dirname, normpath, expanduser,
    exists as path_exists,
    join as path_join
    )
from mimetypes import (
    guess_extension, add_type as add_mimetype,
    init as mimetypes_init
    )
from requests import (
    adapters as req_adapters, codes as req_codes,
    session as req_session
    )

class DAGR():
    MODES = DAGRConfig.DEFAULTS['DeviantArt']['Modes'].split(',')
    def __init__(self, **kwargs):
        logging.addLevelName(25,'INFO')
        logging.addLevelName(15,'INFO')
        logging.addLevelName(5,'TRACE')
        logging.addLevelName(4,'TRACE')
        logger = logging.getLogger(__name__)
        self.__work_queue = {}
        self.errors_count = {}
        self.bulk = kwargs.get('bulk')
        self.modes = kwargs.get('modes')
        self.mode_vals = kwargs.get('mode_val')
        self.deviants = kwargs.get('deviants')
        self.filenames = kwargs.get('filenames')
        self.filter = None if kwargs.get('filter') is None else kwargs.get('filter').split(',')
        self.test = kwargs.get('test')
        self.config = DAGRConfig(kwargs)
        sys.setrecursionlimit(self.config.get('dagr','recursionlimit'))
        self.init_mimetypes()
        self.browser = None
        self.stop_running = threading.Event()
        self.pl_manager = PluginManager(self)
        if self.deviants or self.bulk and self.filenames:
            self.__work_queue = self.__build_queue()

    def init_mimetypes(self):
        mimetypes_init()
        for k,v in self.config.get('dagr.mimetypes').items():
            add_mimetype(k,v)

    @staticmethod
    def _update_d(d, u):
        for k, v in u.items():
            if isinstance(v,  collections.abc.Mapping):
                d[k] = DAGR._update_d(d.get(k, {}), v)
            elif isinstance(d.get(k), collections.abc.Iterable):
                if isinstance(v, collections.abc.Iterable):
                    d[k].extend(v)
                else:
                    d[k].append(v)
            else:
                d[k] = v
        return d

    def get_queue(self):
        return self.__work_queue

    def set_queue(self, queue):
        self.__work_queue = queue

    def __build_queue(self):
        logger = logging.getLogger(__name__)
        if self.bulk:
            bulk_queue = {}
            self.filenames = [abspath(fn) for fn in self.filenames]
            for fn in self.filenames:
                logger.debug('Loading file {}'.format(fn))
                with open(fn, 'r') as fh:
                    DAGR._update_d(bulk_queue, json.load(fh))
            wq = DAGR._convert_queue(bulk_queue)
            wq = self.filter_deviants(wq)
        else:
            wq ={}
            for deviant in self.deviants:
                for mode in self.modes:
                    if self.mode_vals:
                        self._update_d(wq, {deviant:{mode:[self.mode_vals]}})
                    else:
                        self._update_d(wq, {deviant:{mode:[]}})

        logger.log(level=4, msg=pformat(wq))
        return wq

    def filter_deviants(self, queue):
        logger = logging.getLogger(__name__)
        if self.filter is None or not self.filter: return queue
        logger.info('Deviant filter: {}'.format(pformat(self.filter)))
        results = dict((k, queue.get(k)) for k in queue.keys() if k in self.filter)
        logger.log(level=5, msg='Filter results: {}'.format(pformat(results)))
        return dict((k, queue.get(k)) for k in queue.keys() if k in self.filter)
    @staticmethod
    def _convert_queue(queue):
        logger = logging.getLogger(__name__)
        converted = queue.get('deviants', {})
        if 'search' in queue:
            search = queue.pop('search')
            DAGR._update_d(converted, {None:{'search':search}})
        for mode in DAGR.MODES:
            data = queue.get(mode)
            if isinstance(data, collections.abc.Mapping):
                for k, v in data.items():
                    DAGR._update_d(converted, {k:{mode:v}})
            elif isinstance(data, collections.abc.Iterable):
                for v in data:
                    DAGR._update_d(converted, {v:{mode:None}})
            else:
                logger.debug('Mode {} not present'.format(mode))
        return converted

    def save_queue(self, path='.queue'):
        with open(path, 'w') as fh:
            json.dump(self.get_queue(), fh)

    def load_queue(self, path='.queue'):
        with open(path, 'r') as fh:
            self.set_queue(json.load(fh))

    def queue_add(self, work):
        return DAGR.__update_d(self.get_queue(), work)

    def keep_running(self):
        return not self.stop_running.is_set()

    def run(self):
        logger = logging.getLogger(__name__)
        if not self.get_queue():
            raise ValueError('Empty work queue')
        wq = self.get_queue()
        reverse = self.config.get('dagr', 'reverse') == True
        logger.info('Reverse order: {}'.format(reverse))
        self.browser_init()
        while self.keep_running():
            if None in wq.keys():
                nd = wq.pop(None)
                self.rip(nd, None)
            try:
                deviant = next(iter(sorted(wq.keys(), reverse=reverse)))
                modes =  wq.pop(deviant)
            except StopIteration:
                break
            self.rip(modes, deviant)

    def rip(self, modes, deviant=None):
        logger = logging.getLogger(__name__)
        group=None
        if deviant:
            try:
                deviant, group = self.get_deviant(deviant)
            except DagrException:
                logger.warning('Deviant {} not found or deactivated!'.format(deviant))
                return
        logger.log(level=5, msg='Running {} : {}'.format(deviant or '', modes))
        directory = abspath(expanduser(self.config.get('dagr', 'outputdirectory')))
        if deviant:
            try:
                self.make_dirs(path_join(directory, deviant))
            except OSError:
                logger.warning('Failed to create deviant diectory {}'.format(deviant), exc_info=True)
                return
        for mode, mode_vals in modes.items():
            if mode_vals:
                for mval in mode_vals:
                    self._rip(mode, deviant, mval, group)
            else:
                self._rip(mode, deviant, group=group)

    def _rip(self, mode, deviant=None, mval=None, group=False):
        logger=logging.getLogger(__name__)
        if group:
            mode_section = 'deviantart.modes.{}'.format(mode)
            group_url_fmt = self.config.get(mode_section, 'group_url_fmt')
            if not group_url_fmt:
                logger.warning('Unsuported mode {} ignored for group {}'.format(mode, deviant))
                return
            folder_url_fmt = self.config.get(mode_section, 'folder_url_fmt')
            folder_regex = self.config.get(mode_section, 'folder_regex')
            folders = self.get_folders(group_url_fmt, folder_regex, deviant)
            for folder in folders:
                self.rip_pages(folder_url_fmt, mode, deviant, folder)
        else:
            url_fmt = self.config.get('deviantart.modes.{}'.format(mode), 'url_fmt')
            self.rip_pages(url_fmt, mode, deviant, mval)

    def rip_pages(self, url_fmt, mode, deviant=None, mval=None):
        logger = logging.getLogger(__name__)
        msg = ''
        if deviant: msg += '{deviant} : '
        msg += '{mode}'
        if mval: msg += ' : {mval}'
        msg_formatted = msg.format(**locals())
        logger.log(level=15, msg='Ripping {}'.format(msg_formatted))
        base_dir = self.get_base_dir(mode, deviant, mval)
        logger.log(level=3, msg=pformat(locals()))
        if not base_dir: return
        if deviant: deviant_lower = deviant.lower()
        lock_path = path_join(base_dir, '.lock')
        try:
            locked = False
            with portalocker.Lock(lock_path, fail_when_locked = True):
                locked = True
                pages = self.get_pages(url_fmt, mode, deviant, mval, msg)
                if not pages:
                    logger.log(level=15, msg="{} had no deviations".format(msg_formatted))
                    return
                logger.log(level=15, msg="Total deviations in {} found: {}".format(msg_formatted, len(pages)))
                self.download_deviantions(base_dir, pages)
            if locked: remove(lock_path)
        except (portalocker.exceptions.LockException,portalocker.exceptions.AlreadyLocked):
            logger.warning('Skipping locked directory {}'.format(base_dir))
        except PermissionError:
            logger.warning('Unable to unlock {}'.format(base_dir))

    def get_pages(self, url_fmt, mode, deviant=None, mval=None, msg=None):
        logger = logging.getLogger(__name__)
        base_url = self.config.get('deviantart', 'baseurl')
        pages = []
        pages_offset = (self.config.get('deviantart.offsets','search')
            if mode == 'search'
                else self.config.get('deviantart.offsets','page'))
        art_regex = self.config.get('deviantart','artregex')
        if deviant:
            deviant_lower = deviant.lower()
        logger.log(level=3, msg=pformat(locals()))
        for page_no in range(0, self.config.get('deviantart', 'maxpages')):
            offset = page_no * pages_offset
            url = url_fmt.format(**locals())
            if msg:
                logger.log(level=15, msg='Crawling {} page {}'.format(msg.format(**locals()), page_no))
            try:
                html = self.get(url).text
            except DagrException:
                    logger.warning(
                        'Could not find {}'.format(msg.format(**locals())))
                    return pages
            if 'unfindable' in self.modes: return
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
        if not self.config.get('dagr', 'reverse'):
            pages.reverse()
        return pages

    def get_folders(self, url_fmt, folder_regex, deviant):
        logger = logging.getLogger(__name__)
        deviant_lower = deviant.lower()
        base_url = self.config.get('deviantart', 'baseurl')
        regex = folder_regex.format(**locals())
        folders = []
        offset = 0
        while True:
            url = url_fmt.format(**locals())
            html = self.get(url).text
            logger.log(level=4, msg='{}'.format(dict(**locals())))
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
            offset += 10
        if self.config.get('dagr', 'reverse'):
            folders.reverse()
        logger.debug('Found folders {}'.format(pformat(folders)))
        return folders

    def download_deviantions(self, base_dir, pages):
        logger = logging.getLogger(__name__)
        fn_cache = self.config.get('dagr.cache', 'filenames')
        dp_cache = self.config.get('dagr.cache', 'downloadedpages')
        files_list, existing_pages = self.load_cache(base_dir,
            filenames = fn_cache,
            downloaded_pages = dp_cache
        )
        if not self.config.get('dagr','overwrite'):
            pages = [x for x in pages if x not in existing_pages]
        page_count = len(pages)
        logger.log(level=15, msg='Total deviations to download: {}'.format(page_count))
        progress = self.config.get('dagr', 'saveprogress')
        test = self.config.get('dagr','test')
        downloaded_pages = []
        for count, link in enumerate(pages, start=1):
            if progress and count % progress == 0:
                self.update_cache(base_dir, fn_cache,files_list)
                self.update_cache(base_dir, dp_cache, existing_pages)
            logger.info('Downloading {} of {} ( {} )'.format(count, page_count, link))
            filename = ""
            filelink = ""
            try:
                filename, filelink = self.find_link(link)
                if test:
                    print(filelink)
                    continue
                self.download_link(filelink, path_join(base_dir, filename), files_list)
            except (KeyboardInterrupt, SystemExit):
                if downloaded_pages:
                    self.update_cache(base_dir, fn_cache,files_list)
                    self.update_cache(base_dir, dp_cache, existing_pages)
                raise
            except DagrException as get_error:
                self.handle_download_error(link, get_error)
                continue
            else:
                if link not in existing_pages:
                    downloaded_pages.append(link)
                    existing_pages.append(link)
        if downloaded_pages or (not path_exists(path_join(base_dir, fn_cache)) and files_list):
            self.update_cache(base_dir, fn_cache, files_list)
        if downloaded_pages:
            self.update_cache(base_dir, dp_cache, existing_pages)
        if downloaded_pages or (
                not path_exists(path_join(base_dir, self.config.get('dagr.cache','artists')))
                and downloaded_pages):
                        self.update_artists(base_dir, existing_pages, files_list)

    def download_link(self, filelink, dest_path, files_list):
        logger = logging.getLogger(__name__)
        overwrite = self.config.get('dagr', 'overwrite')
        retry_exception_names = self.config.get('dagr.retryexceptionnames')
        if not overwrite:
            glob_name = next((fn for fn in files_list if basename(dest_path) in fn), None)
            if glob_name:
                print(glob_name, "exists - skipping")
                return
        response = None
        tries = {}
        while True:
            try:
                response = self.get_response(filelink)
                break
            except Exception as ex:
                except_name = type(ex).__name__
                if except_name in retry_exception_names:
                    logger.debug('Exception while downloading link')
                    if not except_name in tries:
                        tries[except_name] = 0
                    tries[except_name] += 1
                    if tries[except_name]  < 3:
                        continue
                    raise DagrException('Failed to get url: {}'.format(except_name))
                else:
                    logger.error('Exception name {}'.format(except_name))
                    raise
        if not response.status_code == req_codes.ok:
            raise DagrException('incorrect status code - {}'.format(response.status_code))
        content_type = self.response_get_content_type(response)
        logger.debug(content_type)
        if not content_type:
            raise DagrException('missing content-type')
        file_ext = guess_extension(content_type)
        if not file_ext:
            raise DagrException('unknown content-type - {}'.format(content_type))
        dest_path += file_ext
        dest_path = abspath(dest_path)
        file_exists = path_exists(dest_path)
        if file_exists and not overwrite:
            files_list.append(basename(dest_path))
            logger.warning("{} exists - skipping".format(dest_path))
            return
        while True:
            try:
                with open(dest_path, "wb") as fh:
                    fh.write(response.content)
                break
            except Exception as ex:
                except_name = type(ex).__name__
                if except_name in retry_exception_names:
                    logger.debug('Exception while downloading link')
                    if not except_name in tries:
                        tries[except_name] = 0
                    tries[except_name] += 1
                    if tries[except_name]  < 3:
                        continue
                    raise DagrException('Failed to get url: {}'.format(except_name))
                else:
                    logger.error('Exception name {}'.format(except_name))
                    raise
        if response.headers.get("last-modified"):
            # Set file dates to last modified time
            mod_time = mktime(parsedate(response.headers.get("last-modified")))
            utime(dest_path, (mod_time, mod_time))

        files_list.append(basename(dest_path))
        return dest_path

    def response_get_content_type(self, response):
        if "content-type" in response.headers:
            return next(iter(response.headers.get("content-type").split(";")), None)

    def load_cache_file(self, base_dir, cache_file):
        logger = logging.getLogger(__name__)
        full_path = path_join(base_dir, cache_file)
        try:
            if path_exists(full_path):
                with open(full_path, 'r') as filehandle:
                    return json.load(filehandle)
            else:
                logger.log(level=15, msg='Primary {} cache not found'.format(cache_file))
        except:
            logger.warning('Unable to load primary {} cache:'.format(cache_file), exc_info=True)
        full_path += '.bak'
        try:
            if path_exists(full_path):
                with open(full_path, 'r') as filehandle:
                    return json.load(filehandle)
            else:
                logger.log(level=15, msg='Backup {} cache not found'.format(cache_file))
        except:
            logger.warning('Unable to load backup {} cache:'.format(cache_file), exc_info=True)

    def load_cache(self, base_dir, **kwargs):
        logger = logging.getLogger(__name__)
        def filenames():
            logger.log(level=15, msg='Building filenames cache')
            files_list_raw = glob(path_join(base_dir, '*'))
            return [basename(fn) for fn in files_list_raw]
        def downloaded_pages():
            return []
        def artists():
            return {}
        cache_defaults = {
            'filenames': filenames,
            'downloaded_pages': downloaded_pages,
            'artists': artists
        }
        for cache_type, cache_file in kwargs.items():
            cache_contents = self.load_cache_file(base_dir, cache_file)
            if cache_contents:
                yield cache_contents
            else:
                if not cache_type in cache_defaults:
                    raise ValueError('Unkown cache type: {}'.format(cache_type))
                yield cache_defaults[cache_type]()

    def find_link(self, link):
        logger=logging.getLogger(__name__)
        filelink = None
        filename = basename(link)
        mature_error = False
        self.browser.open(link)
        # Full image link (via download link)
        link_text = re.compile("Download( (Image|File))?")
        img_link = None
        for candidate in self.browser.links("a"):
            if link_text.search(candidate.text) and candidate.get("href"):
                img_link = candidate
                break
        if img_link and img_link.get("data-download_url"):
            return (filename, img_link)
        logger.log(level=15, msg="Download link not found, falling back to alternate methods")
        current_page = self.browser.get_current_page()
        # Fallback 1: try meta (filtering blocked meta)
        filesearch = current_page.find("meta", {"property": "og:image"})
        if filesearch:
            filelink = filesearch['content']
            if basename(filelink).startswith("noentrythumb-"):
                filelink = None
                mature_error = True
        if not filelink:
            # Fallback 2: try collect_rid, full
            filesearch = current_page.find("img",
                                           {"collect_rid": True,
                                            "class": re.compile(".*full")})
            if not filesearch:
                # Fallback 3: try collect_rid, normal
                filesearch = current_page.find("img",
                                               {"collect_rid": True,
                                                "class":
                                                    re.compile(".*normal")})
            if filesearch:
                filelink = filesearch['src']
        page_title = current_page.find("span", {"itemprop": "title"})
        if page_title and page_title.text == "Literature":
            filelink = self.browser.get_url()
            return (filename, filelink)
        try:
            if not filelink:
                for pl_func in self.pl_manager.get_funcs('findlink'):
                    filelink = pl_func(pickle.dumps(current_page))
                    if filelink: break
            if not filelink:
                for pl_func in self.pl_manager.get_funcs('findlink_b'):
                    filelink = pl_func(pickle.dumps(self.browser))
                    if filelink: break
        except RecursionError:
            logger.error('Unable to send context to plugin')
        if not filelink:
            if mature_error:
                if self.config.get('deviantart','maturecontent'):
                    raise DagrException('Unable to find downloadable deviation')
                else:
                    raise DagrException('maybe a mature deviation/' +
                                        'not an image')
            else:
                raise DagrException("all attemps to find a link failed")
        return (filename, filelink)

    def handle_download_error(self, link, link_error):
        logger=logging.getLogger(__name__)
        error_string = str(link_error)
        logger.warning("Download error ({}) : {}".format(link, error_string))
        if error_string in self.errors_count:
            self.errors_count[error_string] += 1
        else:
            self.errors_count[error_string] = 1

    def backup_cache_file(self, file_name):
        backup_name = file_name + '.bak'
        if path_exists(file_name):
            if path_exists(backup_name):
                remove(backup_name)
            rename(file_name, backup_name)

    def update_cache(self, base_dir, cache_file, cache_contents):
        logger = logging.getLogger(__name__)
        full_path = path_join(base_dir, cache_file)
        self.backup_cache_file(full_path)
        logger.log(level=15, msg='Updating {} cache'.format(cache_file))
        with open(full_path, 'w') as filehandle:
            json.dump(cache_contents, filehandle, indent=4, sort_keys=True)

    def update_artists(self, base_dir, pages, files_list):
        logger = logging.getLogger(__name__)
        artists = {}
        for page in pages:
                artist_url = dirname(dirname(page))
                artist_name = basename(artist_url)
                url_basename = basename(page)
                try:
                    real_filename = next(fn for fn in files_list if url_basename in fn)
                except StopIteration:
                    logger.error('Cache entry not found {} : {}'.format(page, url_basename), exc_info=True)
                    raise
                if not artist_name in artists:
                    artists[artist_name] = {'Home Page': artist_url, 'Artworks':{}}
                artists[artist_name]['Artworks'][real_filename] = page
        self.update_cache(base_dir, self.config.get('dagr.cache','artists'), artists)

    def browser_init(self):
        if not self.browser:
            # Set up fake browser
            self._set_browser()

    def _set_browser(self):
        user_agents = (
            'Mozilla/5.0 (Windows NT 6.1; WOW64) AppleWebKit/535.1'
            ' (KHTML, like Gecko) Chrome/14.0.835.202 Safari/535.1',
            'Mozilla/5.0 (Windows NT 6.1; WOW64; rv:7.0.1) Gecko/20100101',
            'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_6_8) AppleWebKit/534.50'
            ' (KHTML, like Gecko) Version/5.1 Safari/534.50',
            'Mozilla/4.0 (compatible; MSIE 8.0; Windows NT 6.1; Trident/4.0)',
            'Opera/9.99 (Windows NT 5.1; U; pl) Presto/9.9.9',
            'Mozilla/5.0 (Macintosh; U; Intel Mac OS X 10_5_6; en-US)'
            ' AppleWebKit/530.5 (KHTML, like Gecko) Chrome/ Safari/530.5',
            'Mozilla/5.0 (Windows; U; Windows NT 6.1; en-US) AppleWebKit/533.2'
            ' (KHTML, like Gecko) Chrome/6.0',
            'Mozilla/5.0 (Windows; U; Windows NT 6.1; pl; rv:1.9.1)'
            ' Gecko/20090624 Firefox/3.5 (.NET CLR 3.5.30729)'
        )
        session = req_session()
        session.headers.update({'Referer': 'https://www.deviantart.com/'})
        if self.config.get('deviantart', 'maturecontent'):
            session.cookies.update({'agegate_state': '1'})
        session.mount('https://', req_adapters.HTTPAdapter(max_retries=3))

        self.browser = StatefulBrowser(session=session,
                                       user_agent=choice(user_agents))

    def get_deviant(self, deviant):
        group = False
        html = self.get('https://www.deviantart.com/{}/'.format(deviant)).text
        search = re.search(r'<title>.[A-Za-z0-9-]*', html,
                            re.IGNORECASE).group(0)[7:]
        deviant = re.sub('[^a-zA-Z0-9_-]+', '', search)
        if re.search('<dt class="f h">Group</dt>', html):
            group = True
        return deviant, group

    def get(self, url):
        logger = logging.getLogger(__name__)
        tries = {}
        response = None
        while True:
            try:
                response =  self.get_response(url)
                break
            except Exception as ex:
                logger.debug('Get exception', exc_info=True)
                except_name = type(ex).__name__
                if except_name in self.RETRY_EXCEPTION_NAMES:
                    if not except_name in tries:
                        tries[except_name] = 0
                    tries[except_name] += 1
                    if tries[except_name]  < 3:
                        continue
                    raise DagrException('Failed to get url: {}'.format(except_name))
                else:
                    raise

        if not response.status_code == req_codes.ok:
            raise DagrException('incorrect status code - {}'.format(response.status_code))
        return response

    def get_response(self, url, *args, **kwargs):
        if isinstance(url, Tag):
            if hasattr(url, 'attrs') and 'href' in url.attrs:
                url = self.browser.absolute_url(url['href'])
        return self.browser.session.get(url, *args, timeout=150, **kwargs)

    @staticmethod
    def make_dirs(directory):
        logger = logging.getLogger(__name__)
        if not path_exists(directory):
            makedirs(directory)
            logger.debug('Created dir {}'.format(directory))

    def get_base_dir(self, mode, deviant=None, mval=None):
        logger = logging.getLogger(__name__)
        directory = abspath(expanduser(self.config.get('dagr', 'outputdirectory')))
        if deviant:
            base_dir = path_join(directory, deviant, mode)
        else:
            base_dir =  path_join(directory, mode)
        if mval:
            use_old = self.config.get('dagr.subdirs','useoldformat')
            move = self.config.get('dagr.subdirs','move')
            mval = normpath(mval)
            old_path = path_join(base_dir, mval)
            new_path = path_join(base_dir, basename(mval))
            if use_old:
                base_dir = old_path
                logger.debug('Old format subdirs enabled')
            elif not new_path == old_path and path_exists(old_path):
                if move:
                    if path_exists(new_path):
                        logger.error('Unable to move {}: subfolder {} already exists'.format(old_path, new_path))
                        return
                    logger.log(level=25,msg='Moving {} to {}'.format(old_path, new_path))
                    try:
                        rename(old_path, new_path)
                        rmdir(dirname(old_path))
                        base_dir = new_path
                    except OSError:
                        logger.error('Unable to move subfolder {}:'.format(new_path), exc_info=True)
                        return
                else:
                    logger.debug('Move subdirs not enabled')
            else:
                base_dir = new_path
        base_dir = abspath(base_dir)
        logger.debug('Base dir: {}'.format(base_dir))
        try:
            DAGR.make_dirs(base_dir)
        except OSError:
            logger.error('Unable to create base_dir', exc_info=True)
            return
        logger.log(level=5, msg=pformat(locals()))
        return base_dir

    def print_errors(self):
        logger = logging.getLogger(__name__)
        if self.errors_count:
            logger.warning("Download errors:")
            for error in self.errors_count:
                logger.warning('* {} : {}'.format(error, self.errors_count[error]))

    def get_lock(self, lock_path):
        pass

class DagrException(Exception):
    def __init__(self, value):
        super(DagrException, self).__init__(value)
        self.parameter = value

    def __str__(self):
        return str(self.parameter)
