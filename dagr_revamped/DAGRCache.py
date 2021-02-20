import logging
import re
from copy import copy
from pathlib import Path, PurePosixPath
from pprint import pformat
from time import time
from platform import node as get_hostname

import portalocker

from .utils import artist_from_url, get_base_dir, shorten_url, unlink_lockfile
from .DAGRHTTPIo import DAGRHTTPIo

logger = logging.getLogger(__name__)


class DAGRCache():

    @staticmethod
    def with_queue_only(config, mode, deviant, mval=None, dagr_io=None,
                        warn_not_found=None, preload_fileslist_policy=None):
        return DAGRCache.get_cache(
            config, mode, deviant, mval=mval, dagr_io=dagr_io,
            load_files=['existing_pages', 'no_link',
                        'queue', 'premium', 'httperrors'],
            warn_not_found=warn_not_found, preload_fileslist_policy=preload_fileslist_policy)

    @staticmethod
    def with_artists_only(config, mode, deviant, mval=None, dagr_io=None,
                          warn_not_found=None, preload_fileslist_policy=None):
        return DAGRCache.get_cache(
            config, mode, deviant, mval=mval, dagr_io=dagr_io, load_files=[
                'artists'],
            warn_not_found=warn_not_found, preload_fileslist_policy=preload_fileslist_policy)

    @staticmethod
    def with_filenames_only(config, mode, deviant, mval=None, dagr_io=None,
                            warn_not_found=None, preload_fileslist_policy=None):
        return DAGRCache.get_cache(
            config, mode, deviant, mval=mval, dagr_io=dagr_io, load_files=[
                'files_list'],
            warn_not_found=warn_not_found, preload_fileslist_policy=preload_fileslist_policy)

    @staticmethod
    def with_nolink_only(config, mode, deviant, mval=None, dagr_io=None,
                         warn_not_found=None, preload_fileslist_policy=None):
        return DAGRCache.get_cache(
            config, mode, deviant, mval=mval, dagr_io=dagr_io, load_files=[
                'no_link'],
            warn_not_found=warn_not_found, preload_fileslist_policy=preload_fileslist_policy)

    @staticmethod
    def get_cache(config, mode, deviant, mval=None, dagr_io=None,
                  load_files=None, warn_not_found=None, preload_fileslist_policy=None):
        base_dir, rel_dir = get_base_dir(config, mode, deviant, mval)
        cache_io = (dagr_io if dagr_io is not None else DAGRHTTPIo).create(
            base_dir, rel_dir, config)
        return DAGRCache(config, cache_io, load_files=load_files, warn_not_found=warn_not_found, preload_fileslist_policy=preload_fileslist_policy)

    def __init__(self, dagr_config, cache_io, load_files=None, warn_not_found=None, preload_fileslist_policy=None):
        self.dagr_config = dagr_config
        self.base_dir = cache_io.base_dir
        self.rel_dir = cache_io.rel_dir
        self.__cache_io = cache_io
        self.__lock = None
        self.__lock_path = None
        self.__warn_not_found = warn_not_found
        config_preload_fileslist_policy = self.dagr_config.get(
            'dagr.cache', 'preload_fileslist_policy')

        if config_preload_fileslist_policy == 'prohibit':
            self.preload_fileslist_policy = 'disabled'
        else:
            self.preload_fileslist_policy = preload_fileslist_policy if not preload_fileslist_policy is None else config_preload_fileslist_policy
            self.preload_http_endpoint = self.dagr_config.get(
                'dagr.cache', 'preload_http_endpoint')
        self.json_http_endpoint = self.dagr_config.get(
            'dagr.cache', 'json_http_endpoint')
        self.settings_name = self.dagr_config.get(
            'dagr.cache', 'settings') or '.settings'
        self.settings = next(self.__load_cache(
            use_backup=False, settings=self.settings_name))
        self.__use_short_urls = self.settings.get('shorturls')
        self.fn_name = self.settings.get('filenames', '.filenames')
        self.ep_name = self.settings.get(
            'downloadedpages', '.dagr_downloaded_pages')
        self.artists_name = self.settings.get('artists', '.artists')
        self.crawled_name = self.settings.get('crawled', '.crawled')
        self.nolink_name = self.settings.get('nolink', '.nolink')
        self.queue_name = self.settings.get('queue', '.queue')
        self.premium_name = self.settings.get('premium', '.premium')
        self.httperrors_name = self.settings.get('httperrors', '.httperrors')

        if load_files is None:
            load_files = [
                'existing_pages', 'no_link', 'queue', 'premium', 'httperrors', 'files_list', 'artists', 'last_crawled'
            ]

        self.__excluded_fnames = [
            '.lock',
            self.settings_name,
            self.fn_name,
            self.ep_name,
            self.artists_name,
            self.crawled_name,
            self.nolink_name,
            self.queue_name,
            self.premium_name,
            self.httperrors_name
        ]

        self.__excluded_fnames_regex = list(
            map(re.compile, map(re.escape, self.__excluded_fnames)))
        self.__excluded_fnames_regex.append(re.compile(r'.*\.tmp'))

        self.__existing_pages_lower = None

        self.__existing_pages = None if not 'existing_pages' in load_files else self.__load_ep()
        self.__no_link = None if not 'no_link' in load_files else self.__load_nolink()
        self.__queue = None if not 'queue' in load_files else self.__load_queue()
        self.__premium = None if not 'premium' in load_files else self.__load_premium()
        self.__httperrors = None if not 'httperrors' in load_files else self.__load_httperrors()
        self.__files_list = None if not 'files_list' in load_files else self.__load_fileslist()
        self.__artists = None if not 'artists' in load_files else self.__load_artists()
        self.__last_crawled = None if not 'last_crawled' in load_files else self.__load_lastcrawled()

        self.__files_list_lower = None
        self.downloaded_pages = []

        self.__queue_stale = False
        self.__premium_stale = False
        self.__nolink_stale = False
        self.__httperrors_stale = False

        if not self.__existing_pages is None and not self.__use_short_urls == self.dagr_config.get('dagr.cache', 'shorturls'):
            self.__convert_urls()

    def __enter__(self):
        try:
            if not self.__lock:
                self.__lock_path = self.base_dir.joinpath('.lock')
                self.__lock = portalocker.RLock(
                    self.__lock_path, fail_when_locked=True)
            self.__lock.acquire()
            return self
        except (portalocker.exceptions.LockException, portalocker.exceptions.AlreadyLocked, OSError) as ex:
            logger.warning(f"Skipping locked directory {self.base_dir}")
            raise DagrCacheLockException(ex)

    def __exit__(self, exc_type, exc_val, exc_tb):
        self.__lock.release()
        if self.__lock._acquire_count == 0:
            unlink_lockfile(self.__lock_path)

    def files_gen(self):
        if self.__files_list is None:
            self.__files_list = self.__load_fileslist()
        return (f for f in self.__files_list if not any(r.match(f) for r in self.__excluded_fnames_regex))

    @ property
    def files_list(self):
        return list(self.files_gen())

    @ property
    def existing_pages(self):
        if self.__existing_pages is None:
            self.__existing_pages = self.__load_ep()
        return self.__existing_pages

    @ property
    def existing_pages_lower(self):
        if self.__existing_pages_lower is None:
            logger.log(
                level=15, msg='Generating lowercase existing pages cache')
            self.__existing_pages_lower = [
                l.lower() for l in self.existing_pages]
        return self.__existing_pages_lower

    @ property
    def artists(self):
        if self.__artists is None:
            self.__artists = self.__load_artists()
        return self.__artists

    @ property
    def last_crawled(self):
        if self.__last_crawled is None:
            self.__last_crawled = self.__load_lastcrawled()
        return self.__last_crawled

    @ property
    def cache_io(self):
        return self.__cache_io

    def __load_cache_file(self, cache_file, use_backup=True, warn_not_found=True):
        return self.__cache_io.load_primary_or_backup(cache_file, use_backup=use_backup, warn_not_found=warn_not_found)

    def __load_cache(self, use_backup=True, warn_not_found=True, default=None, **kwargs):
        def filenames():
            logger.log(level=15, msg='Building filenames cache')
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
            'premium': lambda: [],
            'httperrors': lambda: {}
        }
        for cache_type, cache_file in kwargs.items():
            cache_contents = self.__load_cache_file(
                cache_file, use_backup=use_backup, warn_not_found=warn_not_found)
            if cache_contents:
                yield cache_contents
            else:
                if not default is None:
                    yield default
                else:
                    if not cache_type in cache_defaults:
                        raise ValueError(
                            'Unkown cache type: {}'.format(cache_type))
                    yield cache_defaults[cache_type]()
        logger.log(level=5, msg=pformat(locals()))

    def __load_ep(self):
        logger.log(level=15, msg='Loading existing pages')
        return next(
            self.__load_cache(
                existing_pages=self.ep_name,
                warn_not_found=True if self.__warn_not_found is None else self.__warn_not_found))

    def __load_nolink(self):
        logger.log(level=15, msg='Loading nolink')
        return next(self.__load_cache(
            no_link=self.nolink_name,
            warn_not_found=False if self.__warn_not_found is None else self.__warn_not_found))

    def __load_queue(self):
        logger.log(level=15, msg='Loading queue')
        return next(self.__load_cache(
            queue=self.queue_name,
            warn_not_found=False if self.__warn_not_found is None else self.__warn_not_found))

    def __load_premium(self):
        logger.log(level=15, msg='Loading premium')
        return next(self.__load_cache(
            premium=self.premium_name,
            warn_not_found=False if self.__warn_not_found is None else self.__warn_not_found))

    def __load_fileslist(self):
        logger.log(level=15, msg='Populating files list cache')
        files_in_dir = set()
        filenames_default = None
        if self.preload_fileslist_policy == 'enable':
            if self.preload_http_endpoint:
                try:
                    files_in_dir.update(
                        fn for fn in self.__cache_io.list_dir() if not fn in self.__excluded_fnames)
                    filenames_default = []
                    logger.log(
                        level=15, msg=f"Added {len(files_in_dir)} entrys to preload list")
                except:
                    logger.warn(
                        'Unable to fetch filenames preload list', exc_info=True)

        files_in_dir.update(next(self.__load_cache(
            filenames=self.fn_name,
            warn_not_found=True if self.__warn_not_found is None else self.__warn_not_found,
            default=filenames_default
        )))
        return files_in_dir

    def __load_artists(self):
        logger.log(level=4, msg='Loading artists')
        return next(self.__load_cache(
            artists=self.artists_name,
            warn_not_found=False if self.__warn_not_found is None else self.__warn_not_found))

    def __load_httperrors(self):
        logger.log(level=4, msg='Loading http errors')
        return next(self.__load_cache(
            httperrors=self.httperrors_name,
            warn_not_found=False if self.__warn_not_found is None else self.__warn_not_found))

    def __load_lastcrawled(self):
        logger.log(level=4, msg='Loading last crawled')
        return next(self.__load_cache(
            last_crawled=self.crawled_name,
            warn_not_found=False if self.__warn_not_found is None else self.__warn_not_found))

    def __ep_exists(self):
        return self.__cache_io.exists(self.ep_name, update_cache=False)

    def __fn_exists(self):
        return self.__cache_io.exists(self.fn_name, update_cache=False)

    def __artists_exists(self):
        return self.__cache_io.exists(self.artists_name, update_cache=False)

    def __settings_exists(self):
        return self.__cache_io.exists(self.settings_name, update_cache=False)

    def __update_cache(self, cache_file, cache_contents, do_backup=True):
        if isinstance(cache_contents, set):
            cache_contents = list(cache_contents)
        self.__cache_io.save_json(cache_file, cache_contents, do_backup)

    def __convert_urls(self):
        logger.warning(
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
        logger.log(
            15, 'Sorting {} artist pages'.format(len(updated_pages)))
        for page in updated_pages:
            artist_url_p, artist_name, shortname = artist_from_url(page)
            err = f"Cache entry not found {self.base_dir} : {page} : {shortname}"
            try:
                rfn = self.real_filename(shortname)
                if rfn is None:
                    logger.error(err)
                    raise Exception(err)
            except StopIteration:
                logger.error(err, exc_info=True)
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
                logger.log(4, 'Changing {} to {}'.format(ep, result))
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
        fix_fn = fn_missing and bool(self.__files_list)
        fix_ep = ep_missing and bool(self.__existing_pages)
        fix_artists = artists_missing and bool(
            self.__files_list) and bool(self.__existing_pages)
        if settings_missing:
            self.__update_cache(self.settings_name, self.settings, False)
        if self.downloaded_pages or fix_fn:
            self.__update_cache(self.fn_name, self.__files_list)
        if self.downloaded_pages or fix_ep:
            self.__update_cache(self.ep_name, self.__existing_pages)
        if save_artists:
            if self.downloaded_pages or fix_artists or save_artists == 'force':
                self.update_artists(save_artists == 'force')
        logger.log(level=5, msg=pformat(locals()))

    def save_extras(self, full_crawl):
        if self.__nolink_stale:
            self.save_nolink()
        if self.__queue_stale:
            self.save_queue()
        if self.__premium_stale:
            self.save_premium()
        if self.__httperrors_stale:
            self.save_httperrors()
        if not full_crawl is None:
            self.save_crawled(full_crawl)

    def save_nolink(self):
        if not self.__no_link is None:
            self.__update_cache(self.nolink_name, self.__no_link)
        self.__nolink_stale = False

    def save_queue(self):
        if not self.__queue is None:
            self.__update_cache(self.queue_name, self.__queue)
        self.__queue_stale = False

    def save_premium(self):
        if not self.__premium is None:
            self.__update_cache(self.premium_name, self.__premium)
        self.__premium_stale = False

    def save_httperrors(self):
        if not self.__httperrors is None:
            self.__update_cache(self.httperrors_name, self.__httperrors)
        self.__httperrors_stale = False

    def save_crawled(self, full_crawl=False):
        if full_crawl:
            self.last_crawled['full'] = time()
        else:
            self.last_crawled['short'] = time()
        self.__update_cache(self.crawled_name, self.last_crawled)

    def add_premium(self, page):
        if self.__premium is None:
            self.__premium = self.__load_premium()
        if page in self.__premium:
            return
        self.remove_page_extras(page, 'premium')
        self.__premium.append(page)
        self.__premium_stale = True

    def get_premium(self):
        if self.__premium is None:
            self.__premium = self.__load_premium()
        return copy(self.__premium)

    def get_httperrors(self):
        if self.__httperrors is None:
            self.__httperrors = self.__load_httperrors()
        return copy(self.__httperrors)

    @ property
    def httperrors_exclude(self):
        return set([*self.downloaded_pages, *self.existing_pages])

    def add_httperror(self, page, page_error):
        if self.__httperrors is None:
            self.__httperrors = self.__load_httperrors()
        if not page in self.httperrors_exclude:
            self.__httperrors_stale = True
            if not page in self.__httperrors:
                self.__httperrors[page] = []
            self.__httperrors[page].append({
                'host': get_hostname(),
                'time': time(),
                'error_code': page_error.http_code
            })

    @ property
    def nl_exclude(self):
        if self.__no_link is None:
            self.__no_link = self.__load_nolink()
        return set([*self.downloaded_pages, *self.existing_pages, *self.__no_link, *self.__premium, *self.__httperrors])

    def add_nolink(self, page):
        if self.__no_link is None:
            self.__no_link = self.__load_nolink()
        if self.__premium is None:
            self.__premium = self.__load_premium()
        if self.__httperrors is None:
            self.__httperrors = self.__load_httperrors()
        if page in self.nl_exclude:
            return
        self.__nolink_stale = True
        self.remove_page_extras(page, 'nolink')
        self.__no_link.append(page)

    def remove_nolink(self, pages):
        if self.__no_link is None:
            self.__no_link = self.__load_nolink()
        remove = set([p for p in (pages if isinstance(pages, list)
                                  else list(pages)) if p in self.__no_link])
        rcount = len(remove)
        if rcount > 0:
            self.__nolink_stale = True
            self.__no_link = list(set(self.__no_link) - remove)
        return rcount

    def prune_nolink(self):
        if self.__no_link is None:
            self.__no_link = self.__load_nolink()
        nlcount = len(self.__no_link)
        keep = set(self.__no_link) - self.nl_exclude
        kcount = len(keep)
        delta = nlcount - kcount
        if not delta == 0:
            self.__nolink_stale = True
            self.__no_link = list(keep)
        return delta

    def get_nolink(self):
        if self.__no_link is None:
            self.__no_link = self.__load_nolink()
        return copy(self.__no_link)

    def get_queue(self):
        if self.__queue is None:
            self.__queue = self.__load_queue()
        return copy(self.__queue)

    @ property
    def q_exclude(self):
        if self.__queue is None:
            self.__queue = self.__load_queue()
        if self.__premium is None:
            self.__premium = self.__load_premium()
        if self.__httperrors is None:
            self.__httperrors = self.__load_httperrors()
        return set([*self.downloaded_pages, *self.existing_pages, *self.__queue, *self.__premium, *self.__httperrors])

    def add_queue(self, page):
        if self.__queue is None:
            self.__queue = self.__load_queue()
        if page in self.q_exclude:
            return
        self.__queue_stale = True
        self.__queue.append(page)

    def update_queue(self, pages):
        if self.__queue is None:
            self.__queue = self.__load_queue()
        exclude = self.q_exclude
        enqueue = [p for p in pages if not p in exclude]
        if enqueue:
            self.__queue_stale = True
            self.__queue += enqueue
            self.save_queue()
        return len(enqueue)

    def prune_queue(self):
        if self.__queue is None:
            self.__queue = self.__load_queue()
        qcount = len(self.__queue)
        keep = set(self.__queue) - self.q_exclude
        kcount = len(keep)
        delta = qcount - kcount
        if not delta == 0:
            self.__queue_stale = True
            self.__queue = list(keep)
        return delta

    def remove_page_extras(self, page, reason):
        if self.__no_link is None:
            self.__no_link = self.__load_nolink()
        if self.__queue is None:
            self.__queue = self.__load_queue()
        if self.__premium is None:
            self.__premium = self.__load_premium()
        if self.__httperrors is None:
            self.__httperrors = self.__load_httperrors()
        if page in self.__queue:
            self.__queue.remove(page)
            self.__queue_stale = True
            logger.log(level=5, msg=f"Removed {page} from queue")
        if not reason == 'nolink' and page in self.__no_link:
            self.__no_link.remove(page)
            self.__nolink_stale = True
            logger.log(level=5, msg=f"Removed {page} from no-link list")
        if not reason == 'premium' and page in self.__premium:
            self.__premium.remove(page)
            self.__premium_stale = True
            logger.log(level=5, msg=f"Removed {page} from premium list")
        if not reason == 'httperror' and page in self.__httperrors:
            del self.__httperrors[page]
            self.__httperrors_stale = True
            logger.log(level=5, msg=f"Removed {page} from httperrors list")

    def add_link(self, page):
        self.remove_page_extras(page, 'found')
        if self.__use_short_urls:
            page = shorten_url(page)
        if page not in self.existing_pages:
            self.downloaded_pages.append(page)
            self.existing_pages.append(page)
            if page in self.__queue:
                self.__queue.re
        elif self.dagr_config.get('dagr', 'overwrite'):
            self.downloaded_pages.append(page)

    def check_link(self, page):
        if self.__use_short_urls:
            page = shorten_url(page)
        if page in self.existing_pages:
            return True
        # logger.log(
        #     level=5, msg='Checking for lowercase link {}'.format(page))
        return page.lower() in self.existing_pages_lower

    def filter_links(self, links):
        return [l for l in links if not self.check_link(l)]

    def add_filename(self, fn):
        if self.__files_list is None:
            self.__files_list = self.__load_fileslist()
        if fn in self.__files_list:
            logger.log(
                level=15, msg='{} already in filenames cache'.format(fn))
        else:
            self.__files_list.add(fn)
            self.__cache_io.update_fn_cache(fn)
            # if not self.__files_list_lower is None:
            # self.__files_list_lower[fn.lower()] = fn

    def real_filename(self, shortname):
        sn_lower = shortname.lower()
        return next(fn for fn in self.files_list if sn_lower in fn.lower())

        # if self.__files_list_lower is None:
        #     logger.log(level=15, msg='Generating lowercase fn cache')
        #     lower_gen = ((fn.lower(), fn)
        #             for fn in self.files_gen())
        #     self.__files_list_lower = dict(lower_gen)
        #     logger.log(level=15, msg=f"Generated {len(self.__files_list_lower)} lowercase fn cache items")

        # entry = self.__files_list_lower.get(sn_lower, None)
        # if not entry is None:
        #     logger.log(level=15, msg=f"Got lowercase fn cache hit {entry} for {sn_lower}")
        #     return entry

        # fll_values = self.__files_list_lower.values()

        # for rfn, lfn in lower_gen:
        #     self.__files_list_lower[lfn] = rfn
        #     if lfn == sn_lower:
        #         logger.log(level=15, msg=f"Got lcfn gen hit {rfn} for {sn_lower}")
        #         return rfn

        return None

    def prune_filename(self, fname):
        self.__files_list.discard(fname)


class DagrCacheLockException(Exception):
    pass
