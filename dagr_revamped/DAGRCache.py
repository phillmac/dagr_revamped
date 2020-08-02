import logging
from pathlib import Path, PurePosixPath
from pprint import pformat
from time import time

import portalocker

from .utils import (artist_from_url, get_base_dir, load_primary_or_backup,
                    save_json, shorten_url, unlink_lockfile)

logger = logging.getLogger(__name__)


class DAGRCache():

    @staticmethod
    def get_queue(config, mode, deviant, mval=None):
        base_dir = get_base_dir(config, mode, deviant, mval)
        return DAGRCache(config, base_dir, queue_only=True)

    @staticmethod
    def get_cache(config, mode, deviant, mval=None):
        base_dir = get_base_dir(config, mode, deviant, mval)
        return DAGRCache(config, base_dir)

    def __init__(self, dagr_config, base_dir, queue_only=False):
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
        self.existing_pages = next(
            self.__load_cache(existing_pages=self.ep_name))
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

        if queue_only:
            self.__files_list = None
            self.artists = None
            self.last_crawled = None
        else:
            self.__files_list = next(self.__load_cache(filenames=self.fn_name))
            self.artists = next(self.__load_cache(artists=self.artists_name))
            self.last_crawled = next(self.__load_cache(
                last_crawled=self.crawled_name))

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
        except (portalocker.exceptions.LockException, portalocker.exceptions.AlreadyLocked, OSError) as ex:
            logger.warning(f"Skipping locked directory {self.base_dir}")
            raise DagrCacheLockException(ex)

    def __exit__(self, exc_type, exc_val, exc_tb):
        self.__lock.release()
        if self.__lock._acquire_count == 0:
            unlink_lockfile(self.__lock_path)

    @property
    def files_list(self):
        return [f for f in self.__files_list if not f in self.__excluded_fnames]

    def __load_cache_file(self, cache_file, use_backup=True, warn_not_found=True):
        full_path = self.base_dir.joinpath(cache_file)
        return load_primary_or_backup(full_path)

    def __load_cache(self, use_backup=True, warn_not_found=True, **kwargs):
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
        logger.log(level=5, msg=pformat(locals()))

    def __ep_exists(self):
        return self.base_dir.joinpath(self.ep_name).exists()

    def __fn_exists(self):
        return self.base_dir.joinpath(self.fn_name).exists()

    def __artists_exists(self):
        return self.base_dir.joinpath(self.artists_name).exists()

    def __settings_exists(self):
        return self.base_dir.joinpath(self.settings_name).exists()

    def __update_cache(self, cache_file, cache_contents, do_backup=True):
        full_path = self.base_dir.joinpath(cache_file)
        save_json(full_path, cache_contents, do_backup)

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
            try:
                rfn = self.real_filename(shortname)
            except StopIteration:
                logger.error('Cache entry not found {} : {} : {}'.format(
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
        logger.log(level=5, msg=pformat(locals()))

    def save_extras(self, full_crawl):
        self.save_nolink()
        self.save_queue()
        self.save_premium()
        if full_crawl is True or full_crawl is False:
            self.save_crawled(full_crawl)

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
            logger.log(level=5, msg=f"Removed {page} from queue")
        if page in self.no_link:
            self.no_link.remove(page)
            logger.log(level=5, msg=f"Removed {page} from no-link list")

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
        logger.log(
            level=5, msg='Checking for lowercase link {}'.format(page))
        return page.lower() in (l.lower() for l in self.existing_pages)

    def filter_links(self, links):
        return [l for l in links if not self.check_link(l)]

    def add_filename(self, fn):
        if fn in self.__files_list:
            logger.log(
                level=5, msg='{} allready in filenames cache'.format(fn))
        else:
            self.__files_list.append(fn)

    def real_filename(self, shortname):
        return next(fn for fn in self.files_list if shortname.lower() in fn.lower())


class DagrCacheLockException(Exception):
    pass
