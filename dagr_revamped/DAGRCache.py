import logging
from copy import copy
from pathlib import Path, PurePosixPath
from platform import node as get_hostname
from pprint import pformat
from time import time

import portalocker

from .utils import (artist_from_url, get_base_dir, load_primary_or_backup,
                    save_json, shorten_url, unlink_lockfile)

logger = logging.getLogger(__name__)


class DAGRCache():

    @staticmethod
    def with_queue_only(config, mode, deviant, mval=None, warn_not_found=None):
        base_dir = get_base_dir(config, mode, deviant, mval)
        return DAGRCache(config, base_dir, queue_only=True, warn_not_found=warn_not_found)

    @staticmethod
    def get_cache(config, mode, deviant, mval=None, warn_not_found=None):
        base_dir = get_base_dir(config, mode, deviant, mval)
        return DAGRCache(config, base_dir, warn_not_found=warn_not_found)

    def __init__(self, dagr_config, base_dir, queue_only=False, warn_not_found=None):
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
        self.httperrors_name = self.settings.get('httperrors', '.httperrors')
        self.existing_pages = next(
            self.__load_cache(
                existing_pages=self.ep_name,
                warn_not_found=True if warn_not_found is None else warn_not_found))
        self.__no_link = next(self.__load_cache(
            no_link=self.nolink_name,
            warn_not_found=False if warn_not_found is None else warn_not_found))
        self.__queue = next(self.__load_cache(
            queue=self.queue_name,
            warn_not_found=False if warn_not_found is None else warn_not_found))
        self.__premium = next(self.__load_cache(
            premium=self.premium_name,
            warn_not_found=False if warn_not_found is None else warn_not_found))
        self.__httperrors = next(self.__load_cache(
            httperrors=self.httperrors_name,
            warn_not_found=False if warn_not_found is None else warn_not_found))

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

        self.downloaded_pages = []

        self.__queue_stale = False
        self.__premium_stale = False
        self.__nolink_stale = False
        self.__httperrors_stale = False

        if queue_only:
            self.__files_list = None
            self.artists = None
            self.last_crawled = None
        else:
            self.__files_list = next(self.__load_cache(
                filenames=self.fn_name,
                warn_not_found=True if warn_not_found is None else warn_not_found))
            self.artists = next(self.__load_cache(
                artists=self.artists_name,
                warn_not_found=False if warn_not_found is None else warn_not_found))
            self.last_crawled = next(self.__load_cache(
                last_crawled=self.crawled_name,
                warn_not_found=False if warn_not_found is None else warn_not_found))

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
        return load_primary_or_backup(full_path, use_backup=use_backup, warn_not_found=warn_not_found)

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
            'premium': lambda: [],
            'httperrors': lambda: {}
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
        if page in self.__premium:
            return
        self.remove_page_extras(page, 'premium')
        self.__premium.append(page)
        self.__premium_stale = True

    def get_premium(self):
        return copy(self.__premium)

    def get_httperrors(self):
        return copy(self.__httperrors)

    @property
    def httperrors_exclude(self):
        return set([*self.downloaded_pages, *self.existing_pages])

    def add_httperror(self, page, page_error):
        if not page in self.httperrors_exclude:
            self.__httperrors_stale = True
            if not page in self.__httperrors:
                self.__httperrors[page] = []
            self.__httperrors[page].append({
                'host': get_hostname(),
                'time': time(),
                'error_type': str(type(page_error))
            })

    @property
    def nl_exclude(self):
        return set([*self.downloaded_pages, *self.existing_pages, *self.__no_link, *self.__premium, *self.__httperrors])

    def add_nolink(self, page):
        if page in self.nl_exclude:
            return
        self.__nolink_stale = True
        self.remove_page_extras(page, 'nolink')
        self.__no_link.append(page)

    def remove_nolink(self, pages):
        remove = set([p for p in (pages if isinstance(pages, list)
                                  else list(pages)) if p in self.__no_link])
        rcount = len(remove)
        if rcount > 0:
            self.__nolink_stale = True
            self.__no_link = list(set(self.__no_link) - remove)
        return rcount

    def prune_nolink(self):
        nlcount = len(self.__no_link)
        keep = set(self.__no_link) - self.nl_exclude
        kcount = len(keep)
        delta = nlcount - kcount
        if not delta == 0:
            self.__nolink_stale = True
            self.__no_link = list(keep)
        return delta

    def get_nolink(self):
        return copy(self.__no_link)

    def get_queue(self):
        return copy(self.__queue)

    @property
    def q_exclude(self):
        return set([*self.downloaded_pages, *self.existing_pages, *self.__queue, *self.__premium, *self.__httperrors])

    def add_queue(self, page):
        if page in self.q_exclude:
            return
        self.__queue_stale = True
        self.__queue.append(page)

    def update_queue(self, pages):
        exclude = self.q_exclude
        enqueue = [p for p in pages if not p in exclude]
        if enqueue:
            self.__queue_stale = True
            self.__queue += enqueue
            self.save_queue()
        return len(enqueue)

    def prune_queue(self):
        qcount = len(self.__queue)
        keep = set(self.__queue) - self.q_exclude
        kcount = len(keep)
        delta = qcount - kcount
        if not delta == 0:
            self.__queue_stale = True
            self.__queue = list(keep)
        return delta

    def remove_page_extras(self, page, reason):
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
        if self.settings.get('shorturls'):
            page = shorten_url(page)
        if page not in self.existing_pages:
            self.downloaded_pages.append(page)
            self.existing_pages.append(page)
            if page in self.__queue:
                self.__queue.re
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