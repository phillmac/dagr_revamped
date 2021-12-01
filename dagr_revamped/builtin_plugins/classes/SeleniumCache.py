import logging
from pathlib import Path
from time import time

from pybreaker import CircuitBreakerError

logger = logging.getLogger(__name__)


def deep_tuple(x):
    if isinstance(x, tuple):
        return x
    return tuple(deep_tuple(i) if isinstance(i, list) else i for i in x)


class SlugCache():
    def __init__(self, slug, local_io, remote_io, remote_breaker):
        self.__id = ''.join(random.choices(string.ascii_uppercase + string.digits, k=5))
        logger.debug('Created SlugCache %s', self.__id)
        self.__slug = slug
        self.__local_io = local_io
        self.__remote_io = remote_io
        self.__local_values = set()
        self.__remote_values = set()
        self.__remote_breaker = remote_breaker
        self.__filename = f"{self.__slug}.json"
        self.__load()

    def __del__(self):
        logger.debug('Destroying SlugCache %s', self.__id)

    def __load(self, ignore_breaker=False):
        update_local = self.__local_io.load_primary_or_backup(
            self.__filename, warn_not_found=False)
        if update_local:
            self.__local_values.update(i if isinstance(
                i, str) else deep_tuple(i) for i in update_local)

        update_remote = None
        logger.log(15, 'loading remote %s', self.__filename)
        try:
            if ignore_breaker:
                if self.__remote_io.exists(fname=self.__filename, update_cache=False):
                    update_remote = self.__remote_io.load_json(
                        self.__filename, log_errors=True)
            else:
                if self.__remote_breaker.call(self.__remote_io.exists, fname=self.__filename, update_cache=False):
                    update_remote = self.__remote_breaker.call(
                        self.__remote_io.load_json, self.__filename, log_errors=True)

            if update_remote:
                logger.log(15, 'Loaded %s items from remote %s',
                           len(update_remote), self.__filename)
                self.__remote_values.update(i if isinstance(
                    i, str) else deep_tuple(i) for i in update_remote)
        except Exception:
            logger.exception('Failed to load remote %s cache', self.__slug)

    @property
    def local_stale(self):
        return len(self.__remote_values - self.__local_values) > 0

    @property
    def remote_stale(self):
        return len(self.__local_values - self.__remote_values) > 0

    def __flush_local(self, force_overwrite=False):
        if not force_overwrite:
            self.__local_values.update(self.__remote_values)
        self.__local_io.save_json(
            fname=self.__filename, content=self.__local_values)

    def __flush_remote(self, force_overwrite=False, ignore_breaker=False):
        if not force_overwrite:
            self.__remote_values.update(self.__local_values)

        if ignore_breaker:
            self.__remote_io.save_json(
                fname=self.__filename, content=self.__remote_values)
        else:
            self.__remote_breaker.call(
                self.__remote_io.save_json, fname=self.__filename, content=self.__remote_values)

    def flush(self, force_overwrite=False):
        logger.log(level=15, msg=f"Flushing {self.__slug}")
        if not force_overwrite:
            self.__load()
        if force_overwrite or self.local_stale:
            self.__flush_local(force_overwrite=force_overwrite)
        try:
            if force_overwrite or self.remote_stale:
                self.__flush_remote(force_overwrite=force_overwrite)
        except CircuitBreakerError:
            logger.warning('Unable to flush remote: CircuitBreakerError')

    def query(self):
        result = set()
        result.update(self.__local_values)
        result.update(self.__remote_values)
        return result

    def update(self, values):
        if isinstance(values, dict):
            values = set([tuple(values.items())])
        elif not isinstance(values, set):
            values = set(values)

        if len(values - self.__local_values) > 0:
            self.__local_values.update(values)
            self.__flush_local()

    def remove(self, values):
        if isinstance(values, dict):
            values = set([tuple(values.items())])
        elif not isinstance(values, set):
            values = set(values)

        local_before = set(self.__local_values)
        remote_before = set(self.__remote_values)

        self.__local_values.difference_update(values)
        self.__remote_values.difference_update(values)

        if not local_before == self.__local_values:
            logger.log(level=15, msg='flushing removed local cache values')
            self.__flush_local(force_overwrite=True)

        if not remote_before == self.__remote_values:
            logger.log(level=15, msg='flushing removed remote cache values')
            self.__flush_remote(force_overwrite=True)


class SeleniumCache():
    def __init__(self, local_io, remote_io, remote_breaker):
        self.__local_io = local_io
        self.__remote_io = remote_io
        self.__remote_breaker = remote_breaker
        self.__caches = {}
        self.__flushed = {}

    def flush(self, slug=None, force_overwrite=False):
        if slug is None:
            for s in self.__caches.keys():
                if not self.__flushed.get(s, False) == True:
                    self.__caches.get(s).flush()
        else:
            cache = self.__caches.get(slug)
            if cache:
                cache.flush(force_overwrite=force_overwrite)
                self.__flushed[slug] = True

    def query(self, slug):
        if not slug in self.__caches.keys():
            self.__caches[slug] = SlugCache(
                slug, self.__local_io, self.__remote_io, self.__remote_breaker)
        return self.__caches.get(slug).query()

    def update(self, slug, values):
        if not slug in self.__caches.keys():
            self.__caches[slug] = SlugCache(
                slug, self.__local_io, self.__remote_io, self.__remote_breaker)
        self.__caches.get(slug).update(values)
        self.__flushed[slug] = False

    def remove(self, slug, values):
        if not slug in self.__caches.keys():
            self.__caches[slug] = SlugCache(
                slug, self.__local_io, self.__remote_io,  self.__remote_breaker)
        self.__caches.get(slug).remove(values)
        self.__flushed[slug] = False

    def unload(self, slug):
        if slug in self.__caches.keys():
            if not self.__flushed.get(slug, False) == True:
                self.__caches.get(slug).flush()
                self.__flushed[slug] = True
            del self.__caches[slug]
