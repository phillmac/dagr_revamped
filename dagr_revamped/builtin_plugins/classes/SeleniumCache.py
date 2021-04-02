from dagr_revamped.utils import load_json, save_json
import logging
from pathlib import Path

import pybreaker

logger = logging.getLogger(__name__)


def deep_tuple(x):
    if isinstance(x, tuple):
        return x
    return tuple(deep_tuple(i) if isinstance(i, list) else i for i in x)


class SlugCache():
    def __init__(self, slug, local, remote, remote_breaker):
        self.__slug = slug
        self.__local_values = set()
        self.__remote_values = set()
        self.__remote_breaker = remote_breaker
        self.__values = {
            'remote_values': self.__remote_values,
            'local_values': self.__local_values
        }
        self.__loaded = set()

        self.__caches = {
            'remote primary': remote.joinpath(slug).with_suffix('.json'),
            'remote backup': remote.joinpath(slug).with_suffix('.bak'),
            'local primary': local.joinpath(slug).with_suffix('.json'),
            'local backup': local.joinpath(slug).with_suffix('.bak')
        }
        self.__load()

    def __load(self):
        for ctype in ['remote', 'local']:
            for cname in [k for k in self.__caches.keys() if ctype in k]:
                cpath = self.__caches.get(cname)
                try:
                    if cpath.exists():
                        self.__values.get(f"{ctype}_values").update(
                            i if isinstance(i, str) else deep_tuple(i) for i in load_json(cpath))
                        self.__loaded.add(cname)
                        break
                except:
                    logger.log(
                        level=25, msg=f"Failed to load {self.__slug} {cname} cache {cpath}", exc_info=True)
        if not len(self.__loaded) > 0:
            logger.warning(f"Unable to load any caches for {self.__slug}")
        else:
            logger.log(level=15, msg=f"Loaded caches {self.__loaded}")

    @property
    def local_stale(self):
        return len(self.__remote_values - self.__local_values) > 0

    @property
    def remote_stale(self):
        return len(self.__local_values - self.__remote_values) > 0

    def __flush_local(self, force_overwrite=False):
        if not force_overwrite:
            self.__local_values.update(self.__remote_values)
        save_json(self.__caches.get('local primary'), self.__local_values)

    def __flush_remote(self, force_overwrite=False, ignore_breaker=False):
        def do_flush_remote(remote_values, local_values, caches, force_overwrite):
            if not force_overwrite:
                remote_values.update(local_values)
            save_json(caches.get('remote primary'), remote_values)

        @self.__remote_breaker
        def do_flush_remote_breaker(remote_values, local_values, caches, force_overwrite):
            do_flush_remote(remote_values, local_values,
                            caches, force_overwrite)

        if ignore_breaker:
            do_flush_remote(self.__remote_values,
                            self.__local_values, self.__caches, force_overwrite)
        else:
            do_flush_remote_breaker(
                self.__remote_values, self.__local_values, self.__caches, force_overwrite)

    def flush(self, force_overwrite=False):
        logger.log(level=15, msg=f"Flushing {self.__slug}")
        if not force_overwrite:
            self.__load()
        if force_overwrite or self.local_stale:
            self.__flush_local(force_overwrite=force_overwrite)
        try:
            if force_overwrite or self.remote_stale:
                self.__flush_remote(force_overwrite=force_overwrite)
        except pybreaker.CircuitBreakerError:
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
    def __init__(self, app_config, config):
        output_dir = app_config.output_dir
        self.__local_cache = Path(config.get(
            'local_cache_path', '~/.config/.dagr/seleniumcache')).expanduser().resolve()
        self.__remote_cache = output_dir.joinpath(
            config.get('remote_cache_path', '.selenium')).expanduser().resolve()
        fail_max = config.get('remote_breaker_fail_max', 1)
        reset_timeout = config.get('remote_breaker_reset_timeout', 10)
        self.__remote_breaker = pybreaker.CircuitBreaker(
            fail_max=fail_max, reset_timeout=reset_timeout)
        logger.log(
            level=15, msg=f"Remote cache cb - fail_max: {fail_max} reset_timeout: {reset_timeout}")

        self.__caches = {}
        self.__flushed = {}

        if not self.__local_cache.exists():
            self.__local_cache.mkdir(parents=True)

        if not self.__remote_cache.exists():
            self.__remote_cache.mkdir()

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
                slug, self.__local_cache, self.__remote_cache, self.__remote_breaker)
        return self.__caches.get(slug).query()

    def update(self, slug, values):
        if not slug in self.__caches.keys():
            self.__caches[slug] = SlugCache(
                slug, self.__local_cache, self.__remote_cache, self.__remote_breaker)
        self.__caches.get(slug).update(values)
        self.__flushed[slug] = False

    def remove(self, slug, values):
        if not slug in self.__caches.keys():
            self.__caches[slug] = SlugCache(
                slug, self.__local_cache, self.__remote_cache,  self.__remote_breaker)
        self.__caches.get(slug).remove(values)
        self.__flushed[slug] = False

    def unload(self, slug):
        if slug in self.__caches.keys():
            if not self.__flushed.get(slug, False) == True:
                self.__caches.get(slug).flush()
                self.__flushed[slug] = True
            del self.__caches[slug]
