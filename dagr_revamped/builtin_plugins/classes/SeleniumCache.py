from dagr_revamped.utils import load_json, save_json
from pathlib import Path
import logging

logger = logging.getLogger(__name__)


class SlugCache():
    def __init__(self, slug, local, remote):
        self.__slug = slug
        self.__local_values = set()
        self.__remote_values = set()
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
                            load_json(cpath))
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

    def __flush_local(self):
        self.__local_values.update(self.__remote_values)
        save_json(self.__caches.get('local primary'), self.__local_values)

    def __flush_remote(self):
        self.__remote_values.update(self.__local_values)
        save_json(self.__caches.get('remote primary'), self.__remote_values)

    def flush(self):
        logger.log(level=15, msg=f"Flushing {self.__slug}")
        self.__load()
        if self.local_stale:
            self.__flush_local()
        if self.remote_stale:
            self.__flush_remote()

    def query(self):
        result = set()
        result.update(self.__local_values)
        result.update(self.__remote_values)
        return result

    def update(self, values):
        if not isinstance(values, set):
            values = set(values)

        if len(values - self.__local_values) > 0:
            self.__local_values.update(values)
            self.__flush_local()


class SeleniumCache():
    def __init__(self, app_config, config):
        output_dir = app_config.output_dir
        self.__local_cache = Path(config.get(
            'local_cache_path', '~/.config/.dagr/seleniumcache')).expanduser().resolve()
        self.__remote_cache = output_dir.joinpath(
            config.get('remote_cache_path', '.selenium')).expanduser().resolve()
        self.__caches = {}
        self.__flushed = {}

        if not self.__local_cache.exists():
            self.__local_cache.mkdir(parents=True)

        if not self.__remote_cache.exists():
            self.__remote_cache.mkdir()

    def flush(self, slug=None):
        if slug is None:
            for s in self.__caches.keys():
                if not self.__flushed.get(s, False) == True:
                    self.__caches.get(s).flush()
        else:
            cache = self.__caches.get(slug)
            if cache:
                cache.flush()
                self.__flushed[slug] = True

    def query(self, slug):
        if not slug in self.__caches.keys():
            self.__caches[slug] = SlugCache(
                slug, self.__local_cache, self.__remote_cache)
        return self.__caches.get(slug).query()

    def update(self, slug, values):
        if not slug in self.__caches.keys():
            self.__caches[slug] = SlugCache(
                slug, self.__local_cache, self.__remote_cache)
        self.__caches.get(slug).update(values)
        self.__flushed[slug] = False
