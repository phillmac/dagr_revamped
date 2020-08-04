import logging

from docopt import docopt

from . import __version__
from .config import DAGRConfig
from .dagr_logging import init_logging
from .dagr_logging import log as dagr_log
from .DAGRCache import DAGRCache, DagrCacheLockException
from .DAGRManager import DAGRManager
from .utils import (buffered_file_write, convert_queue, filter_deviants,
                    get_base_dir, load_bulk_files, strip_topdirs)

logger = logging.getLogger(__name__)


class DAGRUtilsCli():

    """
{} v{}

Usage:
dagr-utils.py renamedeviant OLD NEW [-v|-vv|--debug=DEBUGLVL] FILENAMES ...
dagr-utils.py shortenurlcache [-v|-vv|--debug=DEBUGLVL] FILENAMES ...
dagr-utils.py finddupes [--filter=FILTER] [-v|-vv|--debug=DEBUGLVL] FILENAMES
dagr-utils.py updatedirscache [--filter=FILTER] [-v|-vv|--debug=DEBUGLVL] FILENAMES
dagr-utils.py findnolinks [--filter=FILTER] [-v|-vv|--debug=DEBUGLVL] FILENAMES
dagr-utils.py fixnolinks [--filter=FILTER] [-v|-vv|--debug=DEBUGLVL] FILENAMES




Options:
    -v --verbose                            Show more detail, -vv for debug
    --debug=DEBUGLVL                        Show still more detail

    """
    NAME = __package__
    VERSION = __version__

    def __init__(self, config):
        self.arguments = arguments = docopt(self.__doc__.format(
            self.NAME, self.VERSION), version=self.VERSION)
        ll_arg = logging.WARN
        try:
            ll_arg = -1 if arguments.get('--quiet') else (int(arguments.get('--debug')) if arguments.get(
                '--debug') else (int(arguments.get('--verbose') if arguments.get('--verbose') else 0)))
        except Exception:
            ll_arg = 0
            dagr_log(__name__, logging.WARN, 'Unrecognized debug level')

        self.args = {
            'log_level': ll_arg,
            'renamedeviant':  arguments.get('renamedeviant'),
            'shortenurlcache': arguments.get('shortenurlcache'),
            'finddupes': arguments.get('finddupes'),
            'updatedirscache': arguments.get('updatedirscache'),
            'findnolinks': arguments.get('findnolinks'),
            'fixnolinks': arguments.get('fixnolinks'),
            'filenames': arguments.get('FILENAMES'),
            'filter': arguments.get('--filter'),
            'old': arguments.get('OLD'),
            'new': arguments.get('NEW'),
        }


class DAGRUtils():

    def __init__(self, **kwargs):
        self.__utils_cmd_maping = {
            'shortenurlcache': self.shorten_url_cache,
            'renamedeviant': self.rename_deviant,
            'finddupes': self.find_dupes,
            'updatedirscache': self.update_dirs_cache,
            'findnolinks': self.find_nolinks,
            'fixnolinks': self.fix_nolinks
        }
        self.__utils_cmd = next(
            (cmd for cmd in self.__utils_cmd_maping.keys() if kwargs.get(cmd)), None)
        self.__config = kwargs.get('config') or DAGRConfig()
        self.__manager = DAGRManager(self.__config)
        self.__cache = kwargs.get('cache') or DAGRCache
        self.__filenames = kwargs.get('filenames')
        self.__filter = None if kwargs.get('filter') is None else [
            s.strip().lower() for s in kwargs.get('filter').split(',')]
        self.__global_files_mapping = {}
        self.__global_dirs_mapping = {}
        self.__global_deviant_dirs_cache = dict((str(d.name).lower(), d) for d in (
            di for di in self.__config.output_dir.iterdir() if di.is_dir()))
        self.__kwargs = kwargs

    def handle_utils_cmd(self):
        self.__utils_cmd_maping.get(self.__utils_cmd)()

    def build_queue(self):
        wq = load_bulk_files(self.__filenames)
        wq = convert_queue(self.__config, wq)
        wq = filter_deviants(self.__filter, wq)
        return wq

    def walk_queue(self, callback, inc_nd=False):
        wq = self.build_queue()
        if None in wq.keys():
            _nd = wq.pop(None)
        while True:
            try:
                deviant = next(iter(sorted(wq.keys())))
                modes = wq.pop(deviant)
            except StopIteration:
                break
            deviant = str(self.__global_deviant_dirs_cache.get(deviant))
            for mode, mode_vals in modes.items():
                try:
                    if mode_vals:
                        for mval in mode_vals:
                            callback(mode, deviant, mval)
                    else:
                        callback(mode, deviant)
                except DagrCacheLockException:
                    pass

    def shorten_url_cache(self):
        wq = self.build_queue()

    def rename_deviant(self):
        old = self.__kwargs.get('old').lower()
        new = self.__kwargs.get('new').lower()
        print('Renaming from {} to {}'.format(old, new))
        wq = self.build_queue()
        if None in wq.keys():
            wq.pop(None)
        while True:
            try:
                deviant = next(iter(sorted(wq.keys())))
                modes = wq.pop(deviant)
            except StopIteration:
                break
            for mode, mode_vals in modes.items():
                if mode_vals:
                    for mval in mode_vals:
                        self._rename_deviant(mode, deviant, mval)
                else:
                    self._rename_deviant(mode, deviant)

    def _rename_deviant(self, mode, deviant, mval=None):
        old = self.__kwargs.get('old').lower()
        new = self.__kwargs.get('new').lower()
        with self.__cache.get_cache(self.__config, mode, deviant, mval, warn_not_found=False) as cache:
            print('Scanning {}'.format(cache.base_dir))
            renamed = cache.rename_deviant(old, new)
            print('Renamed {} deviations'.format(renamed))
            if renamed > 0:
                cache.save()
            return renamed

    def fix_nolinks(self):
        self.__manager.get_browser().do_login()
        self.walk_queue(self._fix_nolinks, True)

    def _fix_nolinks(self, mode, deviant, mval=None):
        dagr = self.__manager.get_dagr()
        try:
            with self.__cache.get_cache(self.__config, mode, deviant, mval, warn_not_found=False) as cache:
                pages = cache.get_nolink()
                nlcount = len(pages)
                if nlcount > 0:
                    logger.info(f"Ripping {len(pages)} pages to {str(strip_topdirs(self.__config, cache.base_dir))}")
                    dagr.process_deviations(cache, pages)
                    rcount = cache.prune_nolink()
                    logger.log(
                    level=15, msg=f"Removed {rcount} pages from no-link list")
                    cache.save_extras(None)
                    dagr.print_errors()
                    dagr.print_dl_total()
        except DagrCacheLockException:
            pass

    def find_nolinks(self):
        self.walk_queue(self._find_nolinks, True)

    def _find_nolinks(self, mode, deviant, mval=None):
        with self.__cache.get_cache(self.__config, mode, deviant, mval) as cache:
            nlcount = len(cache.get_nolink())
            if nlcount > 0:
                print(str(strip_topdirs(self.__config, cache.base_dir)), nlcount)

    def find_dupes(self):
        self.walk_queue(self._find_dupes, True)
        duplicates = {k: v for k,
                      v in self.__global_files_mapping.items() if len(v) > 1}
        print(f'Total duplicates: {len(duplicates)}')
        od = self.__config.output_dir
        of = od.joinpath('.duplicates.json')
        buffered_file_write(duplicates, of)

    def _find_dupes(self, mode, deviant, mval=None):
        with self.__cache.get_cache(self.__config, mode, deviant, mval, warn_not_found=False) as cache:
            rel_path = str(strip_topdirs(self.__config, cache.base_dir))
            print(f'Scanning {rel_path}')
            for file_name in cache.files_list:
                if not file_name in self.__global_files_mapping:
                    self.__global_files_mapping[file_name] = []
                self.__global_files_mapping[file_name].append(str(rel_path))
            print(f'Total files: {len(self.__global_files_mapping)}')

    def update_dirs_cache(self):
        print('Scanning dirs')
        self.walk_queue(self._update_dirs_cache, True)
        print('Saving cache')
        od = self.__config.output_dir
        of = od.joinpath('.dirs.json')
        buffered_file_write(self.__global_dirs_mapping, of)

    def _update_dirs_cache(self, mode, deviant, mval=None):
        base_dir = get_base_dir(self.__config, mode, deviant, mval)
        rel_path = str(strip_topdirs(self.__config, base_dir))
        print(f'Scanning {rel_path}')
        dir_lower = rel_path.lower()
        if not dir_lower in self.__global_dirs_mapping:
            self.__global_dirs_mapping[dir_lower] = rel_path


def main():
    config = DAGRConfig()
    cli = DAGRUtilsCli(config)
    config.set_args(cli.args)
    init_logging(config)
    utils = DAGRUtils(config=config, **cli.args)
    utils.handle_utils_cmd()


if __name__ == '__main__':
    main()
