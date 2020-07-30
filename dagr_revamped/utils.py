import json
import logging
from io import StringIO
from docopt import docopt
from random import choice
from . import __version__
from pprint import pformat, pprint
from .config import DAGRConfig
from .dagr_logging import init_logging, log as dagr_log
from pathlib import Path, PurePosixPath
from mechanicalsoup import StatefulBrowser
from collections.abc import Mapping, Iterable
from requests import(
    session as req_session, adapters as req_adapters
)


def make_dirs(directory):
    logger = logging.getLogger(__name__)
    if not isinstance(directory, Path):
        directory = Path(directory)
    if not directory.exists():
        directory.mkdir(parents=True)
        logger.debug('Created dir {}'.format(directory))

def strip_topdir(directory):
    if not isinstance(directory, Path):
        directory = Path(directory)
    dirparts = directory.parts[2:]
    return Path(*dirparts)


def get_base_dir(config, mode, deviant=None, mval=None):
    logger = logging.getLogger(__name__)
    directory = config.output_dir
    if deviant:
        base_dir = directory.joinpath(deviant, mode)
    else:
        base_dir = Path(directory, mode)
    if mval:
        mval = Path(mval)
        use_old = config.get('dagr.subdirs', 'useoldformat')
        move = config.get('dagr.subdirs', 'move')
        old_path = base_dir.joinpath(mval)
        new_path = base_dir.joinpath(mval.name)
        if use_old:
            base_dir = old_path
            logger.debug('Old format subdirs enabled')
        elif not new_path == old_path and old_path.exists():
            if move:
                if new_path.exists():
                    logger.error('Unable to move {}: subfolder {} already exists'.format(
                        old_path, new_path))
                    return
                logger.log(level=25, msg='Moving {} to {}'.format(
                    old_path, new_path))
                try:
                    parent = old_path.parent
                    old_path.rename(new_path)
                    parent.rmdir()
                    base_dir = new_path
                except OSError:
                    logger.error('Unable to move subfolder {}:'.format(
                        new_path), exc_info=True)
                    return
            else:
                logger.debug('Move subdirs not enabled')
        else:
            base_dir = new_path
    base_dir = base_dir.resolve()
    logger.debug('Base dir: {}'.format(base_dir))
    try:
        make_dirs(base_dir)
    except OSError:
        logger.error('Unable to create base_dir', exc_info=True)
        return
    logger.log(level=5, msg=pformat(locals()))
    return base_dir


def buffered_file_write(json_content, fname):
    if not isinstance(fname, Path):
        fname = Path(fname)
    buffer = StringIO()
    json.dump(json_content, buffer, indent=4, sort_keys=True)
    buffer.seek(0)
    fname.write_text(buffer.read())


def update_d(d, u):
    for k, v in u.items():
        if isinstance(v,  Mapping):
            d[k] = update_d(d.get(k, {}), v)
        elif isinstance(d.get(k), Iterable):
            if isinstance(v, Iterable):
                d[k].extend(v)
            else:
                d[k].append(v)
        else:
            d[k] = v
    return d


def convert_queue(config, queue):
    logger = logging.getLogger(__name__)
    queue = {k.lower(): v for k, v in queue.items()}
    converted = queue.get('deviants', {})
    if None in converted:
        update_d(converted, {None: converted.pop(None)})
    for ndmode in config.get('deviantart', 'ndmodes').split(','):
        if ndmode in queue:
            mvals = queue.pop(ndmode)
            update_d(converted, {None: {ndmode: mvals}})
    for mode in config.get('deviantart', 'modes').split(','):
        data = queue.get(mode)
        if isinstance(data, Mapping):
            for k, v in data.items():
                update_d(converted, {k: {mode: v}})
        elif isinstance(data, Iterable):
            for v in data:
                update_d(converted, {v: {mode: None}})
        else:
            logger.debug('Mode {} not present'.format(mode))
    return converted


def load_bulk_files(files):
    logger = logging.getLogger(__name__)
    bulk_queue = {}
    files = [Path(fn).resolve() for fn in files]
    for fn in files:
        logger.debug('Loading file {}'.format(fn))
        with fn.open('r') as fh:
            update_d(bulk_queue, json.load(fh))
    return bulk_queue


def filter_deviants(dfilter, queue):
    if dfilter is None or not dfilter:
        return queue
    logger = logging.getLogger(__name__)
    logger.info('Deviant filter: {}'.format(pformat(dfilter)))
    results = dict((k, queue.get(k)) for k in queue.keys() if k in dfilter)
    logger.log(level=5, msg='Filter results: {}'.format(pformat(results)))
    return dict((k, queue.get(k)) for k in queue.keys() if k in dfilter)


def compare_size(dest, content):
    logger = logging.getLogger(__name__)
    if not isinstance(dest, Path):
        dest = Path(dest)
    if not dest.exists():
        return False
    current_size = dest.stat().st_size
    best_size = len(content)
    if not current_size < best_size:
        return True
    logger.info('Current file {} is smaller by {} bytes'.format(
        dest, best_size - current_size))
    return False


def create_browser(mature=False, user_agent=None):
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

    if mature:
        session.cookies.update({'agegate_state': '1'})
    session.mount('https://', req_adapters.HTTPAdapter(max_retries=3))

    if user_agent is None:
        user_agent = choice(user_agents)

    return StatefulBrowser(
        session=session,
        user_agent=user_agent)


def unlink_lockfile(lockfile):
    logger = logging.getLogger(__name__)
    if not isinstance(lockfile, Path):
        lockfile = Path(lockfile)
    if lockfile.exists():
        try:
            lockfile.unlink()
        except PermissionError:
            logger.warning('Unable to unlock {}'.format(lockfile.parent))


def shorten_url(url):
    p = PurePosixPath()
    for u in Path(url).parts[2:]:
        p = p.joinpath(u)
    return str(p)


def artist_from_url(url):
    artist_url_p = PurePosixPath(url).parent.parent
    artist_name = artist_url_p.name
    shortname = PurePosixPath(url).name
    return (artist_url_p, artist_name, shortname)



def save_json(fpath, data):
    if isinstance(data, set):
        data = list(data)
    p = fpath if isinstance(fpath, Path) else Path(fpath)
    buffer = StringIO()
    json.dump(data, buffer, indent=4, sort_keys=True)
    if p.exists():
        backup = p.with_suffix('.bak')
        if backup.exists():
            backup.unlink()
        p.rename(backup)
    buffer.seek(0)
    p.write_text(buffer.read())
    logging.getLogger(__name__).log(level=15, msg=f"Saved {len(data)} items to {fpath}")


def load_json(fpath):
    p = fpath if isinstance(fpath, Path) else Path(fpath)
    with p.open('r') as f:
        return json.load(f)


class DAGRUtilsCli():

    """
{} v{}

Usage:
dagr-utils.py renamedeviant OLD NEW [-v|-vv|--debug=DEBUGLVL] FILENAMES ...
dagr-utils.py shortenurlcache [-v|-vv|--debug=DEBUGLVL] FILENAMES ...
dagr-utils.py finddupes [--filter=FILTER] [-v|-vv|--debug=DEBUGLVL] FILENAMES
dagr-utils.py updatedirscache [--filter=FILTER] [-v|-vv|--debug=DEBUGLVL] FILENAMES


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
            'filenames': arguments.get('FILENAMES'),
            'filter': arguments.get('--filter'),
            'old': arguments.get('OLD'),
            'new': arguments.get('NEW'),
        }


class DAGRUtils():

    def __init__(self, **kwargs):
        from .lib import DAGRCache
        self.__logger = logging.getLogger(__name__)
        self.__utils_cmd_maping = {
            'shortenurlcache': self.shorten_url_cache,
            'renamedeviant': self.rename_deviant,
            'finddupes': self.find_dupes,
            'updatedirscache': self.update_dirs_cache
        }
        self.__utils_cmd = next(
            (cmd for cmd in self.__utils_cmd_maping.keys() if kwargs.get(cmd)), None)
        self.__config = kwargs.get('config') or DAGRConfig()
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
                if mode_vals:
                    for mval in mode_vals:
                        callback(mode, deviant, mval)
                else:
                    callback(mode, deviant)

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
        base_dir = get_base_dir(self.__config, mode, deviant, mval)
        print('Scanning {}'.format(base_dir))
        cache = self.__cache(self.__config, base_dir)
        renamed = cache.rename_deviant(old, new)
        print('Renamed {} deviations'.format(renamed))
        if renamed > 0:
            cache.save()
        return renamed

    def find_dupes(self):
        self.walk_queue(self._find_dupes, True)
        duplicates = {k: v for k,
                      v in self.__global_files_mapping.items() if len(v) > 1}
        print(f'Total duplicates: {len(duplicates)}')
        od = self.__config.output_dir
        of = od.joinpath('.duplicates.json')
        buffered_file_write(duplicates, of)

    def _find_dupes(self, mode, deviant, mval=None):
        base_dir = get_base_dir(self.__config, mode, deviant, mval)
        rel_path = str(strip_topdir(base_dir))
        print(f'Scanning {rel_path}')
        cache = self.__cache(self.__config, base_dir)
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
        rel_path = str(strip_topdir(base_dir))
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
