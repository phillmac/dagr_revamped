import json
import logging
import re
from collections.abc import Iterable, Mapping
from io import StringIO
from pathlib import Path, PurePosixPath
from pprint import pformat, pprint
from random import choice

from mechanicalsoup import StatefulBrowser
from requests import adapters as req_adapters
from requests import session as req_session

logger = logging.getLogger(__name__)


def make_dirs(directory):
    if not isinstance(directory, Path):
        directory = Path(directory).resolve()
    if not directory.exists():
        directory.mkdir(parents=True)
        logger.debug('Created dir {}'.format(directory))


def strip_topdirs(config, directory):
    if not isinstance(directory, Path):
        directory = Path(directory).resolve()

    index = len(config.output_dir.parts)
    dirparts = directory.parts[index:]
    return Path(*dirparts)


def get_base_dir(config, mode, deviant=None, mval=None):
    directory = config.output_dir.expanduser().resolve()
    if deviant:
        base_dir = directory.joinpath(deviant, mode)
    else:
        base_dir = directory.joinpath(mode)
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
    base_dir = base_dir
    logger.debug('Base dir: {}'.format(base_dir))
    try:
        make_dirs(base_dir)
    except OSError:
        logger.error('Unable to create base_dir', exc_info=True)
        return
    logger.log(level=5, msg=pformat(locals()))
    return base_dir, base_dir.relative_to(directory)


def buffered_file_write(json_content, fname):
    if not isinstance(fname, Path):
        fname = Path(fname)
    buffer = StringIO()
    json.dump(json_content, buffer, indent=4, sort_keys=True)
    buffer.seek(0)
    temp = fname.with_suffix('.tmp')
    temp.write_text(buffer.read())
    temp.rename(fname)


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
                update_d(converted, {k.lower(): {mode: v}})
        elif isinstance(data, Iterable):
            for v in data:
                update_d(converted, {v.lower(): {mode: None}})
        else:
            logger.debug('Mode {} not present'.format(mode))
    return converted


def load_bulk_files(files):
    bulk_queue = {}
    for fp in files:
        logger.debug(f"Loading file {fp}")
        update_d(bulk_queue, load_json(fp))
    return bulk_queue


def get_bulk_files_contents(config):
    output_dir = config.output_dir
    filenames = config.get('dagr.bulk.filenames', 'load').split(',')
    filepaths = (output_dir.joinpath(fn)
                 for fn in filenames)
    files_list = (fp for fp in filepaths if fp.exists())
    return load_bulk_files(files_list)


# def __update_bulk_list_entry(bulk, mode, deviant=None, mval=None):
#     updated = False
#     if deviant is None:
#         entry = bulk.get(mode)
#         if entry is None:
#             entry = []
#             bulk[mode] = entry

#         if not mval in entry:
#             entry.append(mval)
#             updated = True

#     else:
#         bulk_deviants = bulk.get('deviants', {})
#         entry = bulk_deviants.get(
#             deviant, bulk_deviants.get(deviant.lower(), {}))

#         if not deviant.lower() in [d.lower() for d in bulk_deviants]:
#             bulk_deviants[deviant] = entry
#             updated = True

#         if not mode in entry:
#             entry[mode] = []
#             updated = True

#         if not mval is None and not mval in entry[mode]:
#             entry[mode].append(mval)
#             updated = True

#     return updated


def save_bulk(config, bulk):
    save_json(config.get('dagr.bulk.filenames', 'save'), bulk)

def prune_dict_duplicates(d):
    for k, v in d.items():
        if isinstance(v,  Mapping):
            d[k] = prune_dict_duplicates(v)
        elif isinstance(v, Iterable):
            d[k] = list(set(v))
        else:
            d[k] = v
    return d

def update_bulk_list(config, entries, force_save=False):
    # bulk = convert_queue(config,
    #                      get_bulk_files_contents(config))

    bulk = get_bulk_files_contents(config)
    blen = len(bulk['gallery']) + len(bulk['favs'])
    lowercase_deviants = {
        'gallery': [d.lower() for d in bulk.get('gallery', [])],
        'favs': [d.lower() for d in bulk.get('favs', [])]
    }

    for e in entries:
        mode = e.get('mode')
        if mode in ['gallery', 'favs']:
            deviant = e.get('deviant')
            if not (deviant.lower() in lowercase_deviants.get(mode)):
                bulk[mode].append(deviant)
                logger.log(level=15, msg="Added {}".format(e))
    prune_dict_duplicates(bulk)
    delta = len(bulk['gallery']) + len(bulk['favs']) - blen
    # updated = False
    #     if __update_bulk_list_entry(bulk, **e):
    #         updated =  True
    #         logger.log(level=15, msg="Added {}".format(e))

    if force_save or delta > 0:
        save_bulk(config, bulk)
        logger.info(f"Added {delta} deviants to bulk gallery list")


# def update_bulk_list(config, mode, deviant=None, mval=None):
#     bulk = convert_queue(config,
#                          get_bulk_files_contents(config))

#     updated = __update_bulk_list_entry(bulk, mode, deviant, mval)

#     if updated:
#         save_bulk(config, bulk)

def filter_deviants(dfilter, queue):
    if dfilter is None or not dfilter:
        return queue
    dfilter_lower = [df.lower() for df in dfilter]
    logger.info('Deviant filter: {}'.format(pformat(dfilter_lower)))
    results = dict((k, queue.get(k))
                   for k in queue.keys() if str(k).lower() in dfilter)
    logger.log(level=15, msg='Filter results: {}'.format(pformat(results)))
    return results


def compare_size(dest, content):
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


def backup_cache_file(fpath):
    if not isinstance(fpath, Path):
        fpath = Path(fpath)
    fpath = fpath.resolve()
    backup = fpath.with_suffix('.bak')
    if fpath.exists():
        if backup.exists():
            backup.unlink()
        fpath.rename(backup)


def unlink_lockfile(lockfile):
    if not isinstance(lockfile, Path):
        lockfile = Path(lockfile)
    if lockfile.exists():
        try:
            lockfile.unlink()
        except (PermissionError, OSError):
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


def save_json(fpath, data, do_backup=True):
    if isinstance(data, set):
        data = list(data)
    fp = ensure_path(fpath)
    if do_backup:
        backup_cache_file(fp)
    buffered_file_write(data, fp)
    logger.log(
        level=15, msg=f"Saved {len(data)} items to {fp}")


def load_json(fpath):
    fp = ensure_path(fpath)
    buffer = StringIO(fp.read_text())
    return json.load(buffer)


def ensure_path(fpath):
    return (fpath if isinstance(fpath, Path) else Path(fpath)).resolve()


def load_primary_or_backup(fpath, use_backup=True, warn_not_found=True):
    if not isinstance(fpath, Path):
        raise Exception('Path required')
    backup = fpath.with_suffix('.bak')
    try:
        if fpath.exists():
            return load_json(fpath)
        elif warn_not_found:
            logger.log(
                level=15, msg='Primary {} cache not found'.format(fpath.name))
    except json.JSONDecodeError:
        logger.warning(
            'Unable to decode primary {} cache:'.format(fpath.name), exc_info=True)
        fpath.replace(fpath.with_suffix('.bad'))
    except:
        logger.warning(
            'Unable to load primary {} cache:'.format(fpath.name), exc_info=True)
    try:
        if use_backup:
            if backup.exists():
                return load_json(backup)
        elif warn_not_found:
            logger.log(
                level=15, msg='Backup {} cache not found'.format(backup.name))
    except:
        logger.warning(
            'Unable to load backup {} cache:'.format(backup.name), exc_info=True)


def dump_html(fpath, page, content):
    if not fpath.exists():
        fpath.mkdir(parents=True)
    fname = (fpath
             .joinpath(re.sub('[^a-zA-Z0-9_-]+', '_', shorten_url(page)))
             .with_suffix('.html'))
    logger.info('Dumping html to {}'.format(fname))
    fname.write_bytes(content)
