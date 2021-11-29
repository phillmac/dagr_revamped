import logging
from email.utils import parsedate
from json import JSONDecodeError
from os import scandir, utime
from pathlib import Path, PurePath, PurePosixPath
from time import mktime

import portalocker

from .exceptions import DagrCacheLockException
from .utils import load_json, save_json, unlink_lockfile

logger = logging.getLogger(__name__)


def get_fname(fname=None, dest=None):
    if fname is None:
        if dest is None:
            raise TypeError('Either fname or dest arg is required')
        return dest.name
    return fname


def get_dir_name(dir_name=None, src=None):
    if dir_name is None:
        if src is None:
            raise TypeError('Either dir_name or src arg is required')
        return src.name
    return dir_name


def get_new_dir_name(new_dir_name=None, dest=None):
    if new_dir_name is None:
        if dest is None:
            raise TypeError(
                'Either new_dir_name or dest arg is required')
        return dest.name
    return new_dir_name


class DAGRIo():
    @staticmethod
    def create(base_dir, rel_dir, config):
        if isinstance(base_dir, PurePath):
            base_dir = Path(base_dir)
        if not base_dir.is_absolute():
            base_dir = config.output_dir.joinpath(base_dir)
        return DAGRIo(base_dir, rel_dir)

    def __init__(self, base_dir, rel_dir):
        self.__base_dir = base_dir
        self.__rel_dir = rel_dir
        self.__rel_dir_name = str(PurePosixPath(rel_dir))
        self.__lock = None
        self.__lock_path = None

    @ property
    def base_dir(self):
        return self.__base_dir

    @ property
    def rel_dir(self):
        return self.__rel_dir

    @ property
    def rel_dir_name(self):
        return self.__rel_dir_name

    def __enter__(self):
        return self

    def __exit__(self, type, value, tb):
        if self.__lock:
            self.release_lock()

    def list_dir(self):
        return (i.name for i in scandir(self.__base_dir))

    def load_json(self, fname, log_errors=None):
        return load_json(self.__base_dir.joinpath(fname))

    def save_json(self, fname, content, do_backup=True, log_errors=None):
        return save_json(self.__base_dir.joinpath(fname), content, do_backup=do_backup)

    def exists(self, fname=None, dest=None, subdir=None, update_cache=None):
        return self.__get_subpath(fname, dest, subdir).exists()

    def replace(self, dest_fname=None, src_fname=None, dest=None, src=None, dest_subdir=None, src_subdir=None):
        return self.__get_subpath(dest_fname, dest, dest_subdir).replace(
            self.__get_subpath(src_fname, src, src_subdir))

    def load_primary_or_backup(self, fname, use_backup=True, warn_not_found=True):
        if not isinstance(fname, str):
            raise Exception('fname arg must be an instance of str')

        fpath = PurePosixPath(fname)
        backup = fpath.with_suffix('.bak').name

        if self.exists(fname, update_cache=False):
            try:
                return self.load_json(fname)
            except JSONDecodeError:
                logger.warning(
                    f"Unable to decode primary {fname} cache:", exc_info=True)
                self.replace(fname, fpath.with_suffix('.bad').name)
            except:
                logger.warning(
                    f"Unable to load primary {fname} cache:", exc_info=True)
        elif warn_not_found:
            logger.log(
                level=15, msg=f"Primary {fname} cache not found")
        try:
            if use_backup:
                if self.exists(backup, update_cache=False):
                    return self.load_json(backup)
            elif warn_not_found:
                logger.log(
                    level=15, msg=f"Backup {backup} cache not found")
        except:
            logger.warning(
                f"Unable to load backup {backup} cache:", exc_info=True)

    def update_fn_cache(self, fname):
        pass

    def write(self, content, fname=None, dest=None, subdir=None):
        written = None
        dest = self.__get_subpath(fname, dest, subdir)
        tmp = dest.with_suffix('.tmp')
        logger.log(level=5, msg=f"Writing item to {dest}")
        with tmp.open('w') as f:
            written = f.write(content)
        logger.log(level=4, msg='Renaming temp file')
        tmp.rename(dest)
        logger.log(level=4, msg='Finished writing')
        return written

    def write_bytes(self, content, fname=None, dest=None, subdir=None):
        written = None
        dest = self.__get_subpath(fname, dest, subdir)
        tmp = dest.with_suffix('.tmp')
        logger.log(level=5, msg=f"Writing item to {dest}")
        written = tmp.write_bytes(content)
        logger.log(level=4, msg='Renaming temp file')
        tmp.rename(dest)
        logger.log(level=4, msg='Finished writing')
        return written

    def utime(self, mtime, fname=None, dest=None, subdir=None):
        mod_time = mktime(parsedate(mtime))
        logger.log(level=4, msg=f"Updating file times to {mod_time}")
        utime(self.__get_subpath(fname, dest, subdir), (mod_time, mod_time))

    def dir_exists(self, subdir=None, dir_name=None):
        dir_item = self.__base_dir if subdir is None else self.__base_dir.joinpath(
            subdir)
        if dir_name is not None:
            dir_item = dir_item.joinpath(dir_name)
        result = (not dir_item.is_symlink()) and dir_item.is_dir()
        logger.debug('Dir: %s Exists: %s', str(dir_item),  result)
        return result

    def mkdir(self, dir_name=None):
        dir_item = self.__base_dir if dir_name is None else self.__base_dir.joinpath(
            PurePath(dir_name).name)
        dir_item.mkdir(parents=True)
        return True

    def rmdir(self, dir_name):
        dir_item = self.__base_dir.joinpath(PurePath(dir_name).name)
        dir_item.rmdir()
        return True

    def rename_dir(self, dir_name=None, src=None, new_dir_name=None, dest=None):
        dir_name = get_dir_name(dir_name, src)
        new_dir_name = get_new_dir_name(new_dir_name, dest)
        old_dir_item = self.__base_dir.joinpath(dir_name)
        new_dir_item = self.__base_dir.joinpath(new_dir_name)

        if not old_dir_item.is_dir():
            raise ValueError('Item is not a directory')

        old_dir_item.rename(new_dir_item)
        return True

    def lock(self):
        try:
            if not self.__lock_path:
                self.__lock_path = self.__base_dir.joinpath('.lock')
            self.__lock = portalocker.RLock(
                self.__lock_path, flags=portalocker.LOCK_EX)
            self.__lock.acquire(timeout=15, fail_when_locked=True)
        except (portalocker.exceptions.LockException, portalocker.exceptions.AlreadyLocked, OSError) as ex:
            logger.warning(f"Skipping locked directory {self.base_dir}")
            raise DagrCacheLockException(ex)

    def stat(self, fname):
        statfp = self.__base_dir.joinpath(PurePosixPath(fname).name)
        s_obj = statfp.stat()
        return {k: getattr(s_obj, k) for k in dir(s_obj) if k.startswith('st_')}

    def release_lock(self):
        self.__lock.release()
        if self.__lock._acquire_count == 0:
            unlink_lockfile(self.__lock_path)
        self.__lock = None

    def __get_subpath(self, fname=None, dest=None, subdir=None):
        fname = get_fname(fname, dest)
        if subdir:
            return self.__base_dir.joinpath(subdir, fname)
        return self.__base_dir.joinpath(fname)
