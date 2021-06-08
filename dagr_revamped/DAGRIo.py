import logging
from email.utils import parsedate
from json import JSONDecodeError
from os import scandir, utime
from pathlib import Path, PurePosixPath
from time import mktime

from .utils import load_json, save_json

logger = logging.getLogger(__name__)


class DAGRIo():
    @staticmethod
    def create(base_dir, rel_dir, _config):
        return DAGRIo(base_dir, rel_dir)

    def __init__(self, base_dir, rel_dir):
        self.__base_dir = base_dir
        self.__rel_dir = rel_dir
        self.__rel_dir_name = str(PurePosixPath(rel_dir))

    @ property
    def base_dir(self):
        return self.__base_dir

    @ property
    def rel_dir(self):
        return self.__rel_dir

    @ property
    def rel_dir_name(self):
        return self.__rel_dir_name

    def list_dir(self):
        return (i.name for i in scandir(self.__base_dir))

    def load_json(self, fname):
        return load_json(self.__base_dir.joinpath(fname))

    def save_json(self, fname, content, do_backup=True):
        return save_json(self.__base_dir.joinpath(fname), content)

    def exists(self, fname=None, dest=None, update_cache=None):
        return self.__get_dest(fname, dest).exists()

    def replace(self, fname, new_fname):
        return self.__base_dir.joinpath(fname).replace(new_fname)

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
                self.replace(fname, fpath.with_suffix('.bak')).name
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

    def write(self, content, fname=None, dest=None):
        written = None
        dest = self.__get_dest(fname, dest)
        tmp = dest.with_suffix('.tmp')
        logger.log(level=5, msg=f"Writing item to {dest}")
        with tmp.open('r') as f:
            written = f.write(content)
        logger.log(level=4, msg='Renaming temp file')
        tmp.rename(dest)
        logger.log(level=4, msg='Finished writing')
        return written

    def write_bytes(self, content, fname=None, dest=None):
        written = None
        dest = self.__get_dest(fname, dest)
        tmp = dest.with_suffix('.tmp')
        logger.log(level=5, msg=f"Writing item to {dest}")
        with tmp.open('rb') as f:
            written = f.write_bytes(content)
        logger.log(level=4, msg='Renaming temp file')
        tmp.rename(dest)
        logger.log(level=4, msg='Finished writing')
        return written

    def utime(self, mtime, fname=None, dest=None):
        mod_time = mktime(parsedate(mtime))
        logger.log(level=4, msg=f"Updating file times to {mod_time}")
        utime(self.__get_dest(fname, dest), (mod_time, mod_time))

    def __get_dest(self, fname=None, dest=None):
        if dest is None:
            if fname is None:
                raise TypeError('Either fname or dest arg is required')
            dest = self.__base_dir.joinpath(fname)
        return dest
