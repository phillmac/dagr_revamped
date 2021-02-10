import logging
from os import scandir
from pathlib import Path, PurePosixPath

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
        return (i.name for i in os.scandir(self.__base_dir))

    def load_json(self, fname):
        return load_json(self.__base_dir.joinpath(fname))

    def save_json(self, fname, content, do_backup=True):
        return save_json(self.__base_dir.joinpath(fname), content)

    def exists(self, fname):
        return self.__base_dir.joinpath(fname).exists()

    def replace(self, fname, new_fname):
        return self.__base_dir.joinpath(fname).replace(new_fname)

    def load_primary_or_backup(self, fname, use_backup=True, warn_not_found=True):
        if not isinstance(fname, str):
            raise Exception('fname arg must be an instance of str')

        fpath = PurePath(fname)
        backup = fpath.with_suffix('.bak').name

        try:
            if self.exists(fname):
                return self.load_json(fname)
            elif warn_not_found:
                logger.log(
                    level=15, msg=f"Primary {fname} cache not found")
        except json.JSONDecodeError:
            logger.warning(
                f"Unable to decode primary {fname} cache:", exc_info=True)
            self.replace(fname, fpath.with_suffix('.bak')).name
        except:
            logger.warning(
                f"Unable to load primary {fname} cache:", exc_info=True)
        try:
            if use_backup:
                if backup.exists():
                    return load_json(backup)
            elif warn_not_found:
                logger.log(
                    level=15, msg="Backup {backup} cache not found")
        except:
            logger.warning(
                f"Unable to load backup {backup} cache:", exc_info=True)