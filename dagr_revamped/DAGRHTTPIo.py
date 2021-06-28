import logging
from os import scandir
from pathlib import Path, PurePosixPath
from pprint import pformat

from .DAGRIo import DAGRIo, get_dir_name, get_fname, get_new_dir_name
from .TCPKeepAliveSession import TCPKeepAliveSession
from .utils import (http_exists, http_fetch_json, http_list_dir, http_mkdir,
                    http_post_file_json, http_post_file_multipart,
                    http_post_json, http_post_raw, http_rename_dir,
                    http_replace)

logger = logging.getLogger(__name__)


class DAGRHTTPIo(DAGRIo):
    @staticmethod
    def create(base_dir, rel_dir, config):
        endpoints = config.get('dagr.io.http.endpoints',
                               key_errors=False) or {}
        return DAGRHTTPIo(base_dir, rel_dir, endpoints)

    def get_rel_path(self, subdir):
        if subdir is None:
            return self.rel_dir
        return str(PurePosixPath(self.rel_dir).joinpath(subdir))

    def __init__(self, base_dir, rel_dir, endpoints):
        super().__init__(base_dir, rel_dir)

        logger.log(level=5, msg=f"HTTP io endpoints: {pformat(endpoints)}")

        self.__exists_ep = endpoints.get('exists', None)
        self.__list_dir_ep = endpoints.get('list_dir', None)
        self.__load_json_ep = endpoints.get('load_json', None)
        self.__save_json_ep = endpoints.get('save_json', None)
        self.__replace_ep = endpoints.get('replace', None)
        self.__update_fn_cache_ep = endpoints.get('update_fn_cache', None)
        self.__write_file_ep = endpoints.get('write_file', None)
        self.__utime_ep = endpoints.get('utime', None)
        self.__dir_exists_ep = endpoints.get('dir_exists', None)
        self.__mkdir_ep = endpoints.get('mkdir', None)
        self.__rename_dir_ep = endpoints.get('rename_dir', None)

        session = TCPKeepAliveSession()

        if self.__exists_ep is None:
            logger.warning('No exists endpoint configured')
        else:
            self.exists = lambda fname=None, dest=None, subdir=None, update_cache=None: http_exists(
                session, self.__exists_ep, dir_path=self.get_rel_path(subdir), itemname=get_fname(fname, dest), update_cache=update_cache)

        if self.__list_dir_ep is None:
            logger.warning('No list dir endpoint configured')
        else:
            self.list_dir = lambda: http_list_dir(
                session, self.__list_dir_ep, self.rel_dir_name)

        if self.__load_json_ep is None:
            logger.warning('No load json endpoint configured')
        else:
            self.load_json = lambda fname: http_fetch_json(
                session, self.__load_json_ep,  path=self.rel_dir_name, filename=fname)

        if self.__save_json_ep is None:
            logger.warning('No save json endpoint configured')
        else:
            self.save_json = lambda fname, content, do_backup=True: http_post_file_json(
                session, self.__save_json_ep, self.rel_dir_name, fname, content, do_backup)

        if self.__replace_ep is None:
            logger.warning('No replace endpoint configured')
        else:
            self.replace = lambda fname, new_fname: http_replace(
                session, self.__replace_ep, self.rel_dir_name, fname, new_fname)

        if self.__update_fn_cache_ep is None:
            logger.warning('No update filename cache endpoint configured')
        else:
            self.update_fn_cache = lambda fname: http_post_json(
                session, self.__update_fn_cache_ep, path=self.rel_dir_name, filenames=[fname])

        if self.__write_file_ep is None:
            logger.warning('No write file endpoint configured')
        else:
            self.write = lambda content, fname=None, dest=None, subdir=None: http_post_file_multipart(
                session, self.__write_file_ep,  self.get_rel_path(subdir), get_fname(fname, dest), content)
            self.write_bytes = lambda content, fname=None, dest=None, subdir=None: http_post_file_multipart(
                session, self.__write_file_ep,  self.get_rel_path(subdir), get_fname(fname, dest), content)

        if self.__utime_ep is None:
            logger.warning('No utime endpoint configured')
        else:
            self.utime = lambda mtime, fname=None, dest=None: http_post_json(
                session, self.__utime_ep,  mtime=mtime, path=self.rel_dir_name, filename=get_fname(fname, dest))

        if self.__dir_exists_ep is None:
            logger.warning('No dir exists endpoint configured')
        else:
            self.dir_exists = lambda dir_name = None: http_exists(
                session, self.__dir_exists_ep, dir_path=self.rel_dir_name, itemname=dir_name)

        if self.__mkdir_ep is None:
            logger.warning('No mkdir endpoint configured')
        else:
            self.mkdir = lambda dir_name = None: http_mkdir(
                session, self.__mkdir_ep, dir_path=self.rel_dir_name, dir_name=dir_name)

        if self.__rename_dir_ep is None:
            logger.warning('No rename dir endpoint configured')
        else:
            self.rename_dir = lambda dir_name=None, src=None, new_dir_name=None, dest=None: http_rename_dir(
                session, self.__rename_dir_ep, dir_path=self.rel_dir_name, dir_name=get_dir_name(dir_name, src), new_dir_name=get_new_dir_name(new_dir_name, dest))
