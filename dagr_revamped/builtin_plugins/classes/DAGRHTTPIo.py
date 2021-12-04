import logging
from os import scandir
from pathlib import Path, PurePosixPath
from pprint import pformat

from dagr_revamped.DAGRIo import (DAGRIo, get_dir_name, get_fname,
                                  get_new_dir_name)
from dagr_revamped.TCPKeepAliveSession import TCPKeepAliveSession
from dagr_revamped.utils import (http_exists, http_fetch_json, http_list_dir,
                                 http_lock_dir, http_mkdir,
                                 http_post_file_json, http_post_file_multipart,
                                 http_post_json, http_post_raw,
                                 http_refresh_lock, http_release_lock,
                                 http_rename_dir, http_replace)

logger = logging.getLogger(__name__)


class DAGRHTTPIo(DAGRIo):
    @staticmethod
    def create(base_dir, rel_dir, config):
        endpoints = config.get('dagr.io.http.endpoints',
                               key_errors=False) or {}
        return DAGRHTTPIo(base_dir, rel_dir, endpoints)

    def get_rel_path(self, subdir=None, dir_name=None):
        if subdir is not None and isinstance (dir_name, Path) and subdir.is_absolute():
            raise Exception('subdir cannot be absolute')
        if dir_name is not None and not isinstance (dir_name, str):
            raise Exception('dir_name must be an instance of str')

        result = PurePosixPath(self.rel_dir)
        if subdir is not None:
            result = result.joinpath(subdir)
        if dir_name is not None:
            result = result.joinpath(dir_name)
        return str(result)

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
        self.__file_stat_ep = endpoints.get('file_stat', None)
        self.__dir_lock_ep = endpoints.get('dir_lock', None)
        self.__session = TCPKeepAliveSession()

        if self.__exists_ep is None:
            logger.warning('No exists endpoint configured')
        else:
            self.exists = lambda fname=None, dest=None, subdir=None, update_cache=None: http_exists(
                self.__session, self.__exists_ep, dir_path=self.get_rel_path(subdir=subdir), itemname=get_fname(fname, dest), update_cache=update_cache)

        if self.__list_dir_ep is None:
            logger.warning('No list dir endpoint configured')
        else:
            self.list_dir = lambda: http_list_dir(
                self.__session, self.__list_dir_ep, self.rel_dir_name)

        if self.__load_json_ep is None:
            logger.warning('No load json endpoint configured')
        else:
            self.load_json = lambda fname, log_errors=True: http_fetch_json(
                self.__session, self.__load_json_ep,  path=self.rel_dir_name, filename=fname, log_errors=log_errors)

        if self.__save_json_ep is None:
            logger.warning('No save json endpoint configured')
        else:
            self.save_json = lambda fname, content, do_backup=True, log_errors=True: http_post_file_json(
                self.__session, self.__save_json_ep, self.rel_dir_name, fname, content, do_backup, log_errors=log_errors)

        if self.__replace_ep is None:
            logger.warning('No replace endpoint configured')
        else:
            self.replace = lambda dest_fname=None, src_fname=None, dest=None, src=None, dest_subdir=None, src_subdir=None: http_replace(
                self.__session, self.__replace_ep, dir_path=self.rel_dir_name, dest_subdir=dest_subdir, dest_fname=get_fname(dest_fname, dest), src_subdir=src_subdir, src_fname=get_fname(src_fname, src))

        if self.__update_fn_cache_ep is None:
            logger.warning('No update filename cache endpoint configured')
        else:
            self.update_fn_cache = lambda fname: http_post_json(
                self.__session, self.__update_fn_cache_ep, path=self.rel_dir_name, filenames=[fname])

        if self.__write_file_ep is None:
            logger.warning('No write file endpoint configured')
        else:
            self.write = lambda content, fname=None, dest=None, subdir=None: http_post_file_multipart(
                self.__session, self.__write_file_ep,  self.get_rel_path(subdir=subdir), get_fname(fname, dest), content)
            self.write_bytes = lambda content, fname=None, dest=None, subdir=None: http_post_file_multipart(
                self.__session, self.__write_file_ep,  self.get_rel_path(subdir=subdir), get_fname(fname, dest), content).get('size')

        if self.__utime_ep is None:
            logger.warning('No utime endpoint configured')
        else:
            self.utime = lambda mtime, fname=None, dest=None: http_post_json(
                self.__session, self.__utime_ep,  mtime=mtime, path=self.rel_dir_name, filename=get_fname(fname, dest))

        if self.__dir_exists_ep is None:
            logger.warning('No dir exists endpoint configured')
        else:
            self.dir_exists = lambda subdir=None, dir_name = None: http_exists(
                self.__session, self.__dir_exists_ep, dir_path=self.get_rel_path(subdir=subdir, dir_name=dir_name))

        if self.__mkdir_ep is None:
            logger.warning('No mkdir endpoint configured')
        else:
            self.mkdir = lambda subdir=None, dir_name = None: http_mkdir(
                self.__session, self.__mkdir_ep, dir_path=self.get_rel_path(subdir=subdir, dir_name=dir_name))

        if self.__rename_dir_ep is None:
            logger.warning('No rename dir endpoint configured')
        else:
            self.rename_dir = lambda dir_name=None, src=None, new_dir_name=None, dest=None: http_rename_dir(
                self.__session, self.__rename_dir_ep, dir_path=self.rel_dir_name, dir_name=get_dir_name(dir_name, src), new_dir_name=get_new_dir_name(new_dir_name, dest))

        if self.__file_stat_ep is None:
            logger.warning('No file stat endpoint configured')
        else:
            self.stat = lambda fname, subdir=None, dir_name = None: http_fetch_json(
                self.__session, self.__file_stat_ep,  path=self.get_rel_path(subdir=subdir, dir_name=dir_name), itemname=fname).get('stat', {})


        if self.__dir_lock_ep is None:
            logger.warning('No dir lock endpoint configured')
        else:
            self.lock = lambda : http_lock_dir(self.__session, self.__dir_lock_ep, dir_path=self.rel_dir_name)
            self.release_lock = lambda : http_release_lock(self.__session, self.__dir_lock_ep, dir_path=self.rel_dir_name)

    def __del__(self):
        super().__del__()

    def close(self):
        self.__session.close()
        super().close()