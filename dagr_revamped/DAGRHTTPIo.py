import logging
from os import scandir
from pathlib import Path, PurePosixPath
from pprint import pformat

from requests import Session

from .DAGRIo import DAGRIo
from .utils import (http_exists, http_fetch_json, http_list_dir,
                    http_post_file_multipart, http_post_json, http_post_raw,
                    http_replace)

logger = logging.getLogger(__name__)


def get_fname(fname=None, dest=None):
    if fname is None:
        if dest is None:
            raise TypeError('Either fname or dest arg is required')
        fname = dest.name
    return fname


class DAGRHTTPIo(DAGRIo):
    @staticmethod
    def create(base_dir, rel_dir, config):
        endpoints = config.get('dagr.io.http.endpoints',
                               key_errors=False) or {}
        return DAGRHTTPIo(base_dir, rel_dir, endpoints)

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

        session = Session()

        if not self.__exists_ep is None:
            self.exists = lambda fname=None, dest=None, update_cache=None: http_exists(
                session, self.__exists_ep, self.rel_dir_name, fname=get_fname(fname, dest), update_cache=update_cache)

        if not self.__list_dir_ep is None:
            self.list_dir = lambda: http_list_dir(
                session, self.__list_dir_ep, self.rel_dir_name)

        if not self.__load_json_ep is None:
            self.load_json = lambda fname: http_fetch_json(
                session, self.__load_json_ep,  self.rel_dir_name, fname=fname)

        if not self.__save_json_ep is None:
            self.save_json = lambda fname, content, do_backup=True: http_post_json(
                session, self.__save_json_ep, self.rel_dir_name, fname, content, do_backup)

        if not self.__replace_ep is None:
            self.replace = lambda fname, new_fname: http_replace(
                session, self.__replace_ep, self.rel_dir_name, fname, new_fname)

        if not self.__update_fn_cache_ep is None:
            self.update_fn_cache = lambda fname: http_post_raw(
                session, self.__update_fn_cache_ep, json={'path': self.rel_dir_name, 'filenames': [fname]})

        if not self.__write_file_ep is None:
            self.write = lambda content, fname=None, dest=None: http_post_file_multipart(
                session, self.__write_file_ep,  self.rel_dir_name, get_fname(fname, dest), content)

        if not self.__write_file_ep is None:
            self.write_bytes = lambda content, fname=None, dest=None: http_post_file_multipart(
                session, self.__write_file_ep,  self.rel_dir_name, get_fname(fname, dest), content)

        if not self.__utime_ep is None:
            self.utime = lambda mtime, fname=None, dest=None: http_post_raw(
                session, self.__utime_ep,  json={'mtime': mtime, 'path': self.rel_dir_name, 'filename': get_fname(fname, dest)})
