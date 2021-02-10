import logging
from os import scandir
from pathlib import Path, PurePosixPath
from pprint import pformat
from requests import Session

from .DAGRIo import DAGRIo
from .utils import http_exists, http_fetch_json, http_list_dir, http_post_json, http_replace

logger = logging.getLogger(__name__)


class DAGRHTTPIo(DAGRIo):
    @staticmethod
    def create(base_dir, rel_dir, config):
        endpoints = config.get('dagr.io.http.endpoints')
        return DAGRHTTPIo(base_dir, rel_dir, endpoints)

    def __init__(self, base_dir, rel_dir, endpoints):
        super().__init__(base_dir, rel_dir)

        logger.log(level=5, msg=f"HTTP io endpoints: {pformat(endpoints)}")

        self.__exists_ep = endpoints.get('exists', None)
        self.__list_dir_ep = endpoints.get('list_dir', None)
        self.__load_json_ep = endpoints.get('load_json', None)
        self.__save_json_ep = endpoints.get('save_json', None)
        self.__replace_ep = endpoints.get('replace', None)

        session = Session()

        if not self.__exists_ep is none:
            self.exists = lambda _self, fname: http_exists(
                session, self.__exists_ep, self.rel_dir_name, fname=fname)

        if not self.__list_dir_ep is None:
            self.list_dir = lambda _self: http_list_dir(
                session, self.__list_dir_ep, self.rel_dir_name)

        if not self.__load_json_ep is None:
            self.load_json = lambda _self, fname: http_fetch_json(
                session, self.__load_json_ep,  self.rel_dir_name, fname=fname)

        if not self.__save_json_ep is None:
            self.save_json = lambda _self, fname, content, do_backup=True: http_post_json(
                session, self.__load_json_ep, self.rel_dir_name, fname, content, do_backup)

        if not self.__replace_ep is None:
            self.replace = lambda _self, fname, new_fname: http_replace(
                session, self.__replace_ep, self.rel_dir_name, fname, new_fname)