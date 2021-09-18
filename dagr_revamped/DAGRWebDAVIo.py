import logging
from json import JSONDecodeError
from os import scandir
from pathlib import Path, PurePosixPath
from pprint import pformat

import easywebdav

from .DAGRIo import DAGRIo
from .utils import load_json, save_json

logger = logging.getLogger(__name__)


class DAGRWebDAVIo(DAGRIo):
    @staticmethod
    def create(base_dir, rel_dir, config):
        webdav_config = config.get('dagr.io.webdav')
        return DAGRWebDAVIo(base_dir, rel_dir,
            host=webdav_config.get('host'),
            port=webdav_config.get('port', 80),
            username=webdav_config.get('username'),
            password=webdav_config.get('password'),
            protocol=webdav_config.get('protocol', 'http')
        )

    def __init__(self, base_dir, rel_dir, host, port, username, password, protocol):
        super().__init__(base_dir, rel_dir)

        logger.log(level=5, msg=f"WebDav URL: {protocol}{host}:{port}")
        logger.log(level=5, msg=f"WebDav user: {username}")

        self.__client = easywebdav.connect(
            host=host, port=port, username=username, password=password, protocol=protocol, path=str(rel_dir))
