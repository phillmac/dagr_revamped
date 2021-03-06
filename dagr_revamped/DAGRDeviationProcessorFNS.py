import logging
from pathlib import PurePosixPath

import requests

from dagr_revamped.lib import DAGRDeviationProcessor


class DAGRDeviationProcessorFNS(DAGRDeviationProcessor):

    def __init__(self, ripper, cache, page_link, **kwargs):
        super().__init__(ripper, cache, page_link, **kwargs)
        self.__logger = logging.getLogger(__name__)
        self.fns_address = kwargs.get('fns_address', self.config.get(
            'dagr.deviationprocessor', 'fns_address'))
        if self.fns_address is None or self.fns_address == '':
            raise Exception('FNS address cannot be empty')
        self.__logger.log(level=5, msg=f"FNS address: {self.fns_address}")

    def verify_exists(self, warn_on_existing=True):
        fname = self.get_fname()
        if not self.force_verify_exists:
            if fname in self.cache.files_list:
                if warn_on_existing:
                    self.__logger.warning(
                        "Cache entry {} exists - skipping".format(fname))
                return False
        if self.force_verify_exists:
            self.__logger.log(
                level=15, msg='Verifying {} really exists'.format(self.get_dest().name))
        if self.fns_dest_exists():
            self.cache.add_filename(fname)
            self.__logger.warning(
                "FS entry {} exists - skipping".format(fname))
            return False
        return True

    def fns_dest_exists(self):
        dest_rel = str(PurePosixPath(self.cache.rel_dir))
        filename = self.get_fname()
        resp = requests.get(self.fns_address, json={
            'path': dest_rel.strip('/'), 'filename': filename})
        resp.raise_for_status()
        result = resp.json()
        self.__logger.log(level=5, msg=f"{resp} {result}")
        return result['exists']
