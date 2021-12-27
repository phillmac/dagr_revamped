import logging
import unittest
from os import urandom
from pathlib import Path

from dagr_revamped.exceptions import DagrCacheLockException
from io_tests_setup import (create_io, select_io_class, setUpTestCase,
                         tearDownTestCase)


class TestIO(unittest.TestCase):

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.container = None
        self.results_dir = None

    def containerLogs(self):
        for log_item in self.container.logs(stdout=True, stderr=True, stream=True, follow=False):
            logging.info(log_item.decode('utf-8'))

    def setUp(self):
        setUpTestCase(self)

    def test_lock(self):
        logging.info('Testing lock io')
        result = None
        lockdir = self.results_dir.joinpath('lockdir')

        lockdir.mkdir()
        with create_io(self, select_io_class(), base_dir=lockdir, rel_dir=lockdir.name) as io:
            try:
                io.lock()
                try:
                    with create_io(self, select_io_class(), base_dir=lockdir, rel_dir=lockdir.name) as io2:
                        io2.lock()
                except DagrCacheLockException:
                    result = True
            except:
                logging.exception('Failed to lock dir')
                self.containerLogs()

        self.assertTrue(result is True)

    def test_rentrant_lock(self):
        logging.info('Testing reentrant lock io')
        result = None
        r_lockdir = self.results_dir.joinpath('rlockdir')

        r_lockdir.mkdir()
        with create_io(self, select_io_class(), base_dir=r_lockdir, rel_dir=r_lockdir.name) as io:
            try:
                io.lock()
                with io:
                    io.lock()
                try:
                    with create_io(self, select_io_class(), base_dir=r_lockdir, rel_dir=r_lockdir.name) as io2:
                        io2.lock()
                except DagrCacheLockException:
                    result = True
                result = True
            except:
                logging.exception('Failed to lock dir')
                self.containerLogs()

        self.assertTrue(result is True)

    def tearDown(self):
        tearDownTestCase(self)


if __name__ == '__main__':
    unittest.main()
