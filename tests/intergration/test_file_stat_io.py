import logging
import unittest
from os import urandom
from pathlib import Path

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

    def test_write_bytes(self):
        result = None
        write_bytes_dir = self.results_dir.joinpath('write_bytes')
        write_bytes_dir.mkdir()
        with create_io(self, select_io_class()) as io:
            try:
                content = urandom(1000000)
                result = io.write_bytes(
                    content, fname='written_bytes', subdir='write_bytes')
            except:
                logging.exception('Failed to write bytes')
                self.containerLogs()
            finally:
                io.close()

        self.assertEqual(result , 1000000)

    def test_stat_with_subdir(self):
        result = None
        stat_dir = self.results_dir.joinpath('stat_subdir')
        stat_dir.mkdir()
        stat_dir.joinpath('stat_file_with_subdir').write_bytes(urandom(1000000))

        with create_io(self, select_io_class()) as io:
            try:
                result = io.stat(fname='stat_file_with_subdir', dir_name='stat_subdir').get('st_size')
            except:
                logging.exception('Failed to stat file')
                self.containerLogs()
            finally:
                io.close()

        self.assertEqual(result , 1000000)

    def test_stat(self):
        result = None
        stat_dir = self.results_dir.joinpath('stat_dir')
        stat_dir.mkdir()
        stat_dir.joinpath('stat_file').write_bytes(urandom(1000000))

        with create_io(self, select_io_class(), rel_dir='stat_dir') as io:
            try:
                result = io.stat(fname='stat_file').get('st_size')
            except:
                logging.exception('Failed to stat file')
                self.containerLogs()
            finally:
                io.close()

        self.assertEqual(result , 1000000)


    def tearDown(self):
        tearDownTestCase(self)


if __name__ == '__main__':
    unittest.main()
