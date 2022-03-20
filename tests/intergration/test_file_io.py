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


    def tearDown(self):
        tearDownTestCase(self)


if __name__ == '__main__':
    unittest.main()
