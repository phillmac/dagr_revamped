import logging
import unittest
from pathlib import Path

from io_tests_setup import (create_io, select_io_class, setUpTestCase,
                         tearDownTestCase)


class TestIO(unittest.TestCase):

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.create_io = lambda: None
        self.container = None
        self.results_dir = None

    def containerLogs(self):
        for log_item in self.container.logs(stdout=True, stderr=True, stream=True, follow=False):
            logging.info(log_item.decode('utf-8'))

    def setUp(self):
        setUpTestCase(self)

    def test_mkdir(self):
        result = None

        with create_io(self, select_io_class()) as io:
            try:
                result = io.mkdir('mkdir_test')
            except:
                logging.exception('Failed to mkdir')
                self.containerLogs()

        self.assertTrue(result is True)

    def test_exists(self):
        exists_result = None
        does_not_exist_result = None
        self.results_dir.joinpath('exists_dir').mkdir()

        with create_io(self, select_io_class()) as io:

            try:
                exists_result = io.exists('exists_dir')
                does_not_exist_result = io.exists('does_not_exist_dir')
            except:
                logging.exception('Failed to test exists')
                self.containerLogs()

        self.assertTrue(exists_result is True)
        self.assertTrue(does_not_exist_result is False)

    def test_rmdir(self):
        result = None
        rm_dir = self.results_dir.joinpath('rmdir')
        rm_dir.mkdir()

        with create_io(self, select_io_class()) as io:
            try:
                result = io.rmdir('rmdir')
            except:
                logging.exception('Failed to test rmdir')
                self.containerLogs()

        self.assertTrue(result is True)
        self.assertFalse(rm_dir.exists())

    def test_rename_dir(self):
        result = None
        old_name = self.results_dir.joinpath('old_name')
        new_name = self.results_dir.joinpath('new_name')
        old_name.mkdir()

        with create_io(self, select_io_class()) as io:
            try:
                result = io.rename_dir(src=old_name, dest=new_name)
            except:
                logging.exception('Failed to test rmdir')
                self.containerLogs()

        self.assertTrue(result is True)
        self.assertFalse(old_name.exists())
        self.assertTrue(new_name.exists())

    def test_move_dir(self):
        result = None
        parent = self.results_dir.joinpath('parent')
        child1 = parent.joinpath('child1')
        child2 = child1.joinpath('child2')

        child2.mkdir(parents=True)

        with create_io(self, select_io_class()) as io:
            try:
                result = io.rename_dir(
                    dir_name='parent/child1/child2', new_dir_name='parent/child2')
            except:
                logging.exception('Failed to test rmdir')
                self.containerLogs()

        self.assertTrue(result is True)
        self.assertTrue(parent.exists())
        self.assertTrue(child1.exists())
        self.assertFalse(child2.exists())
        self.assertTrue(parent.joinpath('child2').exists())

    def tearDown(self):
        tearDownTestCase(self)


if __name__ == '__main__':
    unittest.main()
