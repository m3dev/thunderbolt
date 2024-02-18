import os
import unittest

from thunderbolt.client.local_directory_client import LocalDirectoryClient


class TestLocalDirectoryClient(unittest.TestCase):
    def setUp(self):
        self.client = LocalDirectoryClient('.', None, None, use_cache=False)

    def test_to_absolute_path(self):
        source = './hoge/hoge/piyo'
        self.client.workspace_directory = '../hoge/'
        target = os.path.abspath('../hoge') + '/hoge/piyo'

        output = self.client.to_absolute_path(source)
        self.assertEqual(output, target)
