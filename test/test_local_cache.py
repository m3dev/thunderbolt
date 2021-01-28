import unittest
import os
from pathlib import Path

from thunderbolt.client.local_cache import LocalCache


class TestLocalCache(unittest.TestCase):
    def setUp(self):
        self.base_path = './resources'
        self.local_cache = LocalCache(self.base_path, True)

    def test_init(self):
        self.assertTrue(os.path.exists('./thunderbolt'))

    def test_dump_and_get(self):
        target = {'foo': 'bar'}
        self.local_cache.dump('test.pkl', target)
        output = self.local_cache.get('test.pkl')
        self.assertDictEqual(target, output)

    def test_convert_file_path(self):
        output = self.local_cache._convert_file_path('test.pkl')
        target = Path(os.path.join(os.getcwd(), '.thunderbolt', self.base_path.split('/')[-1], 'test.pkl'))
        self.assertEqual(target, output)

    def tearDown(self):
        self.local_cache.clear()
