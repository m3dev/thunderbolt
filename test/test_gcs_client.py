import unittest
from thunderbolt.client.gcs_client import GCSClient


class TestGCSClient(unittest.TestCase):
    def setUp(self):
        self.base_path = 'gs://bucket/prefix/'
        self.client = GCSClient(self.base_path, None, None)

    def test_convert_absolute_path(self):
        source = 'hoge/piyo'
        target = self.base_path + 'hoge/piyo'
        output = self.client.convert_absolute_path(source)
        self.assertEqual(output, target)
