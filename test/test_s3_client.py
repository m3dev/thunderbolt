import unittest
from thunderbolt.client.s3_client import S3Client


class TestS3Client(unittest.TestCase):
    def setUp(self):
        self.base_path = 's3://bucket/prefix/'
        self.client = S3Client(self.base_path, None, None)

    def test_to_absolute_path(self):
        source = 's3://bucket/prefix/hoge/piyo'
        target = self.base_path + 'hoge/piyo'
        output = self.client.to_absolute_path(source)
        self.assertEqual(output, target)
