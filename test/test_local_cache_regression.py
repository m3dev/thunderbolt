import pickle
import unittest
from os import path

import pandas as pd

from thunderbolt import Thunderbolt
from thunderbolt.client.local_cache import LocalCache

"""
requires:
python sample.py test.TestCaseTask --param=sample --number=1 --workspace-directory=./test_case --local-scheduler
"""


class LocalCacheTest(unittest.TestCase):
    def test_running(self):
        target = Thunderbolt(self._get_test_case_path(), use_cache=False)
        _ = Thunderbolt(self._get_test_case_path())
        output = Thunderbolt(self._get_test_case_path())

        for k, v in target.tasks.items():
            if k == 'last_modified':  # cache file
                continue
            self.assertEqual(v, output.tasks[k])

        output.client.local_cache.clear()

    def _get_test_case_path(self, file_name: str = ''):
        p = path.abspath(path.join(path.dirname(__file__), 'test_case'))
        if file_name:
            return path.join(p, file_name)
        return p
