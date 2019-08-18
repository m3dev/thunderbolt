import thunderbolt
import unittest
from os import path
import pandas as pd
import pickle
"""
requires:
python sample.py test.TestCaseTask --param=sample --number=1 --workspace-directory=./test_case --local-scheduler

running:
python -m unittest discover -s ./
"""


class SimpleLocalTest(unittest.TestCase):
    def setUp(self):
        self.here = path.abspath(path.dirname(__file__))
        self.tb = thunderbolt.Thunderbolt(path.join(self.here, 'test_case'))

    def test_init(self):
        self.assertEqual(self.tb.file_path, path.join(self.here, 'test_case'))
        self.assertEqual(self.tb.task_filters, [''])
        self.assertEqual(self.tb.bucket_name, None)
        self.assertEqual(self.tb.prefix, None)
        self.assertEqual(self.tb.resource, None)
        self.assertEqual(self.tb.s3client, None)
        task = self.tb.tasks[0]
        self.assertEqual(task['task_name'], 'TestCaseTask')
        self.assertEqual(task['task_hash'], 'c5b4a28a606228ac23477557c774a3a0')
        self.assertListEqual(task['task_log']['file_path'], ['./test_case/sample/test_case_c5b4a28a606228ac23477557c774a3a0.pkl'])
        self.assertDictEqual(task['task_params'], {'param': 'sample', 'number': '1'})

    def test_get_task_df(self):
        df = self.tb.get_task_df(all_data=True)
        df = df.drop('last_modified', axis=1)
        target_df = pd.DataFrame([{
            'task_id': 0,
            'task_name': 'TestCaseTask',
            'task_params': {
                'param': 'sample',
                'number': '1'
            },
            'task_hash': 'c5b4a28a606228ac23477557c774a3a0',
            'task_log': {
                'file_path': ['./test_case/sample/test_case_c5b4a28a606228ac23477557c774a3a0.pkl']
            }
        }])
        pd.testing.assert_frame_equal(df, target_df)

    def test_load(self):
        x = self.tb.load(0)
        with open(path.join(self.here, 'test_case/sample/test_case_c5b4a28a606228ac23477557c774a3a0.pkl'), 'rb') as f:
            target = pickle.load(f)
        self.assertListEqual(x, [target])
