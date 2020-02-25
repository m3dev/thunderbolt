import thunderbolt
import unittest
from unittest.mock import patch
from mock import MagicMock
from contextlib import ExitStack
import pandas as pd
from thunderbolt.client.local_directory_client import LocalDirectoryClient
from thunderbolt.client.gcs_client import GCSClient
from thunderbolt.client.s3_client import S3Client


class TestThunderbolt(unittest.TestCase):
    def setUp(self):
        def get_tasks():
            pass

        module_path = 'thunderbolt.client'
        with ExitStack() as stack:
            for module in ['local_directory_client.LocalDirectoryClient', 'gcs_client.GCSClient', 's3_client.S3Client']:
                stack.enter_context(patch('.'.join([module_path, module, 'get_tasks']), side_effect=get_tasks))
            self.tb = thunderbolt.Thunderbolt(None)

    def test_get_client(self):
        source_workspace_directory = ['s3://', 'gs://', 'gcs://', './local', 'hoge']
        source_filters = []
        source_tqdm_disable = False
        target = [S3Client, GCSClient, GCSClient, LocalDirectoryClient, LocalDirectoryClient]

        for s, t in zip(source_workspace_directory, target):
            output = self.tb._get_client(s, source_filters, source_tqdm_disable)
            self.assertEqual(type(output), t)

    def test_get_task_df(self):
        self.tb.tasks = {
            'Task1': {
                'task_name': 'task_name_1',
                'last_modified': 'last_modified_1',
                'task_params': 'task_params_1',
                'task_hash': 'task_hash_1',
                'task_log': 'task_log_1'
            }
        }

        target = pd.DataFrame({
            'task_id': ['Task1'],
            'task_name': ['task_name_1'],
            'last_modified': ['last_modified_1'],
            'task_params': ['task_params_1'],
            'task_hash': ['task_hash_1'],
            'task_log': ['task_log_1']
        })
        output = self.tb.get_task_df(all_data=True)
        pd.testing.assert_frame_equal(output, target)

        target = pd.DataFrame({
            'task_id': ['Task1'],
            'task_name': ['task_name_1'],
            'last_modified': ['last_modified_1'],
            'task_params': ['task_params_1'],
        })
        output = self.tb.get_task_df(all_data=False)
        pd.testing.assert_frame_equal(output, target)

    def test_get_data(self):
        self.tb.tasks = {
            'Task1': {
                'task_name': 'task',
                'last_modified': 'last_modified_1',
                'task_params': 'task_params_1',
                'task_hash': 'task_hash_1',
                'task_log': 'task_log_1'
            },
            'Task2': {
                'task_name': 'task',
                'last_modified': 'last_modified_2',
                'task_params': 'task_params_2',
                'task_hash': 'task_hash_2',
                'task_log': 'task_log_2'
            }
        }
        target = 'Task2'

        with patch('thunderbolt.Thunderbolt.load', side_effect=lambda x: x):
            output = self.tb.get_data('task')
        self.assertEqual(output, target)

    def test_get_data_top_k(self):
        self.tb.tasks = {
            'Task1': {
                'task_name': 'task',
                'last_modified': 'last_modified_1',
                'task_params': 'task_params_1',
                'task_hash': 'task_hash_1',
                'task_log': 'task_log_1'
            },
            'Task2': {
                'task_name': 'task',
                'last_modified': 'last_modified_2',
                'task_params': 'task_params_2',
                'task_hash': 'task_hash_2',
                'task_log': 'task_log_2'
            },
            'Task3': {
                'task_name': 'task',
                'last_modified': 'last_modified_3',
                'task_params': 'task_params_3',
                'task_hash': 'task_hash_3',
                'task_log': 'task_log_3'
            }
        }
        target = ['Task3', 'Task2']

        with patch('thunderbolt.Thunderbolt.load', side_effect=lambda x: x):
            output = self.tb.get_data('task', 2)
        self.assertEqual(output, target)

    def test_load(self):
        self.tb.tasks = {
            'Task1': {
                'task_log': {
                    'file_path': ['./hoge', './piyo']
                }
            },
            'Task2': {
                'task_log': {
                    'file_path': ['./hoge']
                }
            },
        }

        source = 'Task1'
        target = ['./hoge', './piyo']
        with patch('thunderbolt.Thunderbolt._target_load', side_effect=lambda x: x):
            output = self.tb.load(source)
        self.assertListEqual(output, target)

        source = 'Task2'
        target = './hoge'
        with patch('thunderbolt.Thunderbolt._target_load', side_effect=lambda x: x):
            output = self.tb.load(source)
        self.assertEqual(output, target)

    def test_target_load(self):
        source = 'hoge'
        target = 'hoge'

        def make_target(file_path):
            class mock:
                def __init__(self, file_path):
                    self.file_path = file_path

                def load(self):
                    return file_path

            return mock(file_path)

        self.tb.client.to_absolute_path = MagicMock(side_effect=lambda x: x)
        with patch('gokart.target.make_target', side_effect=make_target):
            output = self.tb._target_load(source)
        self.assertEqual(output, target)
