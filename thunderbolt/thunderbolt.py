from datetime import datetime
import os
from pathlib import Path
import pickle
from typing import Union, List, Any
import shutil

import boto3
from boto3 import Session
import gokart
import pandas as pd
from tqdm import tqdm


class Thunderbolt():
    def __init__(self, workspace_directory: str = '', task_filters: Union[str, List[str]] = '', use_tqdm: bool = False, tmp_path: str = './tmp'):
        """Thunderbolt init.

        Set the path to the directory or S3.

        Args:
            workspace_directory: Gokart's TASK_WORKSPACE_DIRECTORY. If None, use $TASK_WORKSPACE_DIRECTORY in os.env.
            task_filters: Filter for task name.
                Load only tasks that contain the specified string here. We can also specify the number of copies.
            use_tqdm: Flag of using tdqm. If False, tqdm not be displayed (default=False).
            tmp_path: Temporary directory when use external load function.
        """
        self.tqdm_disable = not use_tqdm
        self.tmp_path = tmp_path
        self.s3client = None
        if not workspace_directory:
            env = os.getenv('TASK_WORKSPACE_DIRECTORY')
            workspace_directory = env if env else ''
        self.workspace_directory = workspace_directory if workspace_directory.startswith('s3://') else os.path.abspath(workspace_directory)
        self.task_filters = [task_filters] if type(task_filters) == str else task_filters
        self.bucket_name = workspace_directory.replace('s3://', '').split('/')[0] if workspace_directory.startswith('s3://') else None
        self.prefix = '/'.join(workspace_directory.replace('s3://', '').split('/')[1:]) if workspace_directory.startswith('s3://') else None
        self.resource = boto3.resource('s3') if workspace_directory.startswith('s3://') else None
        self.s3client = Session().client('s3') if workspace_directory.startswith('s3://') else None
        self.tasks = self._get_tasks_from_s3() if workspace_directory.startswith('s3://') else self._get_tasks()

    def _get_tasks(self):
        """Load all task_log from workspace_directory."""
        files = {str(path) for path in Path(os.path.join(self.workspace_directory, 'log/task_log')).rglob('*')}
        tasks = {}
        for i, x in enumerate(tqdm(files, disable=self.tqdm_disable)):
            n = x.split('/')[-1]
            if self.task_filters and not [x for x in self.task_filters if x in n]:
                continue
            n = n.split('_')
            modified = datetime.fromtimestamp(os.stat(x).st_mtime)
            with open(x, 'rb') as f:
                task_log = pickle.load(f)
            with open(x.replace('task_log', 'task_params'), 'rb') as f:
                task_params = pickle.load(f)
            tasks[i] = {
                'task_name': '_'.join(n[:-1]),
                'task_params': task_params,
                'task_log': task_log,
                'last_modified': modified,
                'task_hash': n[-1].split('.')[0],
            }
        return tasks

    def _get_tasks_from_s3(self):
        """Load all task_log from S3"""
        files = self._get_s3_keys([], '')
        tasks = {}
        for i, x in enumerate(tqdm(files, disable=self.tqdm_disable)):
            n = x['Key'].split('/')[-1]
            if self.task_filters and not [x for x in self.task_filters if x in n]:
                continue
            n = n.split('_')
            tasks[i] = {
                'task_name': '_'.join(n[:-1]),
                'task_params': pickle.loads(self.resource.Object(self.bucket_name, x['Key'].replace('task_log', 'task_params')).get()['Body'].read()),
                'task_log': pickle.loads(self.resource.Object(self.bucket_name, x['Key']).get()['Body'].read()),
                'last_modified': x['LastModified'],
                'task_hash': n[-1].split('.')[0]
            }
        return tasks

    def _get_s3_keys(self, keys=[], marker=''):
        """Recursively get Key from S3.

        Using s3client api by boto module.
        Reference: https://boto3.amazonaws.com/v1/documentation/api/latest/reference/services/s3.html

        Args:
            keys: The object key to get. Increases with recursion.
            marker: S3 marker. The recursion ends when this is gone.

        Returns:
            Object keys from S3. For example: ['hoge', 'piyo', ...]
        """
        response = self.s3client.list_objects(Bucket=self.bucket_name, Prefix=os.path.join(self.prefix, 'log/task_log'), Marker=marker)
        if 'Contents' in response:
            keys.extend([{'Key': content['Key'], 'LastModified': content['LastModified']} for content in response['Contents']])
            if 'Contents' in response and 'IsTruncated' in response:
                return self._get_s3_keys(keys=keys, marker=keys[-1]['Key'])
        return keys

    def get_task_df(self, all_data: bool = False) -> pd.DataFrame:
        """Get task's pandas DataFrame.

        Args:
            all_data: If True, add `task unique hash` and `task log data` to DataFrame.

        Returns:
            All gokart task infomation pandas.DataFrame.
        """
        df = pd.DataFrame([{
            'task_id': k,
            'task_name': v['task_name'],
            'last_modified': v['last_modified'],
            'task_params': v['task_params'],
            'task_hash': v['task_hash'],
            'task_log': v['task_log']
        } for k, v in self.tasks.items()])
        if all_data:
            return df
        return df[['task_id', 'task_name', 'last_modified', 'task_params']]

    def load(self, task_id: int) -> Union[list, Any]:
        """Load File using gokart.load.

        Args:
            task_id: Specify the ID given by Thunderbolt, Read data into memory.
                Please check `task_id` by using Thunderbolt.get_task_df.

        Returns:
            The return value is data or data list. This is because it may be divided when dumping by gokart.
        """
        data = [self._target_load(x) for x in self.tasks[task_id]['task_log']['file_path']]
        data = data[0] if len(data) == 1 else data
        return data

    def _target_load(self, file_name: str) -> Any:
        """Select gokart load_function and load model.

        Args:
            file_name: Path to gokart's output file.

        Returns:
            Loaded data.
        """
        file_path = os.path.join(os.path.dirname(self.workspace_directory), file_name)
        if file_path.endswith('.zip'):
            tmp_path = os.path.join(os.path.dirname(os.path.abspath(__file__)), os.path.abspath(self.tmp_path))
            zip_client = gokart.zip_client_util.make_zip_client(file_path, tmp_path)
            zip_client.unpack_archive()
            load_function_path = os.path.join(tmp_path, 'load_function.pkl')
            load_function = gokart.target.make_target(load_function_path).load()
            model = load_function(os.path.join(tmp_path, 'model.pkl'))
            shutil.rmtree(tmp_path)
            return model
        return gokart.target.make_target(file_path=file_path).load()
