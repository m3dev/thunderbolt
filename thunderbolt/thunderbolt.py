from datetime import datetime
import os
from pathlib import Path
import pickle

import boto3
from boto3 import Session
import gokart
import pandas as pd
from tqdm import tqdm


class Thunderbolt():
    def __init__(self, file_path: str, task_filters=''):
        self.s3client = None
        self.file_path = file_path
        self.task_filters = [task_filters] if type(task_filters) == str else task_filters
        self.bucket_name = file_path.replace('s3://', '').split('/')[0] if file_path.startswith('s3://') else None
        self.prefix = '/'.join(file_path.replace('s3://', '').split('/')[1:]) if file_path.startswith('s3://') else None
        self.resource = boto3.resource('s3') if file_path.startswith('s3://') else None
        self.s3client = Session().client('s3') if file_path.startswith('s3://') else None
        self.tasks = self._get_tasks_from_s3() if file_path.startswith('s3://') else self._get_tasks()

    def _get_tasks(self):
        """Get task parameters."""
        files = {str(path) for path in Path(os.path.join(self.file_path, 'log/task_log')).rglob('*')}
        tasks = {}
        for i, x in enumerate(tqdm(files)):
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
        """Get task parameters from S3."""
        files = self._get_s3_keys([], '')
        tasks = {}
        for i, x in enumerate(tqdm(files)):
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

    def _get_s3_keys(self, keys: list = [], marker: str = '') -> list:
        """Recursively get Key from S3."""
        response = self.s3client.list_objects(Bucket=self.bucket_name, Prefix=os.path.join(self.prefix, 'log/task_log'), Marker=marker)
        if 'Contents' in response:
            keys.extend([{'Key': content['Key'], 'LastModified': content['LastModified']} for content in response['Contents']])
            if 'IsTruncated' in response:
                return self._get_s3_keys(keys=keys, marker=keys[-1]['Key'])
        return keys

    def get_task_df(self, all_data: bool = False) -> pd.DataFrame:
        """Get task's pandas data frame."""
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

    def load(self, task_id: int) -> list:
        """Load File."""
        return [gokart.target.make_target(file_path=x).load() for x in self.tasks[task_id]['task_log']['file_path']]
