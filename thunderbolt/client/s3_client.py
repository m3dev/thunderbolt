import os
import pickle
from typing import List, Dict, Any

import boto3
from boto3 import Session
from tqdm import tqdm


class S3Client:
    def __init__(self, workspace_directory: str = '', task_filters: List[str] = [], tqdm_disable: bool = False):
        self.workspace_directory = workspace_directory
        self.task_filters = task_filters
        self.tqdm_disable = tqdm_disable
        self.bucket_name = workspace_directory.replace('s3://', '').split('/')[0]
        self.prefix = '/'.join(workspace_directory.replace('s3://', '').split('/')[1:])
        self.resource = boto3.resource('s3')
        self.s3client = Session().client('s3')

    def get_tasks(self) -> Dict[int, Dict[str, Any]]:
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

    def _get_s3_keys(self, keys: List[Dict[str, Any]] = [], marker: str = '') -> List[Dict[str, Any]]:
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
