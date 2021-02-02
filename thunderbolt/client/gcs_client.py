import os
import pickle
from tqdm import tqdm
import warnings
from datetime import datetime
from typing import List, Dict, Any

from thunderbolt.client.local_cache import LocalCache

from gokart.gcs_config import GCSConfig


class GCSClient:
    def __init__(self, workspace_directory: str = '', task_filters: List[str] = [], tqdm_disable: bool = False, use_cache: bool = True):
        """must set $GCS_CREDENTIAL"""
        self.workspace_directory = workspace_directory
        self.task_filters = task_filters
        self.tqdm_disable = tqdm_disable
        self.gcs_client = GCSConfig().get_gcs_client()
        self.local_cache = LocalCache(workspace_directory, use_cache)
        self.use_cache = use_cache

    def get_tasks(self) -> List[Dict[str, Any]]:
        """Load all task_log from GCS"""
        files = self._get_gcs_objects()
        tasks_list = list()
        for x in tqdm(files, disable=self.tqdm_disable):
            n = x.split('/')[-1]
            if self.task_filters and not [f for f in self.task_filters if f in n]:
                continue
            n = n.split('_')

            if self.use_cache:
                cache = self.local_cache.get(x)
                if cache:
                    tasks_list.append(cache)
                    continue

            try:
                meta = self._get_gcs_object_info(x)
                params = {
                    'task_name': '_'.join(n[:-1]),
                    'task_params': pickle.load(self.gcs_client.download(x.replace('task_log', 'task_params'))),
                    'task_log': pickle.load(self.gcs_client.download(x)),
                    'last_modified': datetime.strptime(meta['updated'].split('.')[0], '%Y-%m-%dT%H:%M:%S'),
                    'task_hash': n[-1].split('.')[0]
                }
                tasks_list.append(params)
                if self.use_cache:
                    self.local_cache.dump(x, params)
            except Exception:
                continue

        if len(tasks_list) != len(list(files)):
            warnings.warn(f'[NOT FOUND LOGS] target file: {len(list(files))}, found log file: {len(tasks_list)}')

        return tasks_list

    def _get_gcs_objects(self) -> List[str]:
        """get GCS objects"""
        return self.gcs_client.listdir(os.path.join(self.workspace_directory, 'log/task_log'))

    def _get_gcs_object_info(self, x: str) -> Dict[str, str]:
        """get GCS object meta data"""
        bucket, obj = self.gcs_client._path_to_bucket_and_key(x)
        return self.gcs_client.client.objects().get(bucket=bucket, object=obj).execute()

    def to_absolute_path(self, x: str) -> str:
        """get GCS file path"""
        x = x.lstrip('.').lstrip('/')
        if self.workspace_directory.rstrip('/').split('/')[-1] == x.split('/')[0]:
            x = '/'.join(x.split('/')[1:])
        return x
