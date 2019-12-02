import os
import pickle
from tqdm import tqdm
from datetime import datetime
from typing import List, Dict, Any

from gokart.gcs_config import GCSConfig


class GCSClient:
    def __init__(self, workspace_directory: str = '', task_filters: List[str] = [], tqdm_disable: bool = False):
        """must set $GOOGLE_APPLICATION_CREDENTIALS"""
        self.workspace_directory = workspace_directory
        self.task_filters = task_filters
        self.tqdm_disable = tqdm_disable
        self.gcs_client = GCSConfig().get_gcs_client()

    def get_tasks(self) -> Dict[int, Dict[str, Any]]:
        """Load all task_log from GCS"""
        files = self._get_gcs_objects()
        tasks = {}
        for i, x in enumerate(tqdm(files, disable=self.tqdm_disable)):
            n = x.split('/')[-1]
            if self.task_filters and not [f for f in self.task_filters if f in n]:
                continue
            n = n.split('_')
            meta = self._get_gcs_object_info(x)
            tasks[i] = {
                'task_name': '_'.join(n[:-1]),
                'task_params': pickle.load(self.gcs_client.download(x.replace('task_log', 'task_params'))),
                'task_log': pickle.load(self.gcs_client.download(x)),
                'last_modified': datetime.strptime(meta['updated'].split('.')[0], '%Y-%m-%dT%H:%M:%S'),
                'task_hash': n[-1].split('.')[0]
            }
        return tasks

    def _get_gcs_objects(self) -> List[str]:
        """get GCS objects"""
        return self.gcs_client.listdir(os.path.join(self.workspace_directory, 'log/task_log'))

    def _get_gcs_object_info(self, x: str) -> Dict[str, str]:
        """get GCS object meta data"""
        bucket, obj = self.gcs_client._path_to_bucket_and_key(x)
        return self.gcs_client.client.objects().get(bucket=bucket, object=obj).execute()
