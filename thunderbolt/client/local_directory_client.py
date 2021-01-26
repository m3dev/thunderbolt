from datetime import datetime
import os
from pathlib import Path
import warnings
import pickle
from typing import List, Dict, Any

from tqdm import tqdm


class LocalDirectoryClient:
    def __init__(self, workspace_directory: str = '', task_filters: List[str] = [], tqdm_disable: bool = False):
        self.workspace_directory = os.path.abspath(workspace_directory)
        self.task_filters = task_filters
        self.tqdm_disable = tqdm_disable

    def get_tasks(self) -> List[Dict[str, Any]]:
        """Load all task_log from workspace_directory."""
        files = {str(path) for path in Path(os.path.join(self.workspace_directory, 'log/task_log')).rglob('*')}
        tasks_list = list()
        for x in tqdm(files, disable=self.tqdm_disable):
            n = x.split('/')[-1]
            if self.task_filters and not [x for x in self.task_filters if x in n]:
                continue
            n = n.split('_')

            try:
                modified = datetime.fromtimestamp(os.stat(x).st_mtime)
                with open(x, 'rb') as f:
                    task_log = pickle.load(f)
                with open(x.replace('task_log', 'task_params'), 'rb') as f:
                    task_params = pickle.load(f)
            except Exception:
                continue

            tasks_list.append({
                'task_name': '_'.join(n[:-1]),
                'task_params': task_params,
                'task_log': task_log,
                'last_modified': modified,
                'task_hash': n[-1].split('.')[0],
            })

        if len(tasks_list) != len(files):
            warnings.warn(f'[NOT FOUND LOGS] target file: {len(files)}, found log file: {len(tasks_list)}')

        return tasks_list

    def to_absolute_path(self, x: str) -> str:
        """get file path"""
        x = x.lstrip('.').lstrip('/')
        if self.workspace_directory.rstrip('/').split('/')[-1] == x.split('/')[0]:
            x = '/'.join(x.split('/')[1:])
        x = os.path.join(self.workspace_directory, x)
        return os.path.abspath(x)
