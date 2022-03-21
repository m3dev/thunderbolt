import os
import shutil
from typing import Any, Dict, List, Union

import gokart
import pandas as pd

from thunderbolt.client.gcs_client import GCSClient
from thunderbolt.client.local_directory_client import LocalDirectoryClient
from thunderbolt.client.s3_client import S3Client


class Thunderbolt:

    def __init__(self,
                 workspace_directory: str = '',
                 task_filters: Union[str, List[str]] = '',
                 use_tqdm: bool = False,
                 tmp_path: str = './tmp',
                 use_cache: bool = True):
        """Thunderbolt init.

        Set the path to the directory or S3.

        Args:
            workspace_directory: Gokart's TASK_WORKSPACE_DIRECTORY. If None, use $TASK_WORKSPACE_DIRECTORY in os.env.
            task_filters: Filter for task name.
                Load only tasks that contain the specified string here. We can also specify the number of copies.
            use_tqdm: Flag of using tdqm. If False, tqdm not be displayed (default=False).
            tmp_path: Temporary directory when use external load function.
            use_cache: Flag of using Log Cache.
        """
        self.tmp_path = tmp_path
        if not workspace_directory:
            env = os.getenv('TASK_WORKSPACE_DIRECTORY')
            workspace_directory = env if env else ''
        self.client = self._get_client(workspace_directory, [task_filters] if type(task_filters) == str else task_filters, not use_tqdm, use_cache)
        self.tasks = self._get_tasks_dic(tasks_list=self.client.get_tasks())

    def _get_client(self, workspace_directory, filters, tqdm_disable, use_cache):
        if workspace_directory.startswith('s3://'):
            return S3Client(workspace_directory, filters, tqdm_disable, use_cache)
        elif workspace_directory.startswith('gs://') or workspace_directory.startswith('gcs://'):
            return GCSClient(workspace_directory, filters, tqdm_disable, use_cache)
        return LocalDirectoryClient(workspace_directory, filters, tqdm_disable, use_cache)

    def _get_tasks_dic(self, tasks_list: List[Dict]) -> Dict[int, Dict]:
        return {i: task for i, task in enumerate(sorted(tasks_list, key=lambda x: x['last_modified']))}

    def get_task_df(self, all_data: bool = False) -> pd.DataFrame:
        """Get task's pandas DataFrame.

        Args:
            all_data: If True, add `task unique hash` and `task log data` to DataFrame.

        Returns:
            All gokart task infomation pandas.DataFrame.
        """
        if self.tasks:
            df = pd.DataFrame([{
                'task_id': k,
                'task_name': v['task_name'],
                'last_modified': v['last_modified'],
                'task_params': v['task_params'],
                'task_hash': v['task_hash'],
                'task_log': v['task_log']
            } for k, v in self.tasks.items()])
        else:
            df = pd.DataFrame(columns=['task_id', 'task_name', 'last_modified', 'task_params', 'task_hash', 'task_log'])

        if all_data:
            return df
        return df[['task_id', 'task_name', 'last_modified', 'task_params']]

    def get_data(self, task_name: str, top_k: int = 1) -> Union[list, Any]:
        """Load newest task output data.

        Args:
            task_name: gokart's task name.
            top_k: top-k of newest output data.

        Returns:
            The return value is newest data or data list.
        """
        df = self.get_task_df()
        df = df.sort_values(by='last_modified', ascending=False)

        data = [self.load(df.query(f'task_name=="{task_name}"')['task_id'].iloc[i]) for i in range(top_k)]
        if len(data) == 1:
            return data[0]
        return data

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
        file_path = self.client.to_absolute_path(file_name)
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
