import os
import pickle
import shutil
from pathlib import Path
from typing import Optional


class LocalCache:
    def __init__(self, workspace_directory: str, use_cache: bool):
        """Log file cache.

        dump file: ./.thunderbolt/resources/{task_hash}.pkl
        """
        self.cache_dir = Path(os.path.join(os.getcwd(), '.thunderbolt', workspace_directory.split('/')[-1]))
        if use_cache:
            self.cache_dir.mkdir(parents=True, exist_ok=True)

    def get(self, file_name: str) -> Optional[dict]:
        cache_file_path = self._convert_file_path(file_name)
        if cache_file_path.exists():
            with cache_file_path.open(mode='rb') as f:
                params = pickle.load(f)
            return params
        return None

    def dump(self, file_name: str, params: dict):
        cache_file_path = self._convert_file_path(file_name)
        with cache_file_path.open(mode='wb') as f:
            pickle.dump(params, f)

    def clear(self):
        shutil.rmtree(os.path.join(os.getcwd(), '.thunderbolt'))

    def _convert_file_path(self, file_name: str) -> Path:
        file_name = file_name.split('/')[-1]
        cache_file_path = self.cache_dir.joinpath(file_name)
        return cache_file_path.with_suffix('.pkl')
