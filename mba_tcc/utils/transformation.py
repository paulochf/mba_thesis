from typing import Any

import numpy as np

from json import JSONEncoder, dumps
from pathlib import Path


class NumpyEncoder(JSONEncoder):
    def default(self, obj):
        if isinstance(obj, np.integer):
            return int(obj)
        if isinstance(obj, np.floating):
            return float(obj)
        if isinstance(obj, np.ndarray):
            return obj.tolist()

        return str(obj)
        # return super(NumpyEncoder, self).default(obj)


def path_as_parquet(folder_path: Path, file_name: str) -> Path:
    return (folder_path / file_name).with_suffix(".parquet")


def get_dataset_folder(parent_path: Path, file_number: int, mnemonic: str) -> Path:
    """

    :rtype: object
    """
    return parent_path / f"{file_number:03d}_{mnemonic}"


def save_as_json(obj: Any, save_path: Path):
    save_path.write_text(dumps(obj, indent=4, sort_keys=True, cls=NumpyEncoder))
