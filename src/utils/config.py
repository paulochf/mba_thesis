import os
from pathlib import Path


def get_env_var_as_path(var_name: str) -> Path:
    var_value = os.getenv(var_name)
    if not var_value:
        raise ValueError(f"Env var {var_name} not found.")

    return Path(var_value)
