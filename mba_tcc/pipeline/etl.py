from pathlib import Path

from prefect import flow

from mba_tcc.utils.config import get_env_var_as_path
from mba_tcc.pipeline.tasks.etl.fix import fix_files
from mba_tcc.pipeline.tasks.etl.index import make_files_index
from mba_tcc.pipeline.tasks.etl.tag import tag_ranges_and_counts


@flow()
def etl_flow():
    final_path: Path = get_env_var_as_path("PATH_DATA_FINAL")

    make_files_index(final_path)
    fix_files()
    tag_ranges_and_counts()
