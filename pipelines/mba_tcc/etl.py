from prefect import flow

from mba_tcc.etl.fix_file import fix_files
from mba_tcc.file_index import make_files_index


@flow()
def prepare_files():
    make_files_index()
    fix_files()
