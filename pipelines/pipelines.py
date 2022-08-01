import os
from pathlib import Path

from dotenv import load_dotenv
from mba_tcc.etl import prepare_files

if __name__ == "__main__":
    load_dotenv()
    PATH_DATA_RAW_UCR = os.getenv("PATH_DATA_RAW_UCR")

    dataset_files = Path(PATH_DATA_RAW_UCR).glob("./*.txt")

    prepare_files(dataset_files)
