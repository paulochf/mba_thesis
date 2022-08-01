from dotenv import load_dotenv

from mba_tcc.etl import prepare_files
from utils.config import get_env_var_as_path


if __name__ == "__main__":
    load_dotenv()

    dataset_path = get_env_var_as_path("PATH_DATA_RAW_UCR")
    dataset_files = dataset_path.glob("./*.txt")

    prepare_files(dataset_files)
