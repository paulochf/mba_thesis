from dotenv import load_dotenv
from pipelines.mba_tcc.etl import prepare_files
from utils.config import get_env_var_as_path

if __name__ == "__main__":
    load_dotenv()

    prepare_files()
