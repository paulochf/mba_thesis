from dotenv import load_dotenv

from pipelines.mba_tcc.etl import prepare_files

if __name__ == "__main__":
    load_dotenv()

    prepare_files()
