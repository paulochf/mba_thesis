from dotenv import load_dotenv

from pipeline.etl import prepare_files

if __name__ == "__main__":
    load_dotenv()

    prepare_files()
