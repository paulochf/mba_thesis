from typing import Any

from dotenv import load_dotenv

from mba_tcc.pipeline.etl import etl_flow
from mba_tcc.pipeline.train import train_flow


def main() -> Any:
    load_dotenv()

    etl_flow()
    # train_flow()
