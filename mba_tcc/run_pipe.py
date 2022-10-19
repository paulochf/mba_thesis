from typing import Any

import matplotlib as mpl
from dotenv import load_dotenv

from mba_tcc.pipeline.analyze import analyze_flow
from mba_tcc.pipeline.etl import etl_flow
from mba_tcc.pipeline.predict import predict_flow
from mba_tcc.pipeline.train import train_flow


mpl.use("Agg")


def main() -> Any:
    load_dotenv()

    # etl_flow()
    # train_flow()
    # predict_flow()
    analyze_flow()
