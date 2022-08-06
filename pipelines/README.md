# MBA TCC - Pipelines and Data Workflows

## Installation

    pip install pipenv  # if you haven't already
    pipenv install
    pipenv run main

## Development

    pipenv install --dev
    pipenv shell

## Test

    pipenv run test

## Directory structure

    ├── Pipfile               <- The Pipfile for reproducing the pipelines environment
    ├── run_pipe.py           <- The CLI entry point for all the pipelines
    ├── <repo_name>           <- Code for the various steps of the pipelines
    │   ├── tasks
    │   │   └── etl           <- prefect flows folder
    │   │       ├── fix.py    <- fix flow
    │   │       ├── index.py  <- index flow
    │   │       └── split.py  <- split flow
    │   ├── etl.py            <- Download, generate, and process data
    │   ├── visualize.py      <- Create exploratory and results oriented visualizations
    │   ├── features.py       <- Turn raw data into features for modeling
    │   └── train.py          <- Train and evaluate models
    ├── tests
    │   ├── pytest.ini        <- Test configuration file
    │   ├── etl               <- Tests for each flow file
    │   │   ├── test_fix.py   <- Test fix flow
    │   │   ├── test_index.py <- Test index flow
    │   │   └── test_split.py <- Test split flow
    │   └── fixtures          <- Where to put example inputs and outputs
    │       ├── input.json    <- Test input data
    │       └── output.json   <- Test output data
    └── utils
        ├── config.py         <- Auxiliary functions for configuration (e.g., env vars)
        ├── testing.py        <- Auxiliary functions for tests (e.g., assert files)
        └── transformation.py <- Auxiliary functions for data transforms (e.g., file split)
