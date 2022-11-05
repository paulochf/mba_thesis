from functools import partial

from mba_tcc.utils.config import DEFAULT_VAL_COLUMN


def make_column(col_name: str = DEFAULT_VAL_COLUMN, stat_name: str = None, k: int = None) -> str:
    return f"{col_name}_{stat_name}_{k}"


make_column_mean = partial(make_column, stat_name="rolling_mean")
make_column_std = partial(make_column, stat_name="rolling_std")
make_column_residual = partial(make_column, stat_name="rolling_residual")
make_column_zscore = partial(make_column, stat_name="rolling_zscore")
make_column_sigma3_upper = partial(make_column, stat_name="rolling_sigma_upper")
make_column_sigma3_lower = partial(make_column, stat_name="rolling_sigma_lower")
