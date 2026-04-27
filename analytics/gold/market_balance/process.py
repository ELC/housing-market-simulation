import pandas as pd
from pandera.typing import DataFrame

from analytics.gold.bootstrap import bootstrap_logit_ci_by_group
from analytics.gold.market_balance.schema import GoldMarketBalance


def build_market_balance(aggregate: pd.DataFrame) -> DataFrame[GoldMarketBalance]:
    if aggregate.empty:
        return GoldMarketBalance.validate(
            pd.DataFrame(columns=["time", "metric", "mean", "ci_low", "ci_high"]),
        )
    stats = bootstrap_logit_ci_by_group(
        aggregate,
        group_cols=["time", "metric"],
        value_col="rate",
    )
    return GoldMarketBalance.validate(stats.drop(columns=["n"], errors="ignore"))
