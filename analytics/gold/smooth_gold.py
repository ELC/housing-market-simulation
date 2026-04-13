import numpy as np

from analytics.gold.model import Gold
from analytics.gold.smoother.base import Smoother


def _smooth_stats_table(
    gold: Gold,
    field: str,
    smoother: Smoother,
    *,
    x_col: str,
    group_cols: list[str] | None = None,
) -> None:
    df = getattr(gold, field).copy()
    cols = ["mean", "ci_low", "ci_high"]

    if group_cols:
        for _, g in df.groupby(group_cols, sort=False):
            idx = g.index
            for col in cols:
                smoothed = smoother.smooth(g[[x_col, col]], x_col=x_col, y_col=col)
                df.loc[idx, col] = smoothed[col].values
    else:
        for col in cols:
            df = smoother.smooth(df, x_col=x_col, y_col=col)

    lo = df["ci_low"].to_numpy(dtype=float)
    hi = df["ci_high"].to_numpy(dtype=float)
    mean = df["mean"].to_numpy(dtype=float)
    lo2 = np.minimum(lo, hi)
    hi2 = np.maximum(lo, hi)
    df["ci_low"] = np.minimum(lo2, mean)
    df["ci_high"] = np.maximum(hi2, mean)

    setattr(gold, field, df)


class SmootherTransformer:
    def __init__(self, smoother: Smoother) -> None:
        self._smoother = smoother

    def __call__(self, gold: Gold) -> Gold:
        gold = gold.model_copy()

        _smooth_stats_table(gold, "housed_renter_wealth", self._smoother, x_col="time")
        _smooth_stats_table(gold, "wealth_spread", self._smoother, x_col="time")
        _smooth_stats_table(gold, "rent_comparison", self._smoother, x_col="time", group_cols=["kind"])
        _smooth_stats_table(gold, "wealth_quartiles", self._smoother, x_col="time", group_cols=["quartile"])

        _smooth_stats_table(gold, "time_to_rent_rolling", self._smoother, x_col="time")
        _smooth_stats_table(gold, "rent_duration_rolling", self._smoother, x_col="time")

        _smooth_stats_table(gold, "agent_population", self._smoother, x_col="time")

        return gold
