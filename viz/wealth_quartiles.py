import seaborn as sns
from matplotlib.axes import Axes
from matplotlib.figure import Figure
from pandera.typing import DataFrame

from analytics.gold.wealth_quartiles.schema import WealthQuartiles
from analytics.gold.wealth_spread.schema import WealthSpread
from viz.base import chart
from viz.fast import downsample_evenly

_QUARTILE_PALETTE = {"Q1": "C0", "Q2": "C1", "Q3": "C2", "Q4": "C3"}


def plot_wealth_quartiles(
    data: DataFrame[WealthQuartiles],
    figsize: tuple[float, float] = (14, 4),
    max_points: int = 2_000,
) -> tuple[Figure, Axes]:
    required = {"mean", "ci_low", "ci_high"}
    if not required.issubset(set(data.columns)):
        raise ValueError(
            "Missing bootstrap CI columns. Rebuild with analytics.gold.build_wealth_quartiles() "
            "to attach mean/ci_low/ci_high."
        )
    stats = (
        data[[WealthQuartiles.time, WealthQuartiles.quartile, "mean", "ci_low", "ci_high"]]
        .drop_duplicates()
        .sort_values([WealthQuartiles.quartile, WealthQuartiles.time], kind="stable")
    )
    stats = downsample_evenly(stats, max_rows=max_points, sort_by=[WealthQuartiles.quartile, WealthQuartiles.time])

    with chart(figsize) as (fig, ax):
        sns.lineplot(
            data=stats,
            x=WealthQuartiles.time,
            y="mean",
            hue=WealthQuartiles.quartile,
            palette=_QUARTILE_PALETTE,
            linewidth=2,
            errorbar=None,
            ax=ax,
        )
        for quartile, g in stats.groupby(WealthQuartiles.quartile, sort=False):
            color = _QUARTILE_PALETTE.get(str(quartile), None)
            ax.fill_between(
                g[WealthQuartiles.time].to_numpy(),
                g["ci_low"].to_numpy(),
                g["ci_high"].to_numpy(),
                alpha=0.15,
                linewidth=0,
                color=color,
            )
        ax.set_title("Renter Wealth by Quartile (95% CI)")
        ax.set_xlabel("Time")
        ax.set_ylabel("Money")
        ax.legend()
    return fig, ax


def plot_wealth_spread(
    data: DataFrame[WealthSpread],
    figsize: tuple[float, float] = (14, 4),
    max_points: int = 2_000,
) -> tuple[Figure, Axes]:
    required = {"mean", "ci_low", "ci_high"}
    if not required.issubset(set(data.columns)):
        raise ValueError(
            "Missing bootstrap CI columns. Rebuild with analytics.gold.build_wealth_spread() "
            "to attach mean/ci_low/ci_high."
        )
    stats = (
        data[[WealthSpread.time, "mean", "ci_low", "ci_high"]]
        .drop_duplicates()
        .sort_values(WealthSpread.time, kind="stable")
    )
    stats = downsample_evenly(stats, max_rows=max_points, sort_by=WealthSpread.time)

    with chart(figsize) as (fig, ax):
        sns.lineplot(
            data=stats,
            x=WealthSpread.time,
            y="mean",
            color="C5",
            linewidth=2,
            errorbar=None,
            ax=ax,
            label="mean",
        )
        ax.fill_between(
            stats[WealthSpread.time].to_numpy(),
            stats["ci_low"].to_numpy(),
            stats["ci_high"].to_numpy(),
            color="C5",
            alpha=0.2,
            linewidth=0,
            label="95% CI (bootstrap)",
        )
        ax.axhline(0, ls="--", color="grey", lw=0.8)
        ax.set_title("Wealth Spread (Q4 − Q1)")
        ax.set_xlabel("Time")
        ax.set_ylabel("Q4 − Q1 Wealth")
        ax.legend()
    return fig, ax
