import matplotlib.colors as mcolors
import seaborn as sns
from matplotlib.axes import Axes
from matplotlib.figure import Figure
from pandera.typing import DataFrame

from analytics.gold.wealth_quartiles.schema import WealthQuartiles
from analytics.gold.wealth_spread.schema import WealthSpread
from viz.base import chart, mark_first_rent, set_xlim_with_padding, set_ylim_with_padding
from viz.fast import prepare_ci_stats

_QUARTILE_CYCLE = {"Q1": "C0", "Q2": "C1", "Q3": "C2", "Q4": "C3"}


def plot_wealth_quartiles(
    data: DataFrame[WealthQuartiles],
    figsize: tuple[float, float] = (14, 4),
    max_points: int = 2_000,
    max_t: float | None = None,
    first_rent_time: float | None = None,
) -> tuple[Figure, Axes]:
    required = {"mean", "ci_low", "ci_high"}
    if not required.issubset(set(data.columns)):
        raise ValueError(
            "Missing bootstrap CI columns. Rebuild with analytics.gold.build_wealth_quartiles() "
            "to attach mean/ci_low/ci_high."
        )
    stats = prepare_ci_stats(
        data,
        max_rows=max_points,
        time_col=WealthQuartiles.time,
        group_cols=[WealthQuartiles.quartile],
    )

    with chart(figsize) as (fig, ax):
        palette = {q: mcolors.to_hex(c) for q, c in _QUARTILE_CYCLE.items()}
        sns.lineplot(
            data=stats,
            x=WealthQuartiles.time,
            y="mean",
            hue=WealthQuartiles.quartile,
            palette=palette,
            linewidth=2,
            errorbar=None,
            ax=ax,
        )
        for quartile, g in stats.groupby(WealthQuartiles.quartile, sort=False):
            color = palette.get(str(quartile), None)
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
        mark_first_rent(ax, first_rent_time)
        ax.legend()
        set_ylim_with_padding(ax)
        set_xlim_with_padding(ax, right=max_t)
    return fig, ax


def plot_wealth_spread(
    data: DataFrame[WealthSpread],
    figsize: tuple[float, float] = (14, 4),
    max_points: int = 2_000,
    max_t: float | None = None,
    first_rent_time: float | None = None,
) -> tuple[Figure, Axes]:
    required = {"mean", "ci_low", "ci_high"}
    if not required.issubset(set(data.columns)):
        raise ValueError(
            "Missing bootstrap CI columns. Rebuild with analytics.gold.build_wealth_spread() "
            "to attach mean/ci_low/ci_high."
        )
    stats = prepare_ci_stats(data, max_rows=max_points, time_col=WealthSpread.time)

    with chart(figsize) as (fig, ax):
        color = mcolors.to_hex("C5")
        sns.lineplot(
            data=stats,
            x=WealthSpread.time,
            y="mean",
            color=color,
            linewidth=2,
            errorbar=None,
            ax=ax,
            label="mean",
        )
        ax.fill_between(
            stats[WealthSpread.time].to_numpy(),
            stats["ci_low"].to_numpy(),
            stats["ci_high"].to_numpy(),
            color=color,
            alpha=0.2,
            linewidth=0,
            label="95% CI (bootstrap)",
        )
        ax.axhline(0, ls="--", color="grey", lw=0.8)
        ax.set_title("Wealth Spread (Q4 − Q1)")
        ax.set_xlabel("Time")
        ax.set_ylabel("Q4 − Q1 Wealth")
        mark_first_rent(ax, first_rent_time)
        ax.legend()
        set_xlim_with_padding(ax, right=max_t)
    return fig, ax
