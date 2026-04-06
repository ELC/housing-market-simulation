from matplotlib.axes import Axes
from matplotlib.figure import Figure
from pandera.typing import DataFrame

from analytics.gold.wealth_quartiles.schema import WealthQuartiles
from viz.base import chart

_QUARTILE_COLORS = {"Q1": "C0", "Q2": "C1", "Q3": "C2", "Q4": "C3"}


def plot_wealth_quartiles(
    data: DataFrame[WealthQuartiles],
    figsize: tuple[float, float] = (14, 4),
) -> tuple[Figure, Axes]:
    with chart(figsize) as (fig, ax):
        for q, color in _QUARTILE_COLORS.items():
            subset = data[data[WealthQuartiles.quartile] == q]
            ax.plot(
                subset[WealthQuartiles.time],
                subset[WealthQuartiles.mean_wealth],
                color=color,
                linewidth=1.5,
                label=q,
            )
        ax.set_title("Renter Wealth by Quartile")
        ax.set_xlabel("Time")
        ax.set_ylabel("Mean Wealth")
        ax.legend()
    return fig, ax


def plot_wealth_spread(
    data: DataFrame[WealthQuartiles],
    figsize: tuple[float, float] = (14, 4),
) -> tuple[Figure, Axes]:
    q1 = data[data[WealthQuartiles.quartile] == "Q1"].set_index(WealthQuartiles.time)[WealthQuartiles.mean_wealth]
    q4 = data[data[WealthQuartiles.quartile] == "Q4"].set_index(WealthQuartiles.time)[WealthQuartiles.mean_wealth]
    spread = (q4 - q1).dropna()

    with chart(figsize) as (fig, ax):
        ax.plot(spread.index, spread.values, color="C5", linewidth=1.5)
        ax.axhline(0, ls="--", color="grey", lw=0.8)
        ax.set_title("Wealth Spread (Q4 − Q1)")
        ax.set_xlabel("Time")
        ax.set_ylabel("Q4 − Q1 Mean Wealth")
    return fig, ax
