import seaborn as sns
from matplotlib.axes import Axes
from matplotlib.figure import Figure
from pandera.typing import DataFrame

from analytics.gold.wealth_quartiles.schema import WealthQuartiles
from analytics.gold.wealth_spread.schema import WealthSpread
from viz.base import chart

_QUARTILE_PALETTE = {"Q1": "C0", "Q2": "C1", "Q3": "C2", "Q4": "C3"}


def plot_wealth_quartiles(
    data: DataFrame[WealthQuartiles],
    figsize: tuple[float, float] = (14, 4),
) -> tuple[Figure, Axes]:
    with chart(figsize) as (fig, ax):
        sns.lineplot(
            data=data,
            x=WealthQuartiles.time,
            y=WealthQuartiles.money,
            hue=WealthQuartiles.quartile,
            palette=_QUARTILE_PALETTE,
            ax=ax,
        )
        ax.set_title("Renter Wealth by Quartile (95% CI)")
        ax.set_xlabel("Time")
        ax.set_ylabel("Money")
    return fig, ax


def plot_wealth_spread(
    data: DataFrame[WealthSpread],
    figsize: tuple[float, float] = (14, 4),
) -> tuple[Figure, Axes]:
    with chart(figsize) as (fig, ax):
        sns.lineplot(
            data=data,
            x=WealthSpread.time,
            y=WealthSpread.spread,
            color="C5",
            ax=ax,
        )
        ax.axhline(0, ls="--", color="grey", lw=0.8)
        ax.set_title("Wealth Spread (Q4 − Q1)")
        ax.set_xlabel("Time")
        ax.set_ylabel("Q4 − Q1 Wealth")
    return fig, ax
