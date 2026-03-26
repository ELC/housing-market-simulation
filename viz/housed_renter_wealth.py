import seaborn as sns
from matplotlib.axes import Axes
from matplotlib.figure import Figure
from pandera.typing import DataFrame

from analytics.gold import HousedRenterWealth
from viz.base import chart


def plot_housed_renter_wealth(
    data: DataFrame[HousedRenterWealth],
    figsize: tuple[float, float] = (14, 4),
) -> tuple[Figure, Axes]:
    with chart(figsize) as (fig, ax):
        sns.lineplot(
            data=data,
            x=HousedRenterWealth.time,
            y=HousedRenterWealth.money,
            ax=ax,
        )
        ax.set_title("Average Housed-Renter Wealth (95% CI)")
        ax.set_xlabel("Time")
        ax.set_ylabel("Money")
    return fig, ax
