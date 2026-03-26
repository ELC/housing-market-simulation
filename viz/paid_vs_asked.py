import seaborn as sns
from matplotlib.axes import Axes
from matplotlib.figure import Figure
from pandera.typing import DataFrame

from analytics.gold import RentComparison
from viz.base import chart


def plot_paid_vs_asked(
    data: DataFrame[RentComparison],
    figsize: tuple[float, float] = (14, 4),
) -> tuple[Figure, Axes]:
    with chart(figsize) as (fig, ax):
        sns.lineplot(
            data=data,
            x=RentComparison.time,
            y=RentComparison.amount,
            hue=RentComparison.kind,
            ax=ax,
        )
        ax.set_title("Average Paid Rent vs Asked Rent (95% CI)")
        ax.set_xlabel("Time")
        ax.set_ylabel("Rent")
        ax.legend()
    return fig, ax
