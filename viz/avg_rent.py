import seaborn as sns
from matplotlib.axes import Axes
from matplotlib.figure import Figure
from pandera.typing import DataFrame

from analytics.gold import RentPayments
from viz.base import chart


def plot_avg_rent(
    data: DataFrame[RentPayments],
    figsize: tuple[float, float] = (14, 4),
) -> tuple[Figure, Axes]:
    with chart(figsize) as (fig, ax):
        sns.lineplot(data=data, x=RentPayments.time, y=RentPayments.amount, ax=ax)
        ax.set_title("Average Rent Payment (95% CI)")
        ax.set_xlabel("Time")
        ax.set_ylabel("Amount")
    return fig, ax
