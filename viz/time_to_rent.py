import seaborn as sns
from matplotlib.axes import Axes
from matplotlib.figure import Figure
from pandera.typing import DataFrame

from analytics.gold import TimeToRentRolling
from viz.base import chart


def plot_time_to_rent(
    data: DataFrame[TimeToRentRolling],
    figsize: tuple[float, float] = (14, 4),
) -> tuple[Figure, Axes]:
    with chart(figsize) as (fig, ax):
        ax.scatter(
            data[TimeToRentRolling.time],
            data[TimeToRentRolling.duration],
            alpha=0.25,
            s=25,
            color="C0",
            label="individual",
        )
        sns.lineplot(
            data=data,
            x=TimeToRentRolling.time,
            y=TimeToRentRolling.duration,
            color="C1",
            ax=ax,
        )
        ax.set_title("Time to Rent a Vacant House")
        ax.set_xlabel("Time")
        ax.set_ylabel("Duration (periods)")
        ax.legend()
        ax.set_ylim(bottom=0)
    return fig, ax
