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
        ax.plot(
            data[TimeToRentRolling.time],
            data[TimeToRentRolling.rolling_mean],
            color="C1",
            linewidth=2,
            label="running mean",
        )
        ax.fill_between(
            data[TimeToRentRolling.time],
            data[TimeToRentRolling.rolling_mean] - 1.96 * data[TimeToRentRolling.rolling_std],
            data[TimeToRentRolling.rolling_mean] + 1.96 * data[TimeToRentRolling.rolling_std],
            alpha=0.2,
            color="C1",
            label="95% CI",
        )
        ax.set_title("Time to Rent a Vacant House")
        ax.set_xlabel("Time")
        ax.set_ylabel("Duration (periods)")
        ax.legend()
        ax.set_ylim(bottom=0)
    return fig, ax
