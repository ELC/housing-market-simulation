from matplotlib.axes import Axes
from matplotlib.figure import Figure
from pandera.typing import DataFrame

from analytics.gold import RentDurationRolling
from viz.base import chart


def plot_rent_duration(
    data: DataFrame[RentDurationRolling],
    figsize: tuple[float, float] = (14, 4),
) -> tuple[Figure, Axes]:
    with chart(figsize) as (fig, ax):
        ax.scatter(
            data[RentDurationRolling.time],
            data[RentDurationRolling.duration],
            alpha=0.25,
            s=25,
            color="C0",
            label="individual",
        )
        ax.plot(
            data[RentDurationRolling.time],
            data[RentDurationRolling.rolling_mean],
            color="C1",
            linewidth=2,
            label="running mean",
        )
        ax.fill_between(
            data[RentDurationRolling.time],
            data[RentDurationRolling.rolling_mean] - 1.96 * data[RentDurationRolling.rolling_std],
            data[RentDurationRolling.rolling_mean] + 1.96 * data[RentDurationRolling.rolling_std],
            alpha=0.2,
            color="C1",
            label="95% CI",
        )
        ax.set_title("Rent Duration (Time Until Eviction)")
        ax.set_xlabel("Time")
        ax.set_ylabel("Duration (periods)")
        ax.legend()
        ax.set_ylim(bottom=0)
    return fig, ax
