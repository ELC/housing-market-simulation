import seaborn as sns
from matplotlib.axes import Axes
from matplotlib.figure import Figure
from pandera.typing import DataFrame

from analytics.gold import TimeToRentRolling
from viz.base import chart
from viz.fast import downsample_evenly


def plot_time_to_rent(
    data: DataFrame[TimeToRentRolling],
    figsize: tuple[float, float] = (14, 4),
    max_points: int = 2_000,
) -> tuple[Figure, Axes]:
    stats = (
        data[[TimeToRentRolling.time, "mean", "ci_low", "ci_high"]]
        .drop_duplicates()
        .sort_values(TimeToRentRolling.time, kind="stable")
    )
    stats = downsample_evenly(stats, max_rows=max_points, sort_by=TimeToRentRolling.time)

    with chart(figsize) as (fig, ax):
        sns.lineplot(
            data=stats,
            x=TimeToRentRolling.time,
            y="mean",
            color="C0",
            lw=2,
            errorbar=None,
            ax=ax,
            label="mean",
        )
        ax.fill_between(
            stats[TimeToRentRolling.time].to_numpy(),
            stats["ci_low"].to_numpy(),
            stats["ci_high"].to_numpy(),
            color="C0",
            alpha=0.2,
            linewidth=0,
            label="95% CI (bootstrap)",
        )
        ax.set_title("Time to Rent a Vacant House (95% CI)")
        ax.set_xlabel("Time")
        ax.set_ylabel("Duration (periods)")
        ax.legend()
        ax.set_ylim(bottom=0)
    return fig, ax
