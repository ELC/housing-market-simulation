import seaborn as sns
from matplotlib.axes import Axes
from matplotlib.figure import Figure
from pandera.typing import DataFrame

from analytics.gold import TimeToRentRolling
from viz.base import chart, mark_first_rent, set_xlim_with_padding, set_ylim_with_padding
from viz.fast import prepare_ci_stats


def plot_time_to_rent(
    data: DataFrame[TimeToRentRolling],
    figsize: tuple[float, float] = (14, 4),
    max_points: int = 2_000,
    max_t: float | None = None,
    first_rent_time: float | None = None,
) -> tuple[Figure, Axes]:
    stats = prepare_ci_stats(data, max_rows=max_points, time_col=TimeToRentRolling.time)

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
        mark_first_rent(ax, first_rent_time)
        ax.legend()
        set_ylim_with_padding(ax)
        set_xlim_with_padding(ax, right=max_t)
    return fig, ax
