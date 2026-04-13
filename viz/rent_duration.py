import seaborn as sns
from matplotlib.axes import Axes
from matplotlib.figure import Figure
from pandera.typing import DataFrame

from analytics.gold import RentDurationRolling
from core.settings import SimulationSettings
from viz.base import chart
from viz.fast import downsample_evenly


def plot_rent_duration(
    data: DataFrame[RentDurationRolling],
    settings: SimulationSettings | None = None,
    figsize: tuple[float, float] = (14, 4),
    max_points: int = 2_000,
) -> tuple[Figure, Axes]:
    stats = (
        data[[RentDurationRolling.time, "mean", "ci_low", "ci_high"]]
        .drop_duplicates()
        .sort_values(RentDurationRolling.time, kind="stable")
    )
    stats = downsample_evenly(stats, max_rows=max_points, sort_by=RentDurationRolling.time)

    with chart(figsize) as (fig, ax):
        sns.lineplot(
            data=stats,
            x=RentDurationRolling.time,
            y="mean",
            color="C0",
            lw=2,
            errorbar=None,
            ax=ax,
            label="mean",
        )
        ax.fill_between(
            stats[RentDurationRolling.time].to_numpy(),
            stats["ci_low"].to_numpy(),
            stats["ci_high"].to_numpy(),
            color="C0",
            alpha=0.2,
            linewidth=0,
            label="95% CI (bootstrap)",
        )
        if settings is not None:
            ax.axhline(settings.min_lease_duration, ls="--", color="C2", lw=1, label="min lease")
            ax.axhline(settings.max_lease_duration, ls="--", color="C3", lw=1, label="max lease")
        ax.set_title("Rent Duration (95% CI)")
        ax.set_xlabel("Time")
        ax.set_ylabel("Duration (periods)")
        ax.legend()
        ax.set_ylim(bottom=0)
    return fig, ax
