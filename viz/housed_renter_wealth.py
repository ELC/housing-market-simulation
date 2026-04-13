import seaborn as sns
from matplotlib.axes import Axes
from matplotlib.figure import Figure
from pandera.typing import DataFrame

from analytics.gold import HousedRenterWealth
from viz.base import chart
from viz.fast import downsample_evenly


def plot_housed_renter_wealth(
    data: DataFrame[HousedRenterWealth],
    figsize: tuple[float, float] = (14, 4),
    max_points: int = 2_000,
) -> tuple[Figure, Axes]:
    required = {"mean", "ci_low", "ci_high"}
    if not required.issubset(set(data.columns)):
        raise ValueError(
            "Missing bootstrap CI columns. Rebuild with analytics.gold.build_housed_renter_wealth() "
            "to attach mean/ci_low/ci_high."
        )
    stats = (
        data[[HousedRenterWealth.time, "mean", "ci_low", "ci_high"]]
        .drop_duplicates()
        .sort_values(HousedRenterWealth.time, kind="stable")
    )
    stats = downsample_evenly(stats, max_rows=max_points, sort_by=HousedRenterWealth.time)

    with chart(figsize) as (fig, ax):
        sns.lineplot(
            data=stats,
            x=HousedRenterWealth.time,
            y="mean",
            color="C0",
            linewidth=2,
            errorbar=None,
            ax=ax,
            label="mean",
        )
        ax.fill_between(
            stats[HousedRenterWealth.time].to_numpy(),
            stats["ci_low"].to_numpy(),
            stats["ci_high"].to_numpy(),
            color="C0",
            alpha=0.2,
            linewidth=0,
            label="95% CI (bootstrap)",
        )
        ax.set_title("Average Housed-Renter Wealth (95% CI)")
        ax.set_xlabel("Time")
        ax.set_ylabel("Money")
        ax.legend()
    return fig, ax
