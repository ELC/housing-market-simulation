import seaborn as sns
from matplotlib.axes import Axes
from matplotlib.figure import Figure
from pandera.typing import DataFrame

from analytics.gold import RentPayments
from viz.base import chart
from viz.fast import downsample_evenly


def plot_avg_rent(
    data: DataFrame[RentPayments],
    figsize: tuple[float, float] = (14, 4),
    max_points: int = 2_000,
) -> tuple[Figure, Axes]:
    required = {"mean", "ci_low", "ci_high"}
    if not required.issubset(set(data.columns)):
        raise ValueError(
            "Missing bootstrap CI columns. Rebuild with analytics.gold.build_rent_payments() "
            "to attach mean/ci_low/ci_high."
        )
    stats = (
        data[[RentPayments.time, "mean", "ci_low", "ci_high"]]
        .drop_duplicates()
        .sort_values(RentPayments.time, kind="stable")
    )
    stats = downsample_evenly(stats, max_rows=max_points, sort_by=RentPayments.time)

    with chart(figsize) as (fig, ax):
        sns.lineplot(
            data=stats,
            x=RentPayments.time,
            y="mean",
            color="C0",
            linewidth=2,
            errorbar=None,
            ax=ax,
            label="mean",
        )
        ax.fill_between(
            stats[RentPayments.time].to_numpy(),
            stats["ci_low"].to_numpy(),
            stats["ci_high"].to_numpy(),
            color="C0",
            alpha=0.2,
            linewidth=0,
            label="95% CI (bootstrap)",
        )
        ax.set_title("Average Rent Payment (95% CI)")
        ax.set_xlabel("Time")
        ax.set_ylabel("Amount")
        ax.legend()
    return fig, ax
