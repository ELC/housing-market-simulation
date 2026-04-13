import seaborn as sns
from matplotlib.axes import Axes
from matplotlib.figure import Figure
from pandera.typing import DataFrame

from analytics.gold import RentComparison
from viz.base import chart
from viz.fast import downsample_evenly


def plot_paid_vs_asked(
    data: DataFrame[RentComparison],
    figsize: tuple[float, float] = (14, 4),
    max_points: int = 2_000,
) -> tuple[Figure, Axes]:
    required = {"mean", "ci_low", "ci_high"}
    if not required.issubset(set(data.columns)):
        raise ValueError(
            "Missing bootstrap CI columns. Rebuild with analytics.gold.build_rent_comparison() "
            "to attach mean/ci_low/ci_high."
        )
    stats = (
        data[[RentComparison.time, RentComparison.kind, "mean", "ci_low", "ci_high"]]
        .drop_duplicates()
        .sort_values([RentComparison.kind, RentComparison.time], kind="stable")
    )
    stats = downsample_evenly(stats, max_rows=max_points, sort_by=[RentComparison.kind, RentComparison.time])

    kind_colors: dict[str, str] = {"paid": "C0", "asked": "C1"}
    with chart(figsize) as (fig, ax):
        sns.lineplot(
            data=stats,
            x=RentComparison.time,
            y="mean",
            hue=RentComparison.kind,
            palette=kind_colors,
            linewidth=2,
            errorbar=None,
            ax=ax,
        )
        for kind, g in stats.groupby(RentComparison.kind, sort=False):
            color = kind_colors.get(str(kind), None)
            ax.fill_between(
                g[RentComparison.time].to_numpy(),
                g["ci_low"].to_numpy(),
                g["ci_high"].to_numpy(),
                alpha=0.2,
                linewidth=0,
                color=color,
            )
        ax.set_title("Average Paid Rent vs Asked Rent (95% CI)")
        ax.set_xlabel("Time")
        ax.set_ylabel("Rent")
        ax.legend()
    return fig, ax
