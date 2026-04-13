import seaborn as sns
from matplotlib.axes import Axes
from matplotlib.figure import Figure
from pandera.typing import DataFrame

from analytics.gold import HouseRents
from viz.base import chart
from viz.fast import downsample_evenly


def plot_house_rents(
    data: DataFrame[HouseRents],
    figsize: tuple[float, float] = (14, 4),
    max_points_mean: int = 2_000,
) -> tuple[Figure, Axes]:
    df = data.sort_values([HouseRents.house, HouseRents.time], kind="stable")
    required = {"mean", "ci_low", "ci_high"}
    if not required.issubset(set(df.columns)):
        raise ValueError(
            "Missing bootstrap CI columns. Rebuild with analytics.gold.build_house_rents() "
            "to attach mean/ci_low/ci_high."
        )
    stats = (
        df[[HouseRents.time, "mean", "ci_low", "ci_high"]]
        .drop_duplicates()
        .sort_values(HouseRents.time, kind="stable")
    )
    stats = downsample_evenly(stats, max_rows=max_points_mean, sort_by=HouseRents.time)

    with chart(figsize) as (fig, ax):
        sns.lineplot(
            data=stats,
            x=HouseRents.time,
            y="mean",
            color="black",
            linewidth=2,
            errorbar=None,
            ax=ax,
            label="Mean",
        )
        ax.fill_between(
            stats[HouseRents.time].to_numpy(),
            stats["ci_low"].to_numpy(),
            stats["ci_high"].to_numpy(),
            color="black",
            alpha=0.15,
            linewidth=0,
            label="95% CI (bootstrap)",
        )
        ax.set_title("House Asking Rents Over Time")
        ax.set_xlabel("Time")
        ax.set_ylabel("Rent Price")
        ax.legend()
    return fig, ax
