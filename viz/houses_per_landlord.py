import matplotlib.colors as mcolors
import seaborn as sns
from matplotlib.axes import Axes
from matplotlib.figure import Figure
from pandera.typing import DataFrame

from analytics.gold.houses_per_landlord.schema import GoldHousesPerLandlord
from viz.base import chart, set_xlim_with_padding, set_ylim_with_padding
from viz.fast import prepare_ci_stats


def plot_houses_per_landlord(
    data: DataFrame[GoldHousesPerLandlord],
    figsize: tuple[float, float] = (14, 4),
    max_points: int = 2_000,
    max_t: float | None = None,
) -> tuple[Figure, Axes]:
    stats = prepare_ci_stats(data, max_rows=max_points)

    with chart(figsize) as (fig, ax):
        color = mcolors.to_hex("C2")
        sns.lineplot(data=stats, x="time", y="mean", color=color, lw=2, errorbar=None, ax=ax, label="mean")
        ax.fill_between(
            stats["time"].to_numpy(), stats["ci_low"].to_numpy(), stats["ci_high"].to_numpy(),
            color=color, alpha=0.2, linewidth=0, label="95% CI",
        )
        ax.set_title("Houses per Landlord Over Time (95% CI)")
        ax.set_xlabel("Time")
        ax.set_ylabel("Houses per Landlord")
        ax.legend()
        set_ylim_with_padding(ax)
        set_xlim_with_padding(ax, right=max_t)
    return fig, ax
