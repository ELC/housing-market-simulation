import matplotlib.colors as mcolors
import matplotlib.ticker as mticker
import seaborn as sns
from matplotlib.axes import Axes
from matplotlib.figure import Figure
from pandera.typing import DataFrame

from analytics.gold.landlord_population.schema import GoldLandlordPopulation
from viz.base import chart, set_xlim_with_padding, set_ylim_with_padding
from viz.fast import prepare_ci_stats


def plot_landlord_population(
    data: DataFrame[GoldLandlordPopulation],
    figsize: tuple[float, float] = (14, 4),
    max_points: int = 2_000,
    max_t: float | None = None,
) -> tuple[Figure, Axes]:
    stats = prepare_ci_stats(data, max_rows=max_points)

    with chart(figsize) as (fig, ax):
        color = mcolors.to_hex("C1")
        sns.lineplot(data=stats, x="time", y="mean", color=color, lw=2, errorbar=None, ax=ax, label="mean")
        ax.fill_between(
            stats["time"].to_numpy(), stats["ci_low"].to_numpy(), stats["ci_high"].to_numpy(),
            color=color, alpha=0.2, linewidth=0, label="95% CI",
        )
        ax.set_title("Number of Landlords Over Time (95% CI)")
        ax.set_xlabel("Time")
        ax.set_ylabel("Number of Landlords")
        ax.yaxis.set_major_locator(mticker.MaxNLocator(integer=True))
        ax.legend()
        set_ylim_with_padding(ax)
        set_xlim_with_padding(ax, right=max_t)
    return fig, ax
