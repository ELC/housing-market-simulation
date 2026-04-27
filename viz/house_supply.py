import matplotlib.ticker as mticker
import seaborn as sns
from matplotlib.axes import Axes
from matplotlib.figure import Figure
from pandera.typing import DataFrame

from analytics.gold.house_supply.schema import GoldHouseSupply
from viz.base import chart, set_xlim_with_padding, set_ylim_with_padding
from viz.fast import prepare_ci_stats


def plot_house_supply(
    data: DataFrame[GoldHouseSupply],
    figsize: tuple[float, float] = (14, 4),
    max_points: int = 2_000,
    max_t: float | None = None,
) -> tuple[Figure, Axes]:
    stats = prepare_ci_stats(data, max_rows=max_points)

    with chart(figsize) as (fig, ax):
        sns.lineplot(data=stats, x="time", y="mean", color="C0", lw=2, errorbar=None, ax=ax, label="mean")
        ax.fill_between(
            stats["time"].to_numpy(), stats["ci_low"].to_numpy(), stats["ci_high"].to_numpy(),
            color="C0", alpha=0.2, linewidth=0, label="95% CI",
        )
        ax.set_title("Number of Houses Over Time (95% CI)")
        ax.set_xlabel("Time")
        ax.set_ylabel("Number of Houses")
        ax.yaxis.set_major_locator(mticker.MaxNLocator(integer=True))
        ax.legend()
        set_ylim_with_padding(ax)
        set_xlim_with_padding(ax, right=max_t)
    return fig, ax
