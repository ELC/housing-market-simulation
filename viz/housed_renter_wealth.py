import seaborn as sns
from matplotlib.axes import Axes
from matplotlib.figure import Figure
from pandera.typing import DataFrame

from analytics.gold import HousedRenterWealth
from viz.base import chart, mark_first_rent, set_xlim_with_padding, set_ylim_with_padding
from viz.fast import prepare_ci_stats


def plot_housed_renter_wealth(
    data: DataFrame[HousedRenterWealth],
    figsize: tuple[float, float] = (14, 4),
    max_points: int = 2_000,
    max_t: float | None = None,
    first_rent_time: float | None = None,
) -> tuple[Figure, Axes]:
    required = {"mean", "ci_low", "ci_high"}
    if not required.issubset(set(data.columns)):
        raise ValueError(
            "Missing bootstrap CI columns. Rebuild with analytics.gold.build_housed_renter_wealth() "
            "to attach mean/ci_low/ci_high."
        )
    stats = prepare_ci_stats(data, max_rows=max_points, time_col=HousedRenterWealth.time)

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
        mark_first_rent(ax, first_rent_time)
        ax.legend()
        set_ylim_with_padding(ax)
        set_xlim_with_padding(ax, right=max_t)
    return fig, ax
