import seaborn as sns
from matplotlib.axes import Axes
from matplotlib.figure import Figure
from pandera.typing import DataFrame

from analytics.gold.rent_to_income.schema import GoldRentToIncome
from viz.base import chart, mark_first_rent, set_xlim_with_padding, set_ylim_with_padding
from viz.fast import prepare_ci_stats


def plot_rent_to_income(
    data: DataFrame[GoldRentToIncome],
    figsize: tuple[float, float] = (14, 4),
    max_points: int = 2_000,
    max_t: float | None = None,
    first_rent_time: float | None = None,
) -> tuple[Figure, Axes]:
    stats = prepare_ci_stats(data, max_rows=max_points)

    with chart(figsize) as (fig, ax):
        sns.lineplot(data=stats, x="time", y="mean", color="C5", lw=2, errorbar=None, ax=ax, label="mean")
        ax.fill_between(
            stats["time"].to_numpy(), stats["ci_low"].to_numpy(), stats["ci_high"].to_numpy(),
            color="C5", alpha=0.2, linewidth=0, label="95% CI",
        )
        ax.axhline(y=0.30, ls="--", color="red", alpha=0.7, label="30% threshold")
        ax.set_title("Rent-to-Income Ratio Over Time (95% CI)")
        ax.set_xlabel("Time")
        ax.set_ylabel("Rent / Gross Income")
        mark_first_rent(ax, first_rent_time)
        ax.legend()
        set_ylim_with_padding(ax, bottom=0.0, top=1.0)
        set_xlim_with_padding(ax, right=max_t)
    return fig, ax
