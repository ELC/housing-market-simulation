import seaborn as sns
from matplotlib.axes import Axes
from matplotlib.figure import Figure
from pandera.typing import DataFrame

from analytics.gold.tenure.schema import GoldTenure
from viz.base import chart, mark_first_rent, set_xlim_with_padding, set_ylim_with_padding
from viz.fast import prepare_ci_stats


def plot_tenure(
    data: DataFrame[GoldTenure],
    figsize: tuple[float, float] = (14, 4),
    max_points: int = 2_000,
    max_t: float | None = None,
    first_rent_time: float | None = None,
) -> tuple[Figure, Axes]:
    with chart(figsize) as (fig, ax):
        for kind, color, label in [("agent", "C0", "Renters"), ("landlord", "C1", "Landlords")]:
            subset = prepare_ci_stats(data[data["kind"] == kind], max_rows=max_points)
            if subset.empty:
                continue
            sns.lineplot(data=subset, x="time", y="mean", color=color, lw=2, errorbar=None, ax=ax, label=label)
            ax.fill_between(
                subset["time"].to_numpy(), subset["ci_low"].to_numpy(), subset["ci_high"].to_numpy(),
                color=color, alpha=0.2, linewidth=0,
            )
        ax.set_title("Average Tenure in System (95% CI)")
        ax.set_xlabel("Time")
        ax.set_ylabel("Duration")
        mark_first_rent(ax, first_rent_time)
        ax.legend()
        set_ylim_with_padding(ax)
        set_xlim_with_padding(ax, right=max_t)
    return fig, ax
