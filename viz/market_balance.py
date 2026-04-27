import matplotlib.colors as mcolors
import matplotlib.ticker as mticker
import seaborn as sns
from matplotlib.axes import Axes
from matplotlib.figure import Figure
from pandera.typing import DataFrame

from analytics.gold.market_balance.schema import GoldMarketBalance
from viz.base import chart, mark_first_rent, set_xlim_with_padding
from viz.fast import prepare_ci_stats


def plot_market_balance(
    data: DataFrame[GoldMarketBalance],
    figsize: tuple[float, float] = (14, 4),
    max_points: int = 2_000,
    max_t: float | None = None,
    first_rent_time: float | None = None,
) -> tuple[Figure, Axes]:
    with chart(figsize) as (fig, ax):
        for metric, cycle_color, label in [
            ("vacancy", "C0", "Vacancy Rate"),
            ("homelessness", "C3", "Homelessness Rate"),
        ]:
            subset = prepare_ci_stats(data[data["metric"] == metric], max_rows=max_points)
            if subset.empty:
                continue
            color = mcolors.to_hex(cycle_color)
            sns.lineplot(data=subset, x="time", y="mean", color=color, lw=2, errorbar=None, ax=ax, label=label)
            ax.fill_between(
                subset["time"].to_numpy(), subset["ci_low"].to_numpy(), subset["ci_high"].to_numpy(),
                color=color, alpha=0.2, linewidth=0,
            )
        ax.set_title("Market Balance: Vacancy & Homelessness Rates (95% CI)")
        ax.set_xlabel("Time")
        ax.set_ylabel("Rate")
        ax.yaxis.set_major_formatter(mticker.PercentFormatter(xmax=1.0))
        mark_first_rent(ax, first_rent_time)
        ax.legend()
        set_xlim_with_padding(ax, right=max_t)
    return fig, ax
