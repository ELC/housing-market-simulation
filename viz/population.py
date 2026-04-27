# pyright: reportUnknownMemberType=false

import matplotlib.ticker as mticker
import seaborn as sns
from matplotlib.axes import Axes
from matplotlib.figure import Figure
from pandera.typing import DataFrame

from analytics.gold.agent_population.schema import AgentPopulation
from viz.base import chart, set_xlim_with_padding
from viz.fast import prepare_ci_stats


def plot_population(
    data: DataFrame[AgentPopulation],
    figsize: tuple[float, float] = (14, 4),
    max_points: int = 2_000,
    max_t: float | None = None,
) -> tuple[Figure, Axes]:
    stats = prepare_ci_stats(data, max_rows=max_points, time_col=AgentPopulation.time)

    with chart(figsize) as (fig, ax):
        sns.lineplot(
            data=stats,
            x=AgentPopulation.time,
            y="mean",
            color="C0",
            lw=2,
            errorbar=None,
            ax=ax,
            label="mean",
        )
        ax.fill_between(
            stats[AgentPopulation.time].to_numpy(),
            stats["ci_low"].to_numpy(),
            stats["ci_high"].to_numpy(),
            color="C0",
            alpha=0.2,
            linewidth=0,
            label="95% CI (bootstrap)",
        )
        ax.set_title("Agent Population Over Time (95% CI)")
        ax.set_xlabel("Time")
        ax.set_ylabel("Number of Agents")
        ax.yaxis.set_major_locator(mticker.MaxNLocator(integer=True))
        ax.legend()
        set_xlim_with_padding(ax, right=max_t)
    return fig, ax
