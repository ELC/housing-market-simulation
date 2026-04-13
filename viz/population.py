# pyright: reportUnknownMemberType=false

import matplotlib.ticker as mticker
import seaborn as sns
from matplotlib.axes import Axes
from matplotlib.figure import Figure
from pandera.typing import DataFrame

from analytics.gold.agent_population.schema import AgentPopulation
from analytics.gold.migration_flows.schema import MigrationFlows
from viz.base import chart


def plot_population(
    data: DataFrame[AgentPopulation],
    figsize: tuple[float, float] = (14, 4),
) -> tuple[Figure, Axes]:
    ycol = AgentPopulation.count_smooth if AgentPopulation.count_smooth in data.columns else AgentPopulation.count
    with chart(figsize) as (fig, ax):
        sns.lineplot(
            data=data,
            x=AgentPopulation.time,
            y=ycol,
            color="C0",
            lw=2,
            ax=ax,
        )
        ax.set_title("Agent Population Over Time")
        ax.set_xlabel("Time")
        ax.set_ylabel("Number of Agents")
        ax.yaxis.set_major_locator(mticker.MaxNLocator(integer=True))
    return fig, ax


def plot_migration_flows(
    data: DataFrame[MigrationFlows],
    figsize: tuple[float, float] = (14, 4),
    max_bars: int = 120,
) -> tuple[Figure, Axes]:
    rounded = data.copy()
    rounded[MigrationFlows.time] = rounded[MigrationFlows.time].round(2)
    # Collapse duplicates introduced by rounding and drop the many zero-flow times.
    rounded = (
        rounded.groupby([MigrationFlows.time, MigrationFlows.direction], sort=True, observed=True)[MigrationFlows.agents]
        .sum()
        .reset_index()
    )
    rounded = rounded[rounded[MigrationFlows.agents] > 0]

    # If we still have too many distinct times, keep only the largest flows to stay readable.
    if rounded[MigrationFlows.time].nunique() > max_bars:
        by_time = rounded.groupby(MigrationFlows.time, sort=True, observed=True)[MigrationFlows.agents].sum()
        keep_times = set(by_time.sort_values(ascending=False).head(max_bars).index.to_list())
        rounded = rounded[rounded[MigrationFlows.time].isin(keep_times)].sort_values(MigrationFlows.time, kind="stable")

    with chart(figsize) as (fig, ax):
        sns.barplot(
            data=rounded,
            x=MigrationFlows.time,
            y=MigrationFlows.agents,
            hue=MigrationFlows.direction,
            palette={"entered": "C2", "left": "C3"},
            ax=ax,
        )
        ax.set_title("Migration Flows")
        ax.set_xlabel("Time")
        ax.set_ylabel("Agents per Period")
        ax.yaxis.set_major_locator(mticker.MaxNLocator(integer=True))
        ax.tick_params(axis="x", labelrotation=45)
        ax.legend()
    return fig, ax
