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
    with chart(figsize) as (fig, ax):
        sns.lineplot(
            data=data,
            x=AgentPopulation.time,
            y=AgentPopulation.count,
            color="C0",
            ax=ax,
        )
        ax.set_title("Agent Population Over Time")
        ax.set_xlabel("Time")
        ax.set_ylabel("Number of Agents")
        ax.yaxis.get_major_locator().set_params(integer=True)
    return fig, ax


def plot_migration_flows(
    data: DataFrame[MigrationFlows],
    figsize: tuple[float, float] = (14, 4),
) -> tuple[Figure, Axes]:
    rounded = data.assign(**{MigrationFlows.time: data[MigrationFlows.time].round(2)})
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
        ax.yaxis.get_major_locator().set_params(integer=True)
    return fig, ax
