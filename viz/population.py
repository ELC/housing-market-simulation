from matplotlib.axes import Axes
from matplotlib.figure import Figure
from pandera.typing import DataFrame

from analytics.gold.agent_population.schema import AgentPopulation
from viz.base import chart


def plot_population(
    data: DataFrame[AgentPopulation],
    figsize: tuple[float, float] = (14, 4),
) -> tuple[Figure, Axes]:
    with chart(figsize) as (fig, ax):
        ax.plot(data[AgentPopulation.time], data[AgentPopulation.count], color="C0", linewidth=1.5)
        ax.set_title("Agent Population Over Time")
        ax.set_xlabel("Time")
        ax.set_ylabel("Number of Agents")
        ax.yaxis.get_major_locator().set_params(integer=True)
    return fig, ax


def plot_migration_flows(
    data: DataFrame[AgentPopulation],
    figsize: tuple[float, float] = (14, 4),
) -> tuple[Figure, Axes]:
    with chart(figsize) as (fig, ax):
        ax.bar(
            data[AgentPopulation.time],
            data["entered"],
            width=1.0,
            color="C2",
            alpha=0.7,
            label="entered",
        )
        ax.bar(
            data[AgentPopulation.time],
            -data["left"],
            width=1.0,
            color="C3",
            alpha=0.7,
            label="left",
        )
        ax.axhline(0, ls="-", color="grey", lw=0.5)
        ax.set_title("Migration Flows")
        ax.set_xlabel("Time")
        ax.set_ylabel("Agents per Period")
        ax.legend()
        ax.yaxis.get_major_locator().set_params(integer=True)
    return fig, ax
