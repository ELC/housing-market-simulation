import matplotlib.pyplot as plt
import seaborn as sns
from matplotlib.axes import Axes
from matplotlib.figure import Figure
from pandera.typing import DataFrame

from core.analytics.schemas import OccupancyLog, WealthLog


def plot_housed_renter_wealth(
    wealth_df: DataFrame[WealthLog],
    occupancy_df: DataFrame[OccupancyLog],
    figsize: tuple[float, float] = (14, 4),
) -> tuple[Figure, Axes]:
    housed_agents = (
        occupancy_df.query(f"{OccupancyLog.occupant} != 'vacant'")[
            [OccupancyLog.time, OccupancyLog.occupant]
        ].rename(
            columns={
                OccupancyLog.time: WealthLog.time,
                OccupancyLog.occupant: WealthLog.agent,
            }
        )
        .drop_duplicates()
    )

    housed_renter_wealth = wealth_df.query(
        f"{WealthLog.agent} != 'landlord'"
    ).merge(housed_agents, on=[WealthLog.time, WealthLog.agent])

    fig, ax = plt.subplots(figsize=figsize)
    sns.lineplot(
        data=housed_renter_wealth,
        x=WealthLog.time,
        y=WealthLog.money,
        ax=ax,
    )
    ax.set_title("Average Housed-Renter Wealth (95% CI)")
    ax.set_xlabel("Time")
    ax.set_ylabel("Money")
    plt.tight_layout()
    return fig, ax
