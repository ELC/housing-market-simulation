import matplotlib.pyplot as plt
import seaborn as sns
from matplotlib.axes import Axes
from matplotlib.figure import Figure
from pandera.typing import DataFrame

from core.analytics.schemas import OccupancyLog


def plot_occupancy(
    occupancy_df: DataFrame[OccupancyLog],
    figsize: tuple[float, float] = (14, 5),
) -> tuple[Figure, Axes]:
    renter_names: list[str] = sorted(
        occupancy_df.loc[
            occupancy_df[OccupancyLog.occupant] != "vacant", OccupancyLog.occupant
        ].unique()
    )
    palette: dict[str, str] = {"vacant": "#dddddd"}
    palette.update({name: f"C{i}" for i, name in enumerate(renter_names)})

    fig, ax = plt.subplots(figsize=figsize)
    sns.scatterplot(
        data=occupancy_df,
        x=OccupancyLog.time,
        y=OccupancyLog.house,
        hue=OccupancyLog.occupant,
        palette=palette,
        s=160,
        marker="s",
        edgecolor="none",
        ax=ax,
    )
    ax.set_title("House Occupancy Timeline")
    ax.set_xlabel("Time")
    ax.set_ylabel("")
    sns.move_legend(ax, "center left", bbox_to_anchor=(1, 0.5))
    plt.tight_layout()
    return fig, ax
