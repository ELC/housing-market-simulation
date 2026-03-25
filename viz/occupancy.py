import matplotlib.pyplot as plt
import seaborn as sns
from matplotlib.axes import Axes
from matplotlib.figure import Figure
from pandera.typing import DataFrame

from analytics.gold.schemas import OccupancyTimeline


def plot_occupancy(
    data: DataFrame[OccupancyTimeline],
    figsize: tuple[float, float] = (14, 5),
) -> tuple[Figure, Axes]:
    renter_names: list[str] = sorted(
        data.loc[
            data[OccupancyTimeline.occupant] != "vacant",
            OccupancyTimeline.occupant,
        ].unique()
    )
    palette: dict[str, str] = {"vacant": "#dddddd"}
    palette.update({name: f"C{i}" for i, name in enumerate(renter_names)})

    fig, ax = plt.subplots(figsize=figsize)
    sns.scatterplot(
        data=data,
        x=OccupancyTimeline.time,
        y=OccupancyTimeline.house,
        hue=OccupancyTimeline.occupant,
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
