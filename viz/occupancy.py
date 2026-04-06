import seaborn as sns
from matplotlib.axes import Axes
from matplotlib.figure import Figure
from pandera.typing import DataFrame

from analytics.gold import OccupancyTimeline
from viz.base import chart


def plot_occupancy(
    data: DataFrame[OccupancyTimeline],
    figsize: tuple[float, float] = (14, 5),
) -> tuple[Figure, Axes]:
    special_states = {"vacant", "construction", "demolished"}
    renter_names: list[str] = sorted(
        data.loc[
            ~data[OccupancyTimeline.occupant].isin(special_states),
            OccupancyTimeline.occupant,
        ].unique()
    )
    palette: dict[str, str] = {
        "vacant": "#dddddd",
        "construction": "#FFB347",
        "demolished": "#888888",
    }
    palette.update({name: f"C{i}" for i, name in enumerate(renter_names)})

    with chart(figsize) as (fig, ax):
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
    return fig, ax
