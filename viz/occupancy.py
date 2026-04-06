import seaborn as sns
from matplotlib.axes import Axes
from matplotlib.figure import Figure
from pandera.typing import DataFrame

from analytics.gold import OccupancyTimeline
from viz.base import chart

_SPECIAL_STATES = {"vacant", "construction", "demolished"}
_SPECIAL_MARKERS: dict[str, str] = {
    "vacant": "s",
    "construction": "P",
    "demolished": "X",
}


def plot_occupancy(
    data: DataFrame[OccupancyTimeline],
    figsize: tuple[float, float] = (14, 5),
) -> tuple[Figure, Axes]:
    renter_names: list[str] = sorted(
        data.loc[
            ~data[OccupancyTimeline.occupant].isin(_SPECIAL_STATES),
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
        regular = data[~data[OccupancyTimeline.occupant].isin(_SPECIAL_STATES - {"vacant"})]
        sns.scatterplot(
            data=regular,
            x=OccupancyTimeline.time,
            y=OccupancyTimeline.house,
            hue=OccupancyTimeline.occupant,
            palette=palette,
            s=160,
            marker="s",
            edgecolor="none",
            ax=ax,
        )
        for state in ("construction", "demolished"):
            subset = data[data[OccupancyTimeline.occupant] == state]
            if subset.empty:
                continue
            ax.scatter(
                subset[OccupancyTimeline.time],
                subset[OccupancyTimeline.house],
                color=palette[state],
                s=160,
                marker=_SPECIAL_MARKERS[state],
                edgecolor="none",
                label=state,
                zorder=3,
            )
        ax.set_title("House Occupancy Timeline")
        ax.set_xlabel("Time")
        ax.set_ylabel("")
        ax.legend(loc="center left", bbox_to_anchor=(1, 0.5))
    return fig, ax
