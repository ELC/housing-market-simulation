import seaborn as sns
from matplotlib.axes import Axes
from matplotlib.figure import Figure
from pandera.typing import DataFrame

from analytics.gold import HouseRents
from viz.base import chart


def plot_house_rents(
    data: DataFrame[HouseRents],
    figsize: tuple[float, float] = (14, 4),
) -> tuple[Figure, Axes]:
    with chart(figsize) as (fig, ax):
        sns.lineplot(
            data=data,
            x=HouseRents.time,
            y=HouseRents.rent,
            hue=HouseRents.house,
            ax=ax,
            alpha=0.5,
            linewidth=1,
            legend=False,
        )
        sns.lineplot(
            data=data,
            x=HouseRents.time,
            y=HouseRents.rent,
            ax=ax,
            color="black",
            linewidth=2,
            label="Mean (95% CI)",
        )
        ax.set_title("House Asking Rents Over Time")
        ax.set_xlabel("Time")
        ax.set_ylabel("Rent Price")
        ax.legend()
    return fig, ax
