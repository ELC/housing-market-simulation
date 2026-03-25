import matplotlib.pyplot as plt
import seaborn as sns
from matplotlib.axes import Axes
from matplotlib.figure import Figure
from pandera.typing import DataFrame

from analytics.gold.schemas import RenterWealth


def plot_renter_wealth(
    data: DataFrame[RenterWealth],
    figsize: tuple[float, float] = (14, 5),
) -> tuple[Figure, Axes]:
    fig, ax = plt.subplots(figsize=figsize)
    sns.lineplot(
        data=data,
        x=RenterWealth.time,
        y=RenterWealth.money,
        hue=RenterWealth.agent,
        ax=ax,
    )
    ax.set_title("Renter Wealth Over Time")
    ax.set_xlabel("Time")
    ax.set_ylabel("Money")
    sns.move_legend(ax, "upper left")
    plt.tight_layout()
    return fig, ax
