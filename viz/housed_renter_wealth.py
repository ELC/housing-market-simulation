import matplotlib.pyplot as plt
import seaborn as sns
from matplotlib.axes import Axes
from matplotlib.figure import Figure
from pandera.typing import DataFrame

from analytics.gold.schemas import HousedRenterWealth


def plot_housed_renter_wealth(
    data: DataFrame[HousedRenterWealth],
    figsize: tuple[float, float] = (14, 4),
) -> tuple[Figure, Axes]:
    fig, ax = plt.subplots(figsize=figsize)
    sns.lineplot(
        data=data,
        x=HousedRenterWealth.time,
        y=HousedRenterWealth.money,
        ax=ax,
    )
    ax.set_title("Average Housed-Renter Wealth (95% CI)")
    ax.set_xlabel("Time")
    ax.set_ylabel("Money")
    plt.tight_layout()
    return fig, ax
