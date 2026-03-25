import matplotlib.pyplot as plt
import seaborn as sns
from matplotlib.axes import Axes
from matplotlib.figure import Figure
from pandera.typing import DataFrame

from core.analytics.schemas import WealthLog


def plot_renter_wealth(
    wealth_df: DataFrame[WealthLog],
    figsize: tuple[float, float] = (14, 5),
) -> tuple[Figure, Axes]:
    renter_wealth = wealth_df.query(f"{WealthLog.agent} != 'landlord'")

    fig, ax = plt.subplots(figsize=figsize)
    sns.lineplot(
        data=renter_wealth,
        x=WealthLog.time,
        y=WealthLog.money,
        hue=WealthLog.agent,
        ax=ax,
    )
    ax.set_title("Renter Wealth Over Time")
    ax.set_xlabel("Time")
    ax.set_ylabel("Money")
    sns.move_legend(ax, "upper left")
    plt.tight_layout()
    return fig, ax
