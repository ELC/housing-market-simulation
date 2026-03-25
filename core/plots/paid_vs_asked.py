import matplotlib.pyplot as plt
import seaborn as sns
from matplotlib.axes import Axes
from matplotlib.figure import Figure
from pandera.typing import DataFrame

from core.analytics.schemas import HouseRentLog, RentLog


def plot_paid_vs_asked(
    rent_df: DataFrame[RentLog],
    asking_rent_df: DataFrame[HouseRentLog],
    figsize: tuple[float, float] = (14, 4),
) -> tuple[Figure, Axes]:
    fig, ax = plt.subplots(figsize=figsize)
    sns.lineplot(
        data=rent_df, x=RentLog.time, y=RentLog.amount, ax=ax, label="Paid Rent"
    )
    sns.lineplot(
        data=asking_rent_df,
        x=HouseRentLog.time,
        y=HouseRentLog.rent,
        ax=ax,
        label="Asked Rent",
    )
    ax.set_title("Average Paid Rent vs Asked Rent (95% CI)")
    ax.set_xlabel("Time")
    ax.set_ylabel("Rent")
    ax.legend()
    plt.tight_layout()
    return fig, ax
