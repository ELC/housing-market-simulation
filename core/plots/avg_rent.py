import matplotlib.pyplot as plt
import seaborn as sns
from matplotlib.axes import Axes
from matplotlib.figure import Figure
from pandera.typing import DataFrame

from core.analytics.schemas import RentLog


def plot_avg_rent(
    rent_df: DataFrame[RentLog],
    figsize: tuple[float, float] = (14, 4),
) -> tuple[Figure, Axes]:
    fig, ax = plt.subplots(figsize=figsize)
    sns.lineplot(data=rent_df, x=RentLog.time, y=RentLog.amount, ax=ax)
    ax.set_title("Average Rent Payment (95% CI)")
    ax.set_xlabel("Time")
    ax.set_ylabel("Amount")
    plt.tight_layout()
    return fig, ax
