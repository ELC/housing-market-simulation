import matplotlib.pyplot as plt
import seaborn as sns
from matplotlib.axes import Axes
from matplotlib.figure import Figure
from pandera.typing import DataFrame

from analytics.gold.schemas import RentPayments


def plot_avg_rent(
    data: DataFrame[RentPayments],
    figsize: tuple[float, float] = (14, 4),
) -> tuple[Figure, Axes]:
    fig, ax = plt.subplots(figsize=figsize)
    sns.lineplot(data=data, x=RentPayments.time, y=RentPayments.amount, ax=ax)
    ax.set_title("Average Rent Payment (95% CI)")
    ax.set_xlabel("Time")
    ax.set_ylabel("Amount")
    plt.tight_layout()
    return fig, ax
