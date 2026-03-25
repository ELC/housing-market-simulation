import matplotlib.pyplot as plt
import seaborn as sns
from matplotlib.axes import Axes
from matplotlib.figure import Figure
from pandera.typing import DataFrame

from analytics.gold.schemas import RentComparison


def plot_paid_vs_asked(
    data: DataFrame[RentComparison],
    figsize: tuple[float, float] = (14, 4),
) -> tuple[Figure, Axes]:
    fig, ax = plt.subplots(figsize=figsize)
    sns.lineplot(
        data=data,
        x=RentComparison.time,
        y=RentComparison.amount,
        hue=RentComparison.kind,
        ax=ax,
    )
    ax.set_title("Average Paid Rent vs Asked Rent (95% CI)")
    ax.set_xlabel("Time")
    ax.set_ylabel("Rent")
    ax.legend()
    plt.tight_layout()
    return fig, ax
