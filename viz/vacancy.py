import matplotlib.pyplot as plt
import seaborn as sns
from matplotlib.axes import Axes
from matplotlib.figure import Figure
from pandera.typing import DataFrame

from analytics.gold.schemas import VacancyCount


def plot_vacancy(
    data: DataFrame[VacancyCount],
    figsize: tuple[float, float] = (14, 4),
) -> tuple[Figure, Axes]:
    fig, ax = plt.subplots(figsize=figsize)
    sns.lineplot(data=data, x=VacancyCount.time, y=VacancyCount.n_vacant, ax=ax)
    ax.set_title("Total Vacant Houses Over Time")
    ax.set_xlabel("Time")
    ax.set_ylabel("Vacant Houses")
    ax.set_ylim(bottom=0)
    plt.tight_layout()
    return fig, ax
