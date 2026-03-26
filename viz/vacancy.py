import seaborn as sns
from matplotlib.axes import Axes
from matplotlib.figure import Figure
from pandera.typing import DataFrame

from analytics.gold import VacancyCount
from viz.base import chart


def plot_vacancy(
    data: DataFrame[VacancyCount],
    figsize: tuple[float, float] = (14, 4),
) -> tuple[Figure, Axes]:
    with chart(figsize) as (fig, ax):
        sns.lineplot(data=data, x=VacancyCount.time, y=VacancyCount.n_vacant, ax=ax)
        ax.set_title("Total Vacant Houses Over Time")
        ax.set_xlabel("Time")
        ax.set_ylabel("Vacant Houses")
        ax.set_ylim(bottom=0)
    return fig, ax
