import matplotlib.ticker as mticker
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
        sns.lineplot(data=data, x=VacancyCount.time, y=VacancyCount.n_vacant, label="Vacant", ax=ax, lw=2)
        sns.lineplot(
            data=data,
            x=VacancyCount.time,
            y=VacancyCount.n_construction,
            label="Construction",
            color="#FFB347",
            ax=ax,
            lw=2,
        )
        sns.lineplot(
            data=data,
            x=VacancyCount.time,
            y=VacancyCount.n_demolished,
            label="Demolished",
            color="#888888",
            ax=ax,
            lw=2,
        )
        ax.set_title("House Status Over Time")
        ax.set_xlabel("Time")
        ax.set_ylabel("Houses")
        ax.set_ylim(bottom=0)
        ax.yaxis.set_major_locator(mticker.MaxNLocator(integer=True))
        ax.legend()
    return fig, ax
