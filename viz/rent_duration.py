import seaborn as sns
from matplotlib.axes import Axes
from matplotlib.figure import Figure
from pandera.typing import DataFrame

from analytics.gold import RentDurationRolling
from core.settings import SimulationSettings
from viz.base import chart


def plot_rent_duration(
    data: DataFrame[RentDurationRolling],
    settings: SimulationSettings | None = None,
    figsize: tuple[float, float] = (14, 4),
) -> tuple[Figure, Axes]:
    with chart(figsize) as (fig, ax):
        ax.scatter(
            data[RentDurationRolling.time],
            data[RentDurationRolling.duration],
            alpha=0.25,
            s=25,
            color="C0",
            label="individual",
        )
        sns.lineplot(
            data=data,
            x=RentDurationRolling.time,
            y=RentDurationRolling.duration,
            color="C1",
            ax=ax,
        )
        if settings is not None:
            ax.axhline(settings.min_lease_duration, ls="--", color="C2", lw=1, label="min lease")
            ax.axhline(settings.max_lease_duration, ls="--", color="C3", lw=1, label="max lease")
        ax.set_title("Rent Duration (Time Until Vacancy)")
        ax.set_xlabel("Time")
        ax.set_ylabel("Duration (periods)")
        ax.legend()
        ax.set_ylim(bottom=0)
    return fig, ax
