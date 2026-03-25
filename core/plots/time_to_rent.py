import matplotlib.pyplot as plt
from matplotlib.axes import Axes
from matplotlib.figure import Figure
from pandera.typing import DataFrame

from core.analytics.schemas import TimeToRent


def plot_time_to_rent(
    ttr_df: DataFrame[TimeToRent],
    window: int = 10,
    figsize: tuple[float, float] = (14, 4),
) -> tuple[Figure, Axes]:
    ttr_sorted = ttr_df.sort_values(TimeToRent.time)
    rolling = ttr_sorted[TimeToRent.duration].rolling(window, min_periods=1)
    ttr_plot = ttr_sorted.assign(
        rolling_mean=rolling.mean(),
        rolling_std=rolling.std().fillna(0),
    )

    fig, ax = plt.subplots(figsize=figsize)
    ax.scatter(
        ttr_plot[TimeToRent.time],
        ttr_plot[TimeToRent.duration],
        alpha=0.25,
        s=25,
        color="C0",
        label="individual",
    )
    ax.plot(
        ttr_plot[TimeToRent.time],
        ttr_plot["rolling_mean"],
        color="C1",
        linewidth=2,
        label=f"running mean (w={window})",
    )
    ax.fill_between(
        ttr_plot[TimeToRent.time],
        ttr_plot["rolling_mean"] - 1.96 * ttr_plot["rolling_std"],
        ttr_plot["rolling_mean"] + 1.96 * ttr_plot["rolling_std"],
        alpha=0.2,
        color="C1",
        label="95% CI",
    )
    ax.set_title("Time to Rent a Vacant House")
    ax.set_xlabel("Time")
    ax.set_ylabel("Duration (periods)")
    ax.legend()
    ax.set_ylim(bottom=0)
    plt.tight_layout()
    return fig, ax
