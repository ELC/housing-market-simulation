import seaborn as sns
from matplotlib.axes import Axes
from matplotlib.figure import Figure
from pandera.typing import DataFrame

from analytics.gold import TimeToRentRolling
from viz.base import chart
from viz.fast import downsample_evenly


def plot_time_to_rent(
    data: DataFrame[TimeToRentRolling],
    figsize: tuple[float, float] = (14, 4),
    max_scatter_points: int = 8_000,
) -> tuple[Figure, Axes]:
    df = data.sort_values(TimeToRentRolling.time, kind="stable")
    scatter_df = downsample_evenly(
        df,
        max_rows=max_scatter_points,
        sort_by=None,
    )

    with chart(figsize) as (fig, ax):
        ax.scatter(
            scatter_df[TimeToRentRolling.time],
            scatter_df[TimeToRentRolling.duration],
            alpha=0.25,
            s=25,
            color="C0",
            label="individual",
            rasterized=True,
        )
        required = {"rolling_ci_low", "rolling_ci_high"}
        if not required.issubset(set(df.columns)):
            raise ValueError(
                "Missing rolling bootstrap CI columns. Rebuild with analytics.gold.build_time_to_rent_rolling() "
                "to attach rolling_ci_low/rolling_ci_high."
            )

        sns.lineplot(
            data=df,
            x=TimeToRentRolling.time,
            y=TimeToRentRolling.rolling_mean,
            color="C1",
            lw=2,
            errorbar=None,
            sort=False,
            ax=ax,
            label="rolling mean",
        )
        ax.fill_between(
            df[TimeToRentRolling.time].to_numpy(),
            df["rolling_ci_low"].to_numpy(),
            df["rolling_ci_high"].to_numpy(),
            color="C1",
            alpha=0.15,
            linewidth=0,
            label="95% CI (bootstrap, rolling)",
        )
        ax.set_title("Time to Rent a Vacant House")
        ax.set_xlabel("Time")
        ax.set_ylabel("Duration (periods)")
        ax.legend()
        ax.set_ylim(bottom=0)
    return fig, ax
