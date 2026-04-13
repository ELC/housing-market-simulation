import seaborn as sns
from matplotlib.axes import Axes
from matplotlib.figure import Figure
from pandera.typing import DataFrame

from analytics.gold import RentDurationRolling
from core.settings import SimulationSettings
from viz.base import chart
from viz.fast import downsample_evenly


def plot_rent_duration(
    data: DataFrame[RentDurationRolling],
    settings: SimulationSettings | None = None,
    figsize: tuple[float, float] = (14, 4),
    max_scatter_points: int = 8_000,
) -> tuple[Figure, Axes]:
    df = data.sort_values(RentDurationRolling.time, kind="stable")
    scatter_df = downsample_evenly(df, max_rows=max_scatter_points, sort_by=None)

    with chart(figsize) as (fig, ax):
        ax.scatter(
            scatter_df[RentDurationRolling.time],
            scatter_df[RentDurationRolling.duration],
            alpha=0.25,
            s=25,
            color="C0",
            label="individual",
            rasterized=True,
        )
        required = {"rolling_ci_low", "rolling_ci_high"}
        if not required.issubset(set(df.columns)):
            raise ValueError(
                "Missing rolling bootstrap CI columns. Rebuild with analytics.gold.build_rent_duration_rolling() "
                "to attach rolling_ci_low/rolling_ci_high."
            )

        sns.lineplot(
            data=df,
            x=RentDurationRolling.time,
            y=RentDurationRolling.rolling_mean,
            color="C1",
            lw=2,
            errorbar=None,
            sort=False,
            ax=ax,
            label="rolling mean",
        )
        ax.fill_between(
            df[RentDurationRolling.time].to_numpy(),
            df["rolling_ci_low"].to_numpy(),
            df["rolling_ci_high"].to_numpy(),
            color="C1",
            alpha=0.15,
            linewidth=0,
            label="95% CI (bootstrap, rolling)",
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
