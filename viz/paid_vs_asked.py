import seaborn as sns
from matplotlib.axes import Axes
from matplotlib.figure import Figure
from pandera.typing import DataFrame

from analytics.gold import RentComparison
from viz.base import chart, mark_first_rent, set_xlim_with_padding, set_ylim_with_padding
from viz.fast import prepare_ci_stats


def plot_paid_vs_asked(
    data: DataFrame[RentComparison],
    figsize: tuple[float, float] = (14, 4),
    max_points: int = 2_000,
    max_t: float | None = None,
    first_rent_time: float | None = None,
) -> tuple[Figure, Axes]:
    required = {"mean", "ci_low", "ci_high"}
    if not required.issubset(set(data.columns)):
        raise ValueError(
            "Missing bootstrap CI columns. Rebuild with analytics.gold.build_rent_comparison() "
            "to attach mean/ci_low/ci_high."
        )
    stats = prepare_ci_stats(
        data,
        max_rows=max_points,
        time_col=RentComparison.time,
        group_cols=[RentComparison.kind],
    )

    kind_colors: dict[str, str] = {"paid": "C0", "asked": "C1"}
    with chart(figsize) as (fig, ax):
        sns.lineplot(
            data=stats,
            x=RentComparison.time,
            y="mean",
            hue=RentComparison.kind,
            palette=kind_colors,
            linewidth=2,
            errorbar=None,
            ax=ax,
        )
        for kind, g in stats.groupby(RentComparison.kind, sort=False):
            color = kind_colors.get(str(kind), None)
            ax.fill_between(
                g[RentComparison.time].to_numpy(),
                g["ci_low"].to_numpy(),
                g["ci_high"].to_numpy(),
                alpha=0.2,
                linewidth=0,
                color=color,
            )
        ax.set_title("Average Paid Rent vs Asked Rent (95% CI)")
        ax.set_xlabel("Time")
        ax.set_ylabel("Rent")
        mark_first_rent(ax, first_rent_time)
        ax.legend()
        set_ylim_with_padding(ax)
        set_xlim_with_padding(ax, right=max_t)
    return fig, ax
