import numpy as np
import pandas as pd
from matplotlib.axes import Axes
from matplotlib.colors import ListedColormap
from matplotlib.figure import Figure
from pandera.typing import DataFrame

from analytics.gold import OccupancyTimeline
from viz.base import chart
from viz.fast import evenly_spaced_tick_positions

_SPECIAL_STATES = {"vacant", "construction", "demolished"}
_STATE_ORDER = ["vacant", "occupied", "construction", "demolished"]
_STATE_COLORS: dict[str, str] = {
    "vacant": "#dddddd",
    "occupied": "#4C72B0",
    "construction": "#FFB347",
    "demolished": "#888888",
}


def plot_occupancy(
    data: DataFrame[OccupancyTimeline],
    figsize: tuple[float, float] = (14, 5),
    max_time_points: int = 2_000,
) -> tuple[Figure, Axes]:
    df = data[[OccupancyTimeline.time, OccupancyTimeline.house, OccupancyTimeline.occupant]].copy()
    df["_state"] = np.where(
        df[OccupancyTimeline.occupant].isin(_SPECIAL_STATES),
        df[OccupancyTimeline.occupant],
        "occupied",
    )
    # Pivot to a house x time grid so we can render as an image (much faster
    # than a huge categorical scatter + legend).
    grid = (
        df.pivot(index=OccupancyTimeline.house, columns=OccupancyTimeline.time, values="_state")
        .sort_index(axis=0)
        .sort_index(axis=1)
    )
    times = grid.columns.to_numpy()
    if max_time_points > 0 and len(times) > max_time_points:
        idx = np.linspace(0, len(times) - 1, num=max_time_points, dtype=np.int64)
        idx[0] = 0
        idx[-1] = len(times) - 1
        grid = grid.iloc[:, idx]
        times = grid.columns.to_numpy()

    state_arr = grid.to_numpy()
    codes = np.full(state_arr.shape, fill_value=_STATE_ORDER.index("occupied"), dtype=np.int16)
    # Unknown / missing -> vacant (rare, but makes rendering robust)
    codes[pd.isna(state_arr)] = _STATE_ORDER.index("vacant")
    for state in _SPECIAL_STATES:
        codes[state_arr == state] = _STATE_ORDER.index(state)

    with chart(figsize) as (fig, ax):
        cmap = ListedColormap([_STATE_COLORS[s] for s in _STATE_ORDER])
        ax.imshow(
            codes,
            aspect="auto",
            interpolation="nearest",
            origin="lower",
            cmap=cmap,
            vmin=-0.5,
            vmax=len(_STATE_ORDER) - 0.5,
        )
        ax.set_yticks(range(len(grid.index)))
        ax.set_yticklabels(list(grid.index))

        xticks = evenly_spaced_tick_positions(len(times), max_ticks=8)
        ax.set_xticks(xticks)
        ax.set_xticklabels([f"{float(times[i]):g}" for i in xticks])
        ax.set_title("House Occupancy Timeline")
        ax.set_xlabel("Time")
        ax.set_ylabel("")
    return fig, ax
