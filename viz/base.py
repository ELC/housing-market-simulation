from collections.abc import Generator
from contextlib import contextmanager

import matplotlib.pyplot as plt
from matplotlib.axes import Axes
from matplotlib.figure import Figure

STYLE = "bmh"
DEFAULT_FIGSIZE: tuple[float, float] = (14, 4)


@contextmanager
def chart(
    figsize: tuple[float, float] = DEFAULT_FIGSIZE,
) -> Generator[tuple[Figure, Axes], None, None]:
    with plt.style.context(STYLE):
        fig, ax = plt.subplots(figsize=figsize)
        yield fig, ax
        _ensure_zero_xtick(ax)
        fig.tight_layout()


def _ensure_zero_xtick(ax: Axes) -> None:
    """Guarantee a tick at x=0 so every plot shows where time begins."""
    fig = ax.figure
    if fig is not None:
        fig.canvas.draw()
    ticks = [float(t) for t in ax.get_xticks()]
    xmin, xmax = ax.get_xlim()
    if xmin <= 0.0 <= xmax and not any(abs(t) < 1e-9 for t in ticks):
        ticks = sorted([*ticks, 0.0])
        ax.set_xticks(ticks)
        ax.set_xlim(xmin, xmax)


def mark_first_rent(ax: Axes, time: float | None) -> None:
    """Draw a vertical dashed marker at the first successful rent in the simulation.

    Useful on renter-related plots where data only becomes meaningful after
    the first agent moves into a house. ``time`` of ``None`` is a no-op.
    """
    if time is None:
        return
    ax.axvline(time, ls="--", color="grey", lw=0.8, alpha=0.7, label="first rent")


def set_xlim_with_padding(
    ax: Axes,
    right: float | None = None,
    left: float = 0.0,
    margin: float = 0.02,
) -> None:
    """Anchor x-axis bounds at ``left`` (default 0) with a small visual padding.

    When ``right`` is ``None``, the matplotlib auto-scaled upper bound is used.
    Passing an explicit ``right`` (e.g. ``settings.max_t``) makes the visible
    range identical across plots so they share the same ticks and limits.
    """
    if right is None:
        _, right = ax.get_xlim()
    span = right - left
    pad = span * margin
    ax.set_xlim(left=left - pad, right=right + pad)


def set_ylim_with_padding(
    ax: Axes,
    bottom: float = 0.0,
    top: float | None = None,
    margin: float = 0.02,
) -> None:
    """Anchor y-axis bounds with a small visual padding so data doesn't hug the edges.

    When ``top`` is ``None`` the matplotlib auto-scaled upper bound is preserved
    and only the lower bound is anchored. The padding mimics the breathing room
    matplotlib's auto-scaling provides by default.
    """
    if top is None:
        _, current_top = ax.get_ylim()
        span = current_top - bottom
        ax.set_ylim(bottom=bottom - span * margin)
        return

    span = top - bottom
    pad = span * margin
    ax.set_ylim(bottom=bottom - pad, top=top + pad)
