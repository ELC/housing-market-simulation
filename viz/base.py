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
    """Create a styled figure and apply tight_layout on exit."""
    with plt.style.context(STYLE):
        fig, ax = plt.subplots(figsize=figsize)
        yield fig, ax
        fig.tight_layout()
