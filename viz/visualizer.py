import matplotlib.pyplot as plt
from matplotlib.axes import Axes
from matplotlib.figure import Figure

from analytics.gold.model import Gold
from core.settings import SimulationSettings
from viz.housed_renter_wealth import plot_housed_renter_wealth
from viz.paid_vs_asked import plot_paid_vs_asked
from viz.population import plot_population
from viz.rent_duration import plot_rent_duration
from viz.time_to_rent import plot_time_to_rent
from viz.wealth_quartiles import plot_wealth_quartiles, plot_wealth_spread


class DataVisualizer:
    def __init__(self, gold: Gold, settings: SimulationSettings | None = None) -> None:
        self._gold = gold
        self._settings = settings

    def _show(self, fig: Figure, ax: Axes) -> tuple[Figure, Axes]:
        plt.show()
        return fig, ax

    def plot_housed_renter_wealth(self, **kwargs: object) -> tuple[Figure, Axes]:
        return self._show(*plot_housed_renter_wealth(self._gold.housed_renter_wealth, **kwargs))  # type: ignore[arg-type]

    def plot_paid_vs_asked(self, **kwargs: object) -> tuple[Figure, Axes]:
        return self._show(*plot_paid_vs_asked(self._gold.rent_comparison, **kwargs))  # type: ignore[arg-type]

    def plot_population(self, **kwargs: object) -> tuple[Figure, Axes]:
        return self._show(*plot_population(self._gold.agent_population, **kwargs))  # type: ignore[arg-type]

    def plot_rent_duration(self, **kwargs: object) -> tuple[Figure, Axes]:
        return self._show(*plot_rent_duration(self._gold.rent_duration_rolling, settings=self._settings, **kwargs))  # type: ignore[arg-type]

    def plot_time_to_rent(self, **kwargs: object) -> tuple[Figure, Axes]:
        return self._show(*plot_time_to_rent(self._gold.time_to_rent_rolling, **kwargs))  # type: ignore[arg-type]

    def plot_wealth_quartiles(self, **kwargs: object) -> tuple[Figure, Axes]:
        return self._show(*plot_wealth_quartiles(self._gold.wealth_quartiles, **kwargs))  # type: ignore[arg-type]

    def plot_wealth_spread(self, **kwargs: object) -> tuple[Figure, Axes]:
        return self._show(*plot_wealth_spread(self._gold.wealth_spread, **kwargs))  # type: ignore[arg-type]
