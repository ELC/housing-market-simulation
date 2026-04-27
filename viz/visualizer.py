from functools import cached_property

import matplotlib.pyplot as plt
from matplotlib.axes import Axes
from matplotlib.figure import Figure

from analytics.gold.model import Gold
from core.settings import SimulationSettings
from viz.demolition_age import plot_demolition_age
from viz.house_age import plot_house_age
from viz.house_supply import plot_house_supply
from viz.housed_renter_wealth import plot_housed_renter_wealth
from viz.houses_per_landlord import plot_houses_per_landlord
from viz.landlord_population import plot_landlord_population
from viz.landlord_proportion import plot_landlord_proportion
from viz.landlord_wealth import plot_landlord_wealth
from viz.market_balance import plot_market_balance
from viz.paid_vs_asked import plot_paid_vs_asked
from viz.population import plot_population
from viz.rent_duration import plot_rent_duration
from viz.rent_to_income import plot_rent_to_income
from viz.tenure import plot_tenure
from viz.time_to_rent import plot_time_to_rent
from viz.wealth_quartiles import plot_wealth_quartiles, plot_wealth_spread


class DataVisualizer:
    def __init__(self, gold: Gold, settings: SimulationSettings | None = None) -> None:
        self._gold = gold
        self._settings = settings

    def _show(self, fig: Figure, ax: Axes) -> None:
        plt.show()
        _ = (fig, ax)

    @cached_property
    def _first_rent_time(self) -> float | None:
        """Earliest time at which any renter is housed (i.e. paying rent)."""
        df = self._gold.housed_renter_wealth
        if df.empty:
            return None
        return float(df["time"].min())

    def _with_xlim(self, kwargs: dict[str, object]) -> dict[str, object]:
        """Inject ``max_t`` from settings so every plot shares the same x-axis range."""
        if self._settings is not None:
            kwargs.setdefault("max_t", self._settings.max_t)
        return kwargs

    def _renter_kwargs(self, kwargs: dict[str, object]) -> dict[str, object]:
        """Inject x-axis range plus the first-rent marker for renter-related plots."""
        kwargs = self._with_xlim(kwargs)
        if self._first_rent_time is not None:
            kwargs.setdefault("first_rent_time", self._first_rent_time)
        return kwargs

    def plot_housed_renter_wealth(self, **kwargs: object) -> None:
        self._show(*plot_housed_renter_wealth(self._gold.housed_renter_wealth, **self._renter_kwargs(kwargs)))  # type: ignore[arg-type]

    def plot_paid_vs_asked(self, **kwargs: object) -> None:
        self._show(*plot_paid_vs_asked(self._gold.rent_comparison, **self._renter_kwargs(kwargs)))  # type: ignore[arg-type]

    def plot_population(self, **kwargs: object) -> None:
        self._show(*plot_population(self._gold.agent_population, **self._with_xlim(kwargs)))  # type: ignore[arg-type]

    def plot_rent_duration(self, **kwargs: object) -> None:
        self._show(*plot_rent_duration(self._gold.rent_duration_rolling, settings=self._settings, **self._renter_kwargs(kwargs)))  # type: ignore[arg-type]

    def plot_time_to_rent(self, **kwargs: object) -> None:
        self._show(*plot_time_to_rent(self._gold.time_to_rent_rolling, **self._renter_kwargs(kwargs)))  # type: ignore[arg-type]

    def plot_wealth_quartiles(self, **kwargs: object) -> None:
        self._show(*plot_wealth_quartiles(self._gold.wealth_quartiles, **self._renter_kwargs(kwargs)))  # type: ignore[arg-type]

    def plot_wealth_spread(self, **kwargs: object) -> None:
        self._show(*plot_wealth_spread(self._gold.wealth_spread, **self._renter_kwargs(kwargs)))  # type: ignore[arg-type]

    def plot_house_supply(self, **kwargs: object) -> None:
        self._show(*plot_house_supply(self._gold.house_supply, **self._with_xlim(kwargs)))  # type: ignore[arg-type]

    def plot_landlord_population(self, **kwargs: object) -> None:
        self._show(*plot_landlord_population(self._gold.landlord_population, **self._with_xlim(kwargs)))  # type: ignore[arg-type]

    def plot_houses_per_landlord(self, **kwargs: object) -> None:
        self._show(*plot_houses_per_landlord(self._gold.houses_per_landlord, **self._with_xlim(kwargs)))  # type: ignore[arg-type]

    def plot_landlord_proportion(self, **kwargs: object) -> None:
        self._show(*plot_landlord_proportion(self._gold.landlord_proportion, **self._with_xlim(kwargs)))  # type: ignore[arg-type]

    def plot_tenure(self, **kwargs: object) -> None:
        self._show(*plot_tenure(self._gold.tenure, **self._renter_kwargs(kwargs)))  # type: ignore[arg-type]

    def plot_landlord_wealth(self, **kwargs: object) -> None:
        self._show(*plot_landlord_wealth(self._gold.landlord_wealth, **self._with_xlim(kwargs)))  # type: ignore[arg-type]

    def plot_market_balance(self, **kwargs: object) -> None:
        self._show(*plot_market_balance(self._gold.market_balance, **self._renter_kwargs(kwargs)))  # type: ignore[arg-type]

    def plot_rent_to_income(self, **kwargs: object) -> None:
        self._show(*plot_rent_to_income(self._gold.rent_to_income, **self._renter_kwargs(kwargs)))  # type: ignore[arg-type]

    def plot_house_age(self, **kwargs: object) -> None:
        self._show(*plot_house_age(self._gold.house_age, **self._with_xlim(kwargs)))  # type: ignore[arg-type]

    def plot_demolition_age(self, **kwargs: object) -> None:
        self._show(*plot_demolition_age(self._gold.demolition_age, **self._with_xlim(kwargs)))  # type: ignore[arg-type]
