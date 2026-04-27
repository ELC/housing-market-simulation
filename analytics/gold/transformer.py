"""`GoldTransformer` — builds cross-run statistics from per-run silver aggregates.

Consumes a :class:`~analytics.silver.store.SilverStore` populated by
:class:`~analytics.silver.transformer.SilverTransformer` and dispatches
each pre-aggregated ``(run_id, group_cols, value)`` frame to the matching
``build_*`` function. No raw silver materialization happens here — every
input is sized ``O(n_runs * n_group_keys)``.
"""
from __future__ import annotations

from typing import TYPE_CHECKING

from analytics.gold.agent_population import build_agent_population
from analytics.gold.demolition_age import build_demolition_age
from analytics.gold.house_age import build_house_age
from analytics.gold.house_supply import build_house_supply
from analytics.gold.housed_renter_wealth import build_housed_renter_wealth
from analytics.gold.houses_per_landlord import build_houses_per_landlord
from analytics.gold.landlord_population import build_landlord_population
from analytics.gold.landlord_proportion import build_landlord_proportion
from analytics.gold.landlord_wealth import build_landlord_wealth
from analytics.gold.market_balance import build_market_balance
from analytics.gold.model import Gold
from analytics.gold.rent_comparison import build_rent_comparison
from analytics.gold.rent_duration_rolling import build_rent_duration_rolling
from analytics.gold.rent_to_income import build_rent_to_income
from analytics.gold.tenure import build_tenure
from analytics.gold.time_to_rent_rolling import build_time_to_rent_rolling
from analytics.gold.wealth_quartiles import build_wealth_quartiles
from analytics.gold.wealth_spread import build_wealth_spread

if TYPE_CHECKING:
    from analytics.silver.store import SilverStore


class GoldTransformer:
    def __call__(self, silver: SilverStore) -> Gold:
        get = silver.get_aggregate
        return Gold(
            housed_renter_wealth=build_housed_renter_wealth(get("housed_renter_wealth")),
            rent_comparison=build_rent_comparison(get("rent_comparison")),
            time_to_rent_rolling=build_time_to_rent_rolling(get("time_to_rent_rolling")),
            rent_duration_rolling=build_rent_duration_rolling(get("rent_duration_rolling")),
            wealth_quartiles=build_wealth_quartiles(get("wealth_quartiles")),
            wealth_spread=build_wealth_spread(get("wealth_spread")),
            agent_population=build_agent_population(get("agent_population")),
            house_supply=build_house_supply(get("house_supply")),
            landlord_population=build_landlord_population(get("landlord_population")),
            houses_per_landlord=build_houses_per_landlord(get("houses_per_landlord")),
            landlord_proportion=build_landlord_proportion(get("landlord_proportion")),
            tenure=build_tenure(get("tenure")),
            landlord_wealth=build_landlord_wealth(get("landlord_wealth")),
            market_balance=build_market_balance(get("market_balance")),
            rent_to_income=build_rent_to_income(get("rent_to_income")),
            house_age=build_house_age(get("house_age")),
            demolition_age=build_demolition_age(get("demolition_age")),
        )
