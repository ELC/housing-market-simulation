from analytics.gold.agent_population import build_agent_population
from analytics.gold.housed_renter_wealth import build_housed_renter_wealth
from analytics.gold.model import Gold
from analytics.gold.rent_comparison import build_rent_comparison
from analytics.gold.rent_duration_rolling import build_rent_duration_rolling
from analytics.gold.renter_wealth import build_renter_wealth
from analytics.gold.time_to_rent_rolling import build_time_to_rent_rolling
from analytics.gold.wealth_quartiles import build_wealth_quartiles
from analytics.gold.wealth_spread import build_wealth_spread
from analytics.silver.model import Silver


class GoldTransformer:
    def __call__(self, silver: Silver) -> Gold:
        renter_wealth = build_renter_wealth(silver.wealth, owner_names=silver.owner_names)
        wealth_quartiles = build_wealth_quartiles(renter_wealth)

        return Gold(
            renter_wealth=renter_wealth,
            housed_renter_wealth=build_housed_renter_wealth(
                silver.wealth, silver.occupancy, owner_names=silver.owner_names,
            ),
            rent_comparison=build_rent_comparison(silver.rent_payments, silver.asking_rent),
            time_to_rent_rolling=build_time_to_rent_rolling(silver.time_to_rent),
            rent_duration_rolling=build_rent_duration_rolling(silver.rent_duration),
            wealth_quartiles=wealth_quartiles,
            wealth_spread=build_wealth_spread(wealth_quartiles),
            agent_population=build_agent_population(silver.population),
        )
