from pandera.typing import DataFrame
from pydantic import BaseModel, ConfigDict

from analytics.gold.agent_population.schema import AgentPopulation
from analytics.gold.demolition_age.schema import GoldDemolitionAge
from analytics.gold.house_age.schema import GoldHouseAge
from analytics.gold.house_supply.schema import GoldHouseSupply
from analytics.gold.housed_renter_wealth.schema import HousedRenterWealth
from analytics.gold.houses_per_landlord.schema import GoldHousesPerLandlord
from analytics.gold.landlord_population.schema import GoldLandlordPopulation
from analytics.gold.landlord_proportion.schema import GoldLandlordProportion
from analytics.gold.landlord_wealth.schema import GoldLandlordWealth
from analytics.gold.market_balance.schema import GoldMarketBalance
from analytics.gold.rent_comparison.schema import RentComparison
from analytics.gold.rent_duration_rolling.schema import RentDurationRolling
from analytics.gold.rent_to_income.schema import GoldRentToIncome
from analytics.gold.tenure.schema import GoldTenure
from analytics.gold.time_to_rent_rolling.schema import TimeToRentRolling
from analytics.gold.wealth_quartiles.schema import WealthQuartiles
from analytics.gold.wealth_spread.schema import WealthSpread


class Gold(BaseModel):
    model_config = ConfigDict(arbitrary_types_allowed=True)

    housed_renter_wealth: DataFrame[HousedRenterWealth]
    rent_comparison: DataFrame[RentComparison]
    time_to_rent_rolling: DataFrame[TimeToRentRolling]
    rent_duration_rolling: DataFrame[RentDurationRolling]
    wealth_quartiles: DataFrame[WealthQuartiles]
    wealth_spread: DataFrame[WealthSpread]
    agent_population: DataFrame[AgentPopulation]
    house_supply: DataFrame[GoldHouseSupply]
    landlord_population: DataFrame[GoldLandlordPopulation]
    houses_per_landlord: DataFrame[GoldHousesPerLandlord]
    landlord_proportion: DataFrame[GoldLandlordProportion]
    tenure: DataFrame[GoldTenure]
    landlord_wealth: DataFrame[GoldLandlordWealth]
    market_balance: DataFrame[GoldMarketBalance]
    rent_to_income: DataFrame[GoldRentToIncome]
    house_age: DataFrame[GoldHouseAge]
    demolition_age: DataFrame[GoldDemolitionAge]
