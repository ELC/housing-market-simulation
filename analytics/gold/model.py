from pandera.typing import DataFrame
from pydantic import BaseModel, ConfigDict

from analytics.gold.agent_population.schema import AgentPopulation
from analytics.gold.housed_renter_wealth.schema import HousedRenterWealth
from analytics.gold.rent_comparison.schema import RentComparison
from analytics.gold.rent_duration_rolling.schema import RentDurationRolling
from analytics.gold.renter_wealth.schema import RenterWealth
from analytics.gold.time_to_rent_rolling.schema import TimeToRentRolling
from analytics.gold.wealth_quartiles.schema import WealthQuartiles
from analytics.gold.wealth_spread.schema import WealthSpread


class Gold(BaseModel):
    model_config = ConfigDict(arbitrary_types_allowed=True)

    renter_wealth: DataFrame[RenterWealth]
    housed_renter_wealth: DataFrame[HousedRenterWealth]
    rent_comparison: DataFrame[RentComparison]
    time_to_rent_rolling: DataFrame[TimeToRentRolling]
    rent_duration_rolling: DataFrame[RentDurationRolling]
    wealth_quartiles: DataFrame[WealthQuartiles]
    wealth_spread: DataFrame[WealthSpread]
    agent_population: DataFrame[AgentPopulation]
