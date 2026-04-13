from collections.abc import MutableSequence
from typing import TypedDict

from pandera.typing import DataFrame
from pydantic import BaseModel, ConfigDict

from analytics.silver.asking_rent.schema import HouseRentLog
from analytics.silver.occupancy.schema import OccupancyLog
from analytics.silver.population.schema import PopulationLog
from analytics.silver.rent_duration.schema import RentDuration
from analytics.silver.rent_payments.schema import RentLog
from analytics.silver.time_to_rent.schema import TimeToRent
from analytics.silver.wealth.schema import WealthLog


class SilverCollectors(TypedDict):
    wealth: MutableSequence[DataFrame[WealthLog]]
    occupancy: MutableSequence[DataFrame[OccupancyLog]]
    rent_payments: MutableSequence[DataFrame[RentLog]]
    time_to_rent: MutableSequence[DataFrame[TimeToRent]]
    asking_rent: MutableSequence[DataFrame[HouseRentLog]]
    rent_duration: MutableSequence[DataFrame[RentDuration]]
    population: MutableSequence[DataFrame[PopulationLog]]


class Silver(BaseModel):
    model_config = ConfigDict(arbitrary_types_allowed=True)

    wealth: DataFrame[WealthLog]
    occupancy: DataFrame[OccupancyLog]
    rent_payments: DataFrame[RentLog]
    time_to_rent: DataFrame[TimeToRent]
    asking_rent: DataFrame[HouseRentLog]
    rent_duration: DataFrame[RentDuration]
    owner_names: frozenset[str]
    population: DataFrame[PopulationLog]
