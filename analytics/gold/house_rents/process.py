from pandera.typing import DataFrame

from analytics.gold.house_rents.schema import HouseRents
from analytics.silver.asking_rent.schema import HouseRentLog


def build_house_rents(
    asking: DataFrame[HouseRentLog],
) -> DataFrame[HouseRents]:
    """Pass-through: silver asking rents validated as gold."""
    return HouseRents.validate(asking.reset_index(drop=True))
