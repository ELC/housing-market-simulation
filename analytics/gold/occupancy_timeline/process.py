from pandera.typing import DataFrame

from analytics.gold.occupancy_timeline.schema import OccupancyTimeline
from analytics.silver.occupancy.schema import OccupancyLog


def build_occupancy_timeline(
    occupancy: DataFrame[OccupancyLog],
) -> DataFrame[OccupancyTimeline]:
    """Pass-through: silver occupancy validated as gold."""
    return OccupancyTimeline.validate(occupancy.reset_index(drop=True))
