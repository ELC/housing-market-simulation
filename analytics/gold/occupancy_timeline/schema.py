import pandera as pa
from pandera.typing import Series


class OccupancyTimeline(pa.DataFrameModel):
    """Gold: house occupancy timeline for scatter plots."""

    time: Series[float] = pa.Field(ge=0)
    house: Series[str]
    occupant: Series[str]

    class Config:
        coerce = True
