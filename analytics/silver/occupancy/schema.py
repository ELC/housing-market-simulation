import pandera as pa
from pandera.typing import Series


class OccupancyLog(pa.DataFrameModel):
    """Silver: house occupancy state at each event time."""

    time: Series[float] = pa.Field(ge=0)
    house: Series[str]
    occupant: Series[str]

    class Config:
        coerce = True
