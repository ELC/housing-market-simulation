import pandera as pa
from pandera.typing import Series


class GoldLandlordProportion(pa.DataFrameModel):
    time: Series[float] = pa.Field(ge=0)
    mean: Series[float] = pa.Field(ge=0, le=1)
    ci_low: Series[float] = pa.Field(ge=0, le=1)
    ci_high: Series[float] = pa.Field(ge=0, le=1)

    class Config:
        coerce = True
