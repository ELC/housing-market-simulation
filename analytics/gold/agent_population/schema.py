import pandera as pa
from pandera.typing import Series


class AgentPopulation(pa.DataFrameModel):
    time: Series[float] = pa.Field(ge=0)
    count: Series[float] = pa.Field(ge=0)
    mean: Series[float] = pa.Field(ge=0)
    ci_low: Series[float] = pa.Field(ge=0)
    ci_high: Series[float] = pa.Field(ge=0)
    entered: Series[float] = pa.Field(ge=0)
    left: Series[float] = pa.Field(ge=0)

    class Config:
        coerce = True
