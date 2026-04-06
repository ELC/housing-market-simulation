import pandera as pa
from pandera.typing import Series


class MigrationFlows(pa.DataFrameModel):
    time: Series[float] = pa.Field(ge=0)
    direction: Series[str] = pa.Field(isin=["entered", "left"])
    agents: Series[int] = pa.Field(ge=0)

    class Config:
        coerce = True
