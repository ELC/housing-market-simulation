import pandera as pa
from pandera.typing import Series


class RentComparison(pa.DataFrameModel):
    """Gold: paid vs asked rent in long form for overlay plots."""

    time: Series[float] = pa.Field(ge=0)
    amount: Series[float]
    kind: Series[str]

    class Config:
        coerce = True
