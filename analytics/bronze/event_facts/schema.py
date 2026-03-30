import pandera as pa
from pandera.typing import Series


class EventFact(pa.DataFrameModel):
    time: Series[float] = pa.Field(ge=0)
    event_type: Series[str]
    agent_id: Series[str] = pa.Field(nullable=True)
    house_id: Series[str] = pa.Field(nullable=True)
    amount: Series[float] = pa.Field(nullable=True)

    class Config:
        coerce = True
