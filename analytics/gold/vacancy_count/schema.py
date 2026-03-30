import pandera as pa
from pandera.typing import Series


class VacancyCount(pa.DataFrameModel):
    time: Series[float] = pa.Field(ge=0)
    n_vacant: Series[int] = pa.Field(ge=0)

    class Config:
        coerce = True
