from typing import TypeAlias

from pydantic import BaseModel, ConfigDict


class FrozenModel(BaseModel):
    model_config = ConfigDict(frozen=True, arbitrary_types_allowed=True)


ApplyResult: TypeAlias = tuple["HousingMarket", list["EventType"]]
