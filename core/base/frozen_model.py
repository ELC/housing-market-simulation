from abc import ABC

from pydantic import BaseModel, ConfigDict


class FrozenModel(BaseModel, ABC):
    model_config = ConfigDict(frozen=True, arbitrary_types_allowed=True)
