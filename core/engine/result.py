from collections.abc import Sequence

from pydantic import BaseModel, ConfigDict

from core.events import EventType
from core.market import HousingMarket


class RunResult(BaseModel):
    model_config = ConfigDict(arbitrary_types_allowed=True)

    event_log: Sequence[EventType]
    market: HousingMarket
    landlord_name: str
