from abc import abstractmethod
from pathlib import Path

from pydantic import BaseModel, ConfigDict

from analytics.gold.model import Gold


class PersistenceEngine(BaseModel):
    model_config = ConfigDict(arbitrary_types_allowed=True)

    suffix: str

    @abstractmethod
    def save(self, gold: Gold, path: Path) -> None: ...

    @abstractmethod
    def load(self, path: Path) -> Gold: ...
