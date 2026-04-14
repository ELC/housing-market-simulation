from collections.abc import Sequence
from pathlib import Path

from pydantic import BaseModel, ConfigDict
from tqdm.auto import tqdm

from analytics.gold.model import Gold
from analytics.persistence.engine import PersistenceEngine


class PersistenceManager(BaseModel):
    model_config = ConfigDict(arbitrary_types_allowed=True)

    engines: Sequence[PersistenceEngine]

    def save(self, gold: Gold, path: Path) -> None:
        for engine in tqdm(self.engines, desc="Persisting"):
            engine.save(gold, path / engine.suffix)

    def load(self, path: Path, engine: PersistenceEngine) -> Gold:
        return engine.load(path / engine.suffix)
