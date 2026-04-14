from pathlib import Path

import pandas as pd

from analytics.gold.model import Gold
from analytics.persistence.engine import PersistenceEngine


class FeatherEngine(PersistenceEngine):
    suffix: str = "feather"

    def save(self, gold: Gold, path: Path) -> None:
        path.mkdir(parents=True, exist_ok=True)
        for name in Gold.model_fields:
            df: pd.DataFrame = getattr(gold, name)
            df.to_feather(path / f"{name}.feather")

    def load(self, path: Path) -> Gold:
        frames = {}
        for name in Gold.model_fields:
            frames[name] = pd.read_feather(path / f"{name}.feather")
        return Gold(**frames)
