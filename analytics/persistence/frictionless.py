from pathlib import Path

import pandas as pd
from frictionless import Package, Resource, Schema

from analytics.gold.model import Gold
from analytics.persistence.engine import PersistenceEngine

_DTYPE_MAP = {
    "float64": "number",
    "float32": "number",
    "int64": "integer",
    "int32": "integer",
    "object": "string",
    "string": "string",
    "bool": "boolean",
}


def _schema_from_dataframe(df: pd.DataFrame) -> Schema:
    fields = [
        {"name": col, "type": _DTYPE_MAP.get(str(df[col].dtype), "string")}
        for col in df.columns
    ]
    return Schema.from_descriptor({"fields": fields})


class FrictionlessEngine(PersistenceEngine):
    suffix: str = "frictionless"

    def save(self, gold: Gold, path: Path) -> None:
        path.mkdir(parents=True, exist_ok=True)
        resources: list[Resource] = []
        for name in Gold.model_fields:
            df: pd.DataFrame = getattr(gold, name)
            csv_path = path / f"{name}.csv"
            df.to_csv(csv_path, index=False)
            resources.append(
                Resource(name=name, path=f"{name}.csv", schema=_schema_from_dataframe(df)),
            )
        package = Package(resources=resources, basepath=str(path))
        package.to_json(str(path / "datapackage.json"))

    def load(self, path: Path) -> Gold:
        package = Package(str(path / "datapackage.json"))
        frames = {}
        for name in Gold.model_fields:
            resource = package.get_resource(name)
            frames[name] = resource.to_pandas()
        return Gold(**frames)
