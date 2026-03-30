from collections import defaultdict
from collections.abc import Mapping, Sequence
from typing import Self

from core.base import FrozenModel
from core.entity import Entity, EntityType
from core.settings import SimulationSettings


class HousingMarket(FrozenModel):
    entities: Mapping[type[Entity], Mapping[str, EntityType]]
    settings: SimulationSettings

    @classmethod
    def create(cls, entities: Sequence[EntityType], settings: SimulationSettings) -> Self:
        by_type: defaultdict[type[Entity], dict[str, EntityType]] = defaultdict(dict)
        for e in entities:
            by_type[type(e)][e.id] = e
        return cls(entities=by_type, settings=settings)

    def get[T: Entity](self, entity_id: str, entity_type: type[T]) -> T:
        return self.entities[entity_type][entity_id]  # type: ignore[return-value]

    def entities_of_type[T: Entity](self, entity_type: type[T]) -> tuple[T, ...]:
        return tuple(self.entities.get(entity_type, {}).values())  # type: ignore[return-value]

    def all_entities(self) -> tuple[EntityType, ...]:
        return tuple(e for bucket in self.entities.values() for e in bucket.values())

    def update_entities(self, updates: Mapping[str, EntityType]) -> Self:
        new_entities: defaultdict[type[Entity], dict[str, EntityType]] = defaultdict(
            dict, {t: dict(m) for t, m in self.entities.items()}
        )
        for e in updates.values():
            new_entities[type(e)][e.id] = e
        return self.model_copy(update={"entities": new_entities})
