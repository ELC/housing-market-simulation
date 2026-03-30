import uuid
from collections.abc import Iterator
from typing import NamedTuple

from faker import Faker
from pydantic import BaseModel, PrivateAttr

ENTITY_NAMESPACE = uuid.UUID("a1b2c3d4-e5f6-7890-abcd-ef1234567890")


class IdentityRecord(NamedTuple):
    id: str
    name: str


class EntityIdentity(BaseModel, Iterator[IdentityRecord]):
    provider: str
    seed: int = 42
    _fake: Faker = PrivateAttr(default_factory=Faker)

    def model_post_init(self, _context: object) -> None:
        self._fake.seed_instance(self.seed)

    def __next__(self) -> IdentityRecord:
        name: str = getattr(self._fake, self.provider)()
        return IdentityRecord(str(uuid.uuid5(ENTITY_NAMESPACE, name)), name)
