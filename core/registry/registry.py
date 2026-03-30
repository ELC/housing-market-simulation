from collections.abc import Mapping
from collections.abc import Set as AbstractSet

from pydantic import Field

from core.base import FrozenModel
from core.signals import Signal


class SignalRegistry(FrozenModel):
    dependencies: Mapping[Signal, frozenset[Signal]] = Field(default_factory=dict)
    reverse_dependencies: Mapping[Signal, frozenset[Signal]] = Field(default_factory=dict)

    def propagate(self, invalid: AbstractSet[Signal]) -> frozenset[Signal]:
        result: set[Signal] = set(invalid)
        queue = list(invalid)

        while queue:
            s = queue.pop()
            for dep in self.reverse_dependencies.get(s, frozenset()):
                if dep not in result:
                    result.add(dep)
                    queue.append(dep)

        return frozenset(result)
