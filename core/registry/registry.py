from typing import FrozenSet, Mapping

from pydantic import Field

from core.base import FrozenModel


class SignalRegistry(FrozenModel):
    dependencies: Mapping[str, FrozenSet[str]] = Field(default_factory=dict)
    reverse_dependencies: Mapping[str, FrozenSet[str]] = Field(default_factory=dict)

    def propagate(self, invalid: set[str]) -> set[str]:
        result = set(invalid)
        queue = list(invalid)

        while queue:
            s = queue.pop()
            for dep in self.reverse_dependencies.get(s, frozenset()):
                if dep not in result:
                    result.add(dep)
                    queue.append(dep)

        return result
