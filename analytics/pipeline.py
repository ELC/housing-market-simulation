from collections.abc import Callable, MutableMapping
from typing import Any

from pydantic import BaseModel, ConfigDict, PrivateAttr
from tqdm.auto import tqdm

StepFn = Callable[..., Any]
Step = tuple[str, StepFn]


class AnalyticsPipeline(BaseModel):
    model_config = ConfigDict(arbitrary_types_allowed=True)

    steps: list[Step]
    _results: MutableMapping[str, Any] = PrivateAttr(default_factory=dict)

    def run(self, data: Any, *, verbose: bool = True) -> Any:
        self._results = {}
        for name, step in tqdm(self.steps, desc="Pipeline", disable=not verbose):
            data = step(data)
            self._results[name] = data
        return data

    @property
    def named_steps(self) -> MutableMapping[str, Any]:
        return self._results

    def get_result(self, name: str) -> Any:
        try:
            return self._results[name]
        except KeyError:
            available = list(self._results) or "(pipeline has not been run yet)"
            msg = f"Step {name!r} not found. Available: {available}"
            raise KeyError(msg) from None
