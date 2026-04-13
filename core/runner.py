from collections.abc import Generator
from itertools import islice

from pydantic import BaseModel, Field
from tqdm.auto import tqdm

from core.engine import RunResult
from core.factory import SimulationFactory
from core.settings import SimulationSettings


class SimulationRunner(BaseModel):
    settings: SimulationSettings = Field(default_factory=SimulationSettings)

    def _runs(
        self,
        factory: SimulationFactory,
    ) -> Generator[RunResult, None, None]:
        while True:
            engine, landlord = factory(self.settings)
            yield engine.run(
                n_steps=self.settings.n_steps,
                max_t=self.settings.max_t,
                landlord_name=landlord.name,
            )

    def run(
        self,
        factory: SimulationFactory,
        *,
        verbose: bool = True,
    ) -> Generator[RunResult, None, None]:
        stream = islice(self._runs(factory), self.settings.n_runs)
        it = tqdm(stream, total=self.settings.n_runs, desc="Simulating", disable=not verbose)
        yield from it
