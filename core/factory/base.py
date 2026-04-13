from collections.abc import Callable

from core.engine import SimulationEngine
from core.entity import Agent
from core.settings import SimulationSettings

EngineSetup = tuple[SimulationEngine, Agent]
SimulationFactory = Callable[[SimulationSettings], EngineSetup]
