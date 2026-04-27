from collections.abc import Callable

from core.engine import SimulationEngine
from core.settings import SimulationSettings

SimulationFactory = Callable[[SimulationSettings], SimulationEngine]
