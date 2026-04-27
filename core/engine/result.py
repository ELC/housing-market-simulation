from collections.abc import Sequence

from pydantic import BaseModel, ConfigDict

from core.events import EventType
from core.settings import SimulationSettings


class RunResult(BaseModel):
    """Output of a single simulation run.

    Fully event-sourced: ``event_log`` is the authoritative ledger, and
    ``settings`` carries the configuration the run was executed under
    (used by analytics that need parameters not derivable from events,
    e.g. ``vacancy_decay_rate``).

    The runtime ``HousingMarket`` is intentionally not exposed: every piece
    of information downstream consumers need is either in ``event_log`` or
    in ``settings``.
    """

    model_config = ConfigDict(arbitrary_types_allowed=True)

    event_log: Sequence[EventType]
    settings: SimulationSettings
