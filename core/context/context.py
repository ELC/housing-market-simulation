from collections.abc import Sequence

from core.base import FrozenModel
from core.events.bid import Bid


class SimulationContext(FrozenModel):
    pending_bids: Sequence[Bid] = ()
