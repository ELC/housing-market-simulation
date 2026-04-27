from __future__ import annotations

import math
from collections import defaultdict
from typing import TYPE_CHECKING, ClassVar, Self

from core.entity.agent import Agent
from core.entity.house import ConstructionState, House, VacantState
from core.events.aging import HouseAged
from core.events.base import ApplyResult, Event
from core.events.maintenance import MaintenanceDue
from core.events.rent import RentStarted
from core.signals import Signal

if TYPE_CHECKING:
    from core.context import SimulationContext
    from core.events.bid import Bid
    from core.market import HousingMarket


class AuctionClear(Event):
    invalidates: ClassVar[frozenset[Signal]] = frozenset({Signal.MARKET_RENT, Signal.HOMELESSNESS})

    def apply(
        self, market: HousingMarket, context: SimulationContext,
    ) -> ApplyResult[Self | RentStarted | HouseAged | MaintenanceDue]:
        bids_by_house = self._group_bids(context)
        updated_houses: dict[str, House] = {}
        events: list[Self | RentStarted | HouseAged | MaintenanceDue] = []
        matched: set[str] = {occ for h in market.entities_of_type(House) if (occ := h.occupant_id())}

        for house in market.entities_of_type(House):
            match house.state:
                case VacantState():
                    new_rent, vacant_events = self._handle_vacant(house, bids_by_house, market, matched)
                    updated_houses[house.id] = house.model_copy(update={"rent_price": new_rent})
                    events.extend(vacant_events)

                case ConstructionState() as state if state.remaining_time <= 0:
                    updated_houses[house.id] = house.model_copy(
                        update={"state": VacantState(last_update_time=self.time)},
                    )
                    events.append(HouseAged(
                        time=self.time + market.settings.aging_interval,
                        house_id=house.id,
                    ))
                    events.append(MaintenanceDue(
                        time=self.time + 1,
                        house_id=house.id,
                    ))

                case ConstructionState() as state:
                    updated_houses[house.id] = house.model_copy(
                        update={"state": ConstructionState(remaining_time=state.remaining_time - 1)},
                    )

                case _:
                    pass

        new_market = market.update_entities(updated_houses)
        new_context = context.model_copy(update={"pending_bids": ()})

        events.append(self.model_copy(update={"time": self.time + 1}))
        return new_market, new_context, events

    def _group_bids(self, context: SimulationContext) -> dict[str, list[Bid]]:  # noqa: PLR6301
        grouped: dict[str, list[Bid]] = defaultdict(list)
        for b in context.pending_bids:
            grouped[b.house_id].append(b)
        return grouped

    def _handle_vacant(
        self,
        house: House,
        bids_by_house: dict[str, list[Bid]],
        market: HousingMarket,
        matched: set[str],
    ) -> tuple[float, list[RentStarted]]:
        bids = bids_by_house.get(house.id, [])
        valid = [
            b for b in bids
            if b.price >= house.rent_price
            and b.agent_id not in matched
            and market.has_entity(b.agent_id, Agent)
        ]

        if not valid:
            decay = math.exp(-market.settings.vacancy_decay_rate)
            return house.rent_price * decay, []

        sorted_bids = sorted(valid, key=lambda b: b.price, reverse=True)
        winner = sorted_bids[0]

        second_price = sorted_bids[1].price if len(sorted_bids) > 1 else house.rent_price
        clearing_price = max(second_price, house.rent_price)

        matched.add(winner.agent_id)
        rent_event = RentStarted(time=self.time, house_id=house.id, tenant_id=winner.agent_id)

        return clearing_price, [rent_event]
