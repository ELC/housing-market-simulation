import math
from collections import defaultdict
from typing import TYPE_CHECKING

from core.events.base import ApplyResult, Event
from core.events.bid import Bid
from core.events.rent import RentStarted
from core.house.house import House
from core.house.state import ConstructionState, VacantState
from core.signals import Signal

if TYPE_CHECKING:
    from core.market import HousingMarket


class AuctionClear(Event):
    def invalidates(self) -> set[Signal]:
        return {Signal.MARKET_RENT, Signal.HOMELESSNESS}

    def apply(self, market: "HousingMarket") -> ApplyResult:
        bids_by_house = self._group_bids(market)
        updated_houses: dict[str, House] = {}
        events: list = []
        matched: set[str] = {occ for h in market.houses if (occ := h.occupant_id())}

        for house in market.houses:
            match house.state:
                case VacantState():
                    result = self._handle_vacant(house, bids_by_house, market, matched)
                    updated_houses[house.id] = result.house
                    events.extend(result.events)

                case ConstructionState():
                    updated_houses[house.id] = self._handle_construction(house)

        new_market = market.update_entities(updated_houses).model_copy(
            update={"pending_bids": ()}
        )

        return new_market, events

    def _group_bids(self, market: "HousingMarket") -> dict[str, list[Bid]]:
        grouped: dict[str, list[Bid]] = defaultdict(list)
        for b in market.pending_bids:
            grouped[b.house_id].append(b)
        return grouped

    def _handle_vacant(
        self,
        house: House,
        bids_by_house: dict[str, list[Bid]],
        market: "HousingMarket",
        matched: set[str],
    ) -> "_AuctionResult":
        bids = bids_by_house.get(house.id, [])
        valid = [
            b for b in bids if b.price >= house.rent_price and b.agent_id not in matched
        ]

        if not valid:
            decay = math.exp(-market.settings.vacancy_decay_rate)
            new_price = house.rent_price * decay
            updated = house.model_copy(update={"rent_price": new_price})
            return _AuctionResult(updated, [])

        sorted_bids = sorted(valid, key=lambda b: b.price, reverse=True)
        winner = sorted_bids[0]

        second_price = sorted_bids[1].price if len(sorted_bids) > 1 else house.rent_price
        clearing_price = max(second_price, house.rent_price)
        updated = house.model_copy(update={"rent_price": clearing_price})

        matched.add(winner.agent_id)
        rent_event = RentStarted(
            time=self.time, house_id=house.id, tenant_id=winner.agent_id
        )

        return _AuctionResult(updated, [rent_event])

    def _handle_construction(self, house: House) -> House:
        state = house.state
        if state.remaining_time <= 0:
            return house.model_copy(
                update={"state": VacantState(last_update_time=self.time)}
            )
        return house.model_copy(
            update={"state": ConstructionState(remaining_time=state.remaining_time - 1)}
        )


class _AuctionResult:
    __slots__ = ("house", "events")

    def __init__(self, house: House, events: list) -> None:
        self.house = house
        self.events = events
