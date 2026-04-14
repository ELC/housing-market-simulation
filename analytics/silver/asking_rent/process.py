import math

import pandas as pd
from pandera.typing import DataFrame

from analytics.bronze.event_facts.schema import EventFact
from analytics.silver.asking_rent.schema import HouseRentLog
from core.entity.house import House
from core.entity.house.state import ConstructionState
from core.events import (
    AuctionClear,
    Evicted,
    HouseDemolished,
    HouseRebuilt,
    RentCollected,
    RentExpired,
    RentStarted,
)
from core.market import HousingMarket

_OFF_MARKET = {"construction", "demolished"}


def project_asking_rent(
    facts: DataFrame[EventFact],
    initial_market: HousingMarket,
) -> DataFrame[HouseRentLog]:
    decay: float = math.exp(-initial_market.settings.vacancy_decay_rate)
    houses = initial_market.entities_of_type(House)
    house_ids: list[str] = [h.id for h in houses]
    house_names: dict[str, str] = {h.id: h.name for h in houses}
    rent: dict[str, float] = {h.id: h.rent_price for h in houses}

    starts = facts.query(f"{EventFact.event_type} == '{RentStarted.event_name()}'")[
        [EventFact.time, EventFact.house_id, EventFact.agent_id]
    ]
    evicts = facts.query(f"{EventFact.event_type} == '{Evicted.event_name()}'")[[EventFact.time, EventFact.house_id]]
    expired = facts.query(f"{EventFact.event_type} == '{RentExpired.event_name()}'")[[EventFact.time, EventFact.house_id]]
    vacated = pd.concat([evicts, expired], ignore_index=True)
    demolished = facts.query(f"{EventFact.event_type} == '{HouseDemolished.event_name()}'")[[EventFact.time, EventFact.house_id]]
    rebuilt_raw = facts.query(f"{EventFact.event_type} == '{HouseRebuilt.event_name()}'")
    rebuilt = rebuilt_raw[[EventFact.time, EventFact.house_id]]
    rebuild_completions = pd.DataFrame([
        {EventFact.time: row[EventFact.time] + row[EventFact.amount], EventFact.house_id: row[EventFact.house_id], "occupant": "vacant"}
        for _, row in rebuilt_raw.iterrows()
    ])

    initial_occupants = [
        "construction" if isinstance(h.state, ConstructionState) else (h.occupant_id() or "vacant")
        for h in houses
    ]
    completions = pd.DataFrame([
        {EventFact.time: float(h.state.remaining_time), EventFact.house_id: h.id, "occupant": "vacant"}
        for h in houses
        if isinstance(h.state, ConstructionState)
    ])

    occ_events = pd.concat(
        [
            pd.DataFrame({
                EventFact.time: [0.0] * len(house_ids),
                EventFact.house_id: house_ids,
                "occupant": initial_occupants,
            }),
            completions,
            starts.rename(columns={EventFact.agent_id: "occupant"}),
            vacated.assign(occupant="vacant"),
            demolished.assign(occupant="demolished"),
            rebuilt.assign(occupant="construction"),
            rebuild_completions,
        ],
        ignore_index=True,
    ).sort_values(EventFact.time)

    event_times: list[float] = sorted(facts[EventFact.time].unique())

    occ_wide = (
        occ_events
        .groupby([EventFact.house_id, EventFact.time])["occupant"]
        .last()
        .unstack(EventFact.house_id)
        .reindex(event_times)
        .ffill()
    )

    auction_set: set[float] = set(facts.query(f"{EventFact.event_type} == '{AuctionClear.event_name()}'")[EventFact.time])
    eviction_dict = vacated.groupby(EventFact.time)[EventFact.house_id].apply(set).to_dict()

    clearing = starts[[EventFact.time, EventFact.house_id]].merge(
        facts.query(f"{EventFact.event_type} == '{RentCollected.event_name()}'"),
        on=[EventFact.time, EventFact.house_id],
    )
    clearing_prices: dict[tuple[float, str], float] = dict(
        zip(
            zip(clearing[EventFact.time], clearing[EventFact.house_id], strict=False),
            clearing[EventFact.amount],
            strict=False,
        )
    )

    rows = list[dict[str, float | str]]()
    prev_vacant: set[str] = set(house_ids)

    for t in event_times:
        curr_vacant = {hid for hid in house_ids if occ_wide[hid][t] == "vacant"}
        on_market = {hid for hid in house_ids if occ_wide[hid][t] not in _OFF_MARKET}

        if t in auction_set:
            was_vacant = prev_vacant | eviction_dict.get(t, set[str]())
            for hid in was_vacant & curr_vacant:
                rent[hid] *= decay
            for hid in was_vacant - curr_vacant:
                if (t, hid) in clearing_prices:
                    rent[hid] = clearing_prices[t, hid]

        rows.extend(
            {
                HouseRentLog.time: t,
                HouseRentLog.house: house_names[hid],
                HouseRentLog.rent: rent[hid],
            }
            for hid in on_market
        )

        prev_vacant = curr_vacant

    return HouseRentLog.validate(pd.DataFrame(rows))
