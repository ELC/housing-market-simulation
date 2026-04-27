import math

import pandas as pd
from pandera.typing import DataFrame

from analytics.bronze.event_facts.schema import EventFact
from analytics.silver.asking_rent.schema import HouseRentLog
from core.events import (
    AuctionClear,
    Evicted,
    HouseBuilt,
    HouseDemolished,
    RentCollected,
    RentExpired,
    RentStarted,
)
from core.settings import SimulationSettings

_OFF_MARKET = {"construction", "demolished"}


def project_asking_rent(
    facts: DataFrame[EventFact],
    settings: SimulationSettings,
    house_names: dict[str, str] | None = None,
) -> DataFrame[HouseRentLog]:
    decay: float = math.exp(-settings.vacancy_decay_rate)
    if house_names is None:
        house_names = {}

    house_ids: list[str] = []
    rent: dict[str, float] = {}

    built_rows = facts.query(f"{EventFact.event_type} == '{HouseBuilt.event_name()}'")
    for _, row in built_rows.iterrows():
        hid = row[EventFact.house_id]
        if hid and hid not in rent:
            house_ids.append(hid)
            house_names.setdefault(hid, hid)
            rent[hid] = row[EventFact.amount] / max(1, settings.max_construction_time)

    starts = facts.query(f"{EventFact.event_type} == '{RentStarted.event_name()}'")[
        [EventFact.time, EventFact.house_id, EventFact.agent_id]
    ]
    evicts = facts.query(f"{EventFact.event_type} == '{Evicted.event_name()}'")[[EventFact.time, EventFact.house_id]]
    expired = facts.query(f"{EventFact.event_type} == '{RentExpired.event_name()}'")[[EventFact.time, EventFact.house_id]]
    vacated = pd.concat([evicts, expired], ignore_index=True)
    demolished = facts.query(f"{EventFact.event_type} == '{HouseDemolished.event_name()}'")[[EventFact.time, EventFact.house_id]]

    built_occ = (
        built_rows[[EventFact.time, EventFact.house_id]]
        .assign(occupant="construction")
    )

    occ_events = pd.concat(
        [
            starts.rename(columns={EventFact.agent_id: "occupant"}),
            vacated.assign(occupant="vacant"),
            demolished.assign(occupant="demolished"),
            built_occ,
        ],
        ignore_index=True,
    ).sort_values(EventFact.time)

    if occ_events.empty:
        return HouseRentLog.validate(
            pd.DataFrame(columns=[HouseRentLog.time, HouseRentLog.house, HouseRentLog.rent])
        )

    event_times: list[float] = sorted(facts[EventFact.time].unique())

    all_house_ids = list(set(house_ids) | set(occ_events[EventFact.house_id].dropna().unique()))

    occ_wide = (
        occ_events
        .groupby([EventFact.house_id, EventFact.time])["occupant"]
        .last()
        .unstack(EventFact.house_id)
        .reindex(event_times)
        .ffill()
    )

    auction_set: set[float] = set(
        facts.query(f"{EventFact.event_type} == '{AuctionClear.event_name()}'")[EventFact.time]
    )
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

    active_ids = [hid for hid in all_house_ids if hid in occ_wide.columns]

    rows = list[dict[str, float | str]]()
    prev_vacant: set[str] = set()

    for t in event_times:
        curr_vacant: set[str] = set()
        on_market: set[str] = set()
        for hid in active_ids:
            if hid not in occ_wide.columns:
                continue
            val = occ_wide[hid].get(t)
            if pd.isna(val):
                continue
            if val == "vacant":
                curr_vacant.add(hid)
            if val not in _OFF_MARKET:
                on_market.add(hid)

        if t in auction_set:
            was_vacant = prev_vacant | eviction_dict.get(t, set[str]())
            for hid in was_vacant & curr_vacant:
                if hid in rent:
                    rent[hid] *= decay
            for hid in was_vacant - curr_vacant:
                if (t, hid) in clearing_prices:
                    rent[hid] = clearing_prices[t, hid]

        rows.extend(
            {
                HouseRentLog.time: t,
                HouseRentLog.house: house_names.get(hid, hid),
                HouseRentLog.rent: rent.get(hid, 0.0),
            }
            for hid in on_market
        )

        prev_vacant = curr_vacant

    if not rows:
        return HouseRentLog.validate(
            pd.DataFrame(columns=[HouseRentLog.time, HouseRentLog.house, HouseRentLog.rent])
        )
    return HouseRentLog.validate(pd.DataFrame(rows))
