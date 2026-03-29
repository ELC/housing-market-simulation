import math

import pandas as pd
from pandera.typing import DataFrame

from analytics.bronze.event_facts.schema import EventFact
from analytics.silver.asking_rent.schema import HouseRentLog
from core.market import HousingMarket


def project_asking_rent(
    facts: DataFrame[EventFact],
    initial_market: HousingMarket,
) -> DataFrame[HouseRentLog]:
    """Reconstruct each house's listed rent price over time from bronze data only.

    At each auction_clear: vacant houses that remain vacant decay by
    exp(-vacancy_decay_rate); houses that get rented adopt the clearing
    price (captured from the first rent_collected at the same time).
    """
    decay: float = math.exp(-initial_market.settings.vacancy_decay_rate)
    house_ids: list[str] = [h.id for h in initial_market.houses]
    rent: dict[str, float] = {h.id: h.rent_price for h in initial_market.houses}

    starts = facts.query(f"{EventFact.event_type} == 'rent_started'")[
        [EventFact.time, EventFact.house_id, EventFact.agent_id]
    ]
    evicts = facts.query(f"{EventFact.event_type} == 'evicted'")[[EventFact.time, EventFact.house_id]]

    occ_events = pd.concat(
        [
            pd.DataFrame({
                EventFact.time: [0.0] * len(house_ids),
                EventFact.house_id: house_ids,
                "occupant": ["vacant"] * len(house_ids),
            }),
            starts.rename(columns={EventFact.agent_id: "occupant"}),
            evicts.assign(occupant="vacant"),
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

    auction_set: set[float] = set(facts.query(f"{EventFact.event_type} == 'auction_clear'")[EventFact.time])
    eviction_dict: dict[float, set[str]] = evicts.groupby(EventFact.time)[EventFact.house_id].apply(set).to_dict()

    clearing = starts[[EventFact.time, EventFact.house_id]].merge(
        facts.query(f"{EventFact.event_type} == 'rent_collected'"),
        on=[EventFact.time, EventFact.house_id],
    )
    clearing_prices: dict[tuple[float, str], float] = dict(
        zip(
            zip(clearing[EventFact.time], clearing[EventFact.house_id], strict=False),
            clearing[EventFact.amount],
            strict=False,
        )
    )

    rows: list[dict[str, float | str]] = []
    prev_vacant: set[str] = set(house_ids)

    for t in event_times:
        curr_vacant: set[str] = {hid for hid in house_ids if occ_wide.loc[t, hid] == "vacant"}

        if t in auction_set:
            was_vacant = prev_vacant | eviction_dict.get(t, set())
            for hid in was_vacant & curr_vacant:
                rent[hid] *= decay
            for hid in was_vacant - curr_vacant:
                if (t, hid) in clearing_prices:
                    rent[hid] = clearing_prices[t, hid]

        rows.extend(
            {
                HouseRentLog.time: t,
                HouseRentLog.house: hid,
                HouseRentLog.rent: rent[hid],
            }
            for hid in house_ids
        )

        prev_vacant = curr_vacant

    return HouseRentLog.validate(pd.DataFrame(rows))
