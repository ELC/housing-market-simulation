import math

import pandas as pd
from pandera.typing import DataFrame

from analytics.bronze.schemas import EventFact
from analytics.silver.schemas import (
    HouseRentLog,
    OccupancyLog,
    RentLog,
    TimeToRent,
    WealthLog,
)
from core.market import HousingMarket


def project_wealth(
    facts: DataFrame[EventFact],
    initial_market: HousingMarket,
) -> DataFrame[WealthLog]:
    """Cumulative agent wealth via a signed-delta ledger, ffilled to every event time."""
    house_owners: dict[str, str] = {h.id: h.owner_id for h in initial_market.houses}
    event_times: list[float] = sorted(facts[EventFact.time].unique())

    inc: pd.DataFrame = (
        facts.query(f"{EventFact.event_type} == 'income'")
        [[EventFact.time, EventFact.agent_id, EventFact.amount]]
        .rename(columns={EventFact.agent_id: WealthLog.agent, EventFact.amount: "delta"})
    )

    rent: pd.DataFrame = facts.query(f"{EventFact.event_type} == 'rent_collected'")

    debits: pd.DataFrame = (
        rent[[EventFact.time, EventFact.agent_id, EventFact.amount]]
        .rename(columns={EventFact.agent_id: WealthLog.agent, EventFact.amount: "delta"})
        .assign(delta=lambda df: -df["delta"])
    )

    credits: pd.DataFrame = (
        rent[[EventFact.time, EventFact.house_id, EventFact.amount]]
        .assign(**{WealthLog.agent: lambda df: df[EventFact.house_id].map(house_owners)})
        .drop(columns=[EventFact.house_id])
        .rename(columns={EventFact.amount: "delta"})
    )

    initials: pd.DataFrame = pd.DataFrame(
        {
            EventFact.time: [0.0] * len(initial_market.agents),
            WealthLog.agent: [a.id for a in initial_market.agents],
            "delta": [a.money for a in initial_market.agents],
        }
    )

    return (
        pd.concat([initials, inc, debits, credits], ignore_index=True)
        .sort_values(EventFact.time, kind="mergesort")
        .assign(**{WealthLog.money: lambda df: df.groupby(WealthLog.agent)["delta"].cumsum()})
        .groupby([WealthLog.agent, EventFact.time])[WealthLog.money]
        .last()
        .unstack(WealthLog.agent)
        .reindex(event_times)
        .ffill()
        .rename_axis(columns=WealthLog.agent)
        .stack()
        .rename(WealthLog.money)
        .reset_index()
        .pipe(WealthLog.validate)
    )


def project_occupancy(
    facts: DataFrame[EventFact],
    initial_market: HousingMarket,
) -> DataFrame[OccupancyLog]:
    """House occupancy from rent_started / evicted events, ffilled to every event time."""
    event_times: list[float] = sorted(facts[EventFact.time].unique())

    starts: pd.DataFrame = (
        facts.query(f"{EventFact.event_type} == 'rent_started'")
        [[EventFact.time, EventFact.house_id, EventFact.agent_id]]
        .rename(
            columns={
                EventFact.house_id: OccupancyLog.house,
                EventFact.agent_id: OccupancyLog.occupant,
            }
        )
    )

    evicts: pd.DataFrame = (
        facts.query(f"{EventFact.event_type} == 'evicted'")
        [[EventFact.time, EventFact.house_id]]
        .rename(columns={EventFact.house_id: OccupancyLog.house})
        .assign(**{OccupancyLog.occupant: "vacant"})
    )

    initials: pd.DataFrame = pd.DataFrame(
        {
            EventFact.time: [0.0] * len(initial_market.houses),
            OccupancyLog.house: [h.id for h in initial_market.houses],
            OccupancyLog.occupant: [
                h.occupant_id() or "vacant" for h in initial_market.houses
            ],
        }
    )

    return (
        pd.concat([initials, starts, evicts], ignore_index=True)
        .sort_values(EventFact.time)
        .groupby([OccupancyLog.house, EventFact.time])[OccupancyLog.occupant]
        .last()
        .unstack(OccupancyLog.house)
        .reindex(event_times)
        .ffill()
        .rename_axis(columns=OccupancyLog.house)
        .stack()
        .rename(OccupancyLog.occupant)
        .reset_index()
        .pipe(OccupancyLog.validate)
    )


def project_rent_payments(
    facts: DataFrame[EventFact],
) -> DataFrame[RentLog]:
    """Extract individual rent payment records from the fact table."""
    return (
        facts.query(f"{EventFact.event_type} == 'rent_collected'")
        [[EventFact.time, EventFact.house_id, EventFact.agent_id, EventFact.amount]]
        .rename(
            columns={
                EventFact.house_id: RentLog.house,
                EventFact.agent_id: RentLog.tenant,
            }
        )
        .reset_index(drop=True)
        .pipe(RentLog.validate)
    )


def project_time_to_rent(
    facts: DataFrame[EventFact],
    initial_market: HousingMarket,
) -> DataFrame[TimeToRent]:
    """Pair each vacancy start (eviction or t=0) with the next rent_started for the same house."""
    initial_vacant: pd.DataFrame = pd.DataFrame(
        {
            "start": [0.0] * len(initial_market.houses),
            EventFact.house_id: [h.id for h in initial_market.houses],
        }
    )

    vacancy_starts: pd.DataFrame = (
        pd.concat(
            [
                initial_vacant,
                facts.query(f"{EventFact.event_type} == 'evicted'")
                [[EventFact.time, EventFact.house_id]]
                .rename(columns={EventFact.time: "start"}),
            ],
            ignore_index=True,
        )
        .sort_values("start")
        .assign(rank=lambda df: df.groupby(EventFact.house_id).cumcount())
    )

    vacancy_ends: pd.DataFrame = (
        facts.query(f"{EventFact.event_type} == 'rent_started'")
        [[EventFact.time, EventFact.house_id]]
        .rename(columns={EventFact.time: "end"})
        .sort_values("end")
        .assign(rank=lambda df: df.groupby(EventFact.house_id).cumcount())
    )

    return (
        vacancy_starts.merge(vacancy_ends, on=[EventFact.house_id, "rank"])
        .assign(
            **{
                TimeToRent.time: lambda df: df["end"],
                TimeToRent.house: lambda df: df[EventFact.house_id],
                TimeToRent.duration: lambda df: df["end"] - df["start"],
            }
        )
        [[TimeToRent.time, TimeToRent.house, TimeToRent.duration]]
        .pipe(TimeToRent.validate)
    )


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

    starts = (
        facts.query(f"{EventFact.event_type} == 'rent_started'")
        [[EventFact.time, EventFact.house_id, EventFact.agent_id]]
    )
    evicts = (
        facts.query(f"{EventFact.event_type} == 'evicted'")
        [[EventFact.time, EventFact.house_id]]
    )

    occ_events: pd.DataFrame = pd.concat(
        [
            pd.DataFrame(
                {
                    EventFact.time: [0.0] * len(house_ids),
                    EventFact.house_id: house_ids,
                    "occupant": ["vacant"] * len(house_ids),
                }
            ),
            starts.rename(columns={EventFact.agent_id: "occupant"}),
            evicts.assign(occupant="vacant"),
        ],
        ignore_index=True,
    ).sort_values(EventFact.time)

    event_times: list[float] = sorted(facts[EventFact.time].unique())

    occ_wide: pd.DataFrame = (
        occ_events.groupby([EventFact.house_id, EventFact.time])["occupant"]
        .last()
        .unstack(EventFact.house_id)
        .reindex(event_times)
        .ffill()
    )

    auction_set: set[float] = set(
        facts.query(f"{EventFact.event_type} == 'auction_clear'")[EventFact.time]
    )
    eviction_dict: dict[float, set[str]] = (
        evicts.groupby(EventFact.time)[EventFact.house_id].apply(set).to_dict()
    )

    clearing = starts[[EventFact.time, EventFact.house_id]].merge(
        facts.query(f"{EventFact.event_type} == 'rent_collected'"),
        on=[EventFact.time, EventFact.house_id],
    )
    clearing_prices: dict[tuple[float, str], float] = dict(
        zip(
            zip(clearing[EventFact.time], clearing[EventFact.house_id]),
            clearing[EventFact.amount],
        )
    )

    rows: list[dict[str, float | str]] = []
    prev_vacant: set[str] = set(house_ids)

    for t in event_times:
        curr_vacant: set[str] = {
            hid for hid in house_ids if occ_wide.loc[t, hid] == "vacant"
        }

        if t in auction_set:
            was_vacant = prev_vacant | eviction_dict.get(t, set())
            for hid in was_vacant & curr_vacant:
                rent[hid] *= decay
            for hid in was_vacant - curr_vacant:
                if (t, hid) in clearing_prices:
                    rent[hid] = clearing_prices[(t, hid)]

        for hid in house_ids:
            rows.append(
                {
                    HouseRentLog.time: t,
                    HouseRentLog.house: hid,
                    HouseRentLog.rent: rent[hid],
                }
            )

        prev_vacant = curr_vacant

    return HouseRentLog.validate(pd.DataFrame(rows))
