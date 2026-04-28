"""Per-house asking-rent log.

The algorithm is intrinsically sequential — at each auction, the rent
for a house that *stayed vacant* decays, and the rent for a house that
*just got occupied* jumps to the clearing price — so it cannot be fully
vectorised. We do, however, push every per-time-step inner operation
down to numpy: occupancy and on-market state become 2-D boolean masks
indexed as ``[time_idx, house_idx]``, so the time loop becomes O(T)
instead of O(T·H) scalar pandas lookups.
"""
from __future__ import annotations

import math

import numpy as np
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


def _empty() -> DataFrame[HouseRentLog]:
    return HouseRentLog.validate(
        pd.DataFrame(columns=[HouseRentLog.time, HouseRentLog.house, HouseRentLog.rent]),
    )


def project_asking_rent(
    facts: DataFrame[EventFact],
    settings: SimulationSettings,
    house_names: dict[str, str] | None = None,
) -> DataFrame[HouseRentLog]:
    decay: float = math.exp(-settings.vacancy_decay_rate)
    if house_names is None:
        house_names = {}

    et = EventFact.event_type
    eft = EventFact.time
    eh = EventFact.house_id
    ea = EventFact.agent_id
    eamt = EventFact.amount

    type_idx = facts[et]
    built_rows = facts[type_idx == HouseBuilt.event_name()]
    starts = facts[type_idx == RentStarted.event_name()][[eft, eh, ea]]
    evicts = facts[type_idx == Evicted.event_name()][[eft, eh]]
    expired = facts[type_idx == RentExpired.event_name()][[eft, eh]]
    vacated = pd.concat([evicts, expired], ignore_index=True)
    demolished = facts[type_idx == HouseDemolished.event_name()][[eft, eh]]

    rent_init: dict[str, float] = {}
    built_h = built_rows[eh].to_numpy()
    built_a = built_rows[eamt].to_numpy()
    for hid, amt in zip(built_h, built_a, strict=False):
        if hid and hid not in rent_init:
            house_names.setdefault(hid, hid)
            rent_init[hid] = float(amt) / max(1, settings.max_construction_time)

    built_occ = built_rows[[eft, eh]].assign(occupant="construction")
    starts_occ = starts.rename(columns={ea: "occupant"})
    vacated_occ = vacated.assign(occupant="vacant")
    demolished_occ = demolished.assign(occupant="demolished")

    occ_events = pd.concat(
        [starts_occ, vacated_occ, demolished_occ, built_occ], ignore_index=True,
    ).sort_values(eft, kind="mergesort")

    if occ_events.empty:
        return _empty()

    event_times = np.sort(facts[eft].unique())

    occ_wide = (
        occ_events
        .groupby([eh, eft])["occupant"]
        .last()
        .unstack(eh)
        .reindex(event_times)
        .ffill()
    )

    house_ids: list[str] = list(occ_wide.columns)
    h_index = {h: i for i, h in enumerate(house_ids)}
    H = len(house_ids)
    T = len(event_times)
    if H == 0 or T == 0:
        return _empty()

    occ_arr = occ_wide.to_numpy()
    nan_mask = pd.isna(occ_arr)
    vacant_mask = (occ_arr == "vacant")
    construction_mask = (occ_arr == "construction")
    demolished_mask = (occ_arr == "demolished")
    on_market_mask = ~(construction_mask | demolished_mask | nan_mask)

    auction_set = set(facts.loc[type_idx == AuctionClear.event_name(), eft].to_numpy().tolist())
    auction_at_t = np.fromiter(
        (float(t) in auction_set for t in event_times), dtype=bool, count=T,
    )

    time_to_idx = {float(t): i for i, t in enumerate(event_times)}

    evict_mask = np.zeros((T, H), dtype=bool)
    if not vacated.empty:
        for t, hid in zip(vacated[eft].to_numpy(), vacated[eh].to_numpy(), strict=False):
            ti = time_to_idx.get(float(t))
            hi = h_index.get(hid)
            if ti is not None and hi is not None:
                evict_mask[ti, hi] = True

    rent_collected = facts[type_idx == RentCollected.event_name()]
    clearing = starts[[eft, eh]].merge(rent_collected, on=[eft, eh])
    clearing_arr = np.full((T, H), np.nan)
    if not clearing.empty:
        for t, hid, amt in zip(
            clearing[eft].to_numpy(),
            clearing[eh].to_numpy(),
            clearing[eamt].to_numpy(),
            strict=False,
        ):
            ti = time_to_idx.get(float(t))
            hi = h_index.get(hid)
            if ti is not None and hi is not None:
                clearing_arr[ti, hi] = float(amt)

    rent_vec = np.zeros(H)
    for hid, val in rent_init.items():
        if hid in h_index:
            rent_vec[h_index[hid]] = val

    rent_arr = np.zeros((T, H))
    prev_vac = np.zeros(H, dtype=bool)

    for ti in range(T):
        curr_vac = vacant_mask[ti]
        if auction_at_t[ti]:
            was_vacant = prev_vac | evict_mask[ti]
            decay_mask = was_vacant & curr_vac
            if decay_mask.any():
                rent_vec[decay_mask] *= decay
            newly_occ = was_vacant & ~curr_vac
            if newly_occ.any():
                cp = clearing_arr[ti]
                cp_valid = newly_occ & ~np.isnan(cp)
                if cp_valid.any():
                    rent_vec[cp_valid] = cp[cp_valid]

        rent_arr[ti] = rent_vec
        prev_vac = curr_vac

    rows_t, rows_h = np.where(on_market_mask)
    if rows_t.size == 0:
        return _empty()

    times_out = event_times[rows_t]
    house_arr = np.array(house_ids, dtype=object)
    house_ids_out = house_arr[rows_h]
    rents_out = rent_arr[rows_t, rows_h]
    names_out = np.array([house_names.get(h, h) for h in house_ids_out], dtype=object)

    out = pd.DataFrame({
        HouseRentLog.time: times_out,
        HouseRentLog.house: names_out,
        HouseRentLog.rent: rents_out,
    })
    return HouseRentLog.validate(out)
