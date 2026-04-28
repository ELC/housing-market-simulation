"""Per-house age over time, reconstructed from the event log.

For each house we have:

- one ``HouseBuilt`` event setting age to 0 at construction completion;
- a sequence of ``HouseAged`` events, each incrementing age by 1;
- optionally one ``HouseDemolished`` event that ends the timeline.

We collapse those into one ``(time, house, age)`` row per age change, then
``merge_asof`` against the cartesian grid of ``(event_times × houses)`` to
forward-fill age between events. Finally, we mask out rows past each
house's demolition time.
"""
from __future__ import annotations

import numpy as np
import pandas as pd
from pandera.typing import DataFrame

from analytics.bronze.event_facts.schema import EventFact
from analytics.silver.house_age.schema import HouseAgeLog
from core.events import HouseAged, HouseBuilt, HouseDemolished


def _empty() -> DataFrame[HouseAgeLog]:
    return HouseAgeLog.validate(
        pd.DataFrame(columns=[HouseAgeLog.time, HouseAgeLog.house, HouseAgeLog.age]),
    )


def project_house_age(facts: DataFrame[EventFact]) -> DataFrame[HouseAgeLog]:
    type_idx = facts[EventFact.event_type]

    built = facts.loc[
        type_idx == HouseBuilt.event_name(),
        [EventFact.time, EventFact.house_id],
    ].copy()
    built = built.dropna(subset=[EventFact.house_id])
    if built.empty:
        return _empty()

    aged = facts.loc[
        type_idx == HouseAged.event_name(),
        [EventFact.time, EventFact.house_id],
    ].copy()
    aged = aged.dropna(subset=[EventFact.house_id])

    demolished = facts.loc[
        type_idx == HouseDemolished.event_name(),
        [EventFact.time, EventFact.house_id],
    ].dropna(subset=[EventFact.house_id])

    house_ids: list[str] = list(pd.unique(built[EventFact.house_id]))
    if not house_ids:
        return _empty()

    built["age"] = 0.0
    if not aged.empty:
        aged = aged.sort_values([EventFact.house_id, EventFact.time], kind="mergesort")
        aged["age"] = aged.groupby(EventFact.house_id, sort=False).cumcount() + 1
        aged["age"] = aged["age"].astype(float)
        age_updates = pd.concat([built, aged], ignore_index=True)
    else:
        age_updates = built

    age_updates = (
        age_updates
        .sort_values(EventFact.time, kind="mergesort")
        .reset_index(drop=True)
    )
    age_updates[EventFact.time] = age_updates[EventFact.time].astype(float)

    event_times = np.sort(facts[EventFact.time].astype(float).unique())
    grid = pd.DataFrame({
        EventFact.time: np.repeat(event_times, len(house_ids)),
        EventFact.house_id: np.tile(house_ids, len(event_times)),
    })
    grid[EventFact.time] = grid[EventFact.time].astype(float)
    grid = grid.sort_values(EventFact.time, kind="mergesort").reset_index(drop=True)

    merged = pd.merge_asof(
        grid,
        age_updates,
        on=EventFact.time,
        by=EventFact.house_id,
        direction="backward",
    )
    merged = merged.dropna(subset=["age"])
    if merged.empty:
        return _empty()

    if not demolished.empty:
        dem_map = dict(zip(
            demolished[EventFact.house_id].to_numpy(),
            demolished[EventFact.time].astype(float).to_numpy(),
            strict=False,
        ))
        dem_times = merged[EventFact.house_id].map(dem_map).to_numpy(dtype=float, na_value=np.inf)
        merged = merged[merged[EventFact.time].to_numpy() <= dem_times]

    if merged.empty:
        return _empty()

    out = pd.DataFrame({
        HouseAgeLog.time: merged[EventFact.time].to_numpy(),
        HouseAgeLog.house: merged[EventFact.house_id].to_numpy(),
        HouseAgeLog.age: merged["age"].to_numpy(),
    })
    return HouseAgeLog.validate(out)
