import pandas as pd
from pandera.typing import DataFrame

from analytics.bronze.event_facts.schema import EventFact
from analytics.silver.house_age.schema import HouseAgeLog
from core.events import HouseAged, HouseBuilt, HouseDemolished


def project_house_age(facts: DataFrame[EventFact]) -> DataFrame[HouseAgeLog]:
    event_times: list[float] = sorted(facts[EventFact.time].unique())

    age_events: list[dict] = []

    built_rows = facts.query(f"{EventFact.event_type} == '{HouseBuilt.event_name()}'")
    for _, row in built_rows.iterrows():
        hid = row[EventFact.house_id]
        if hid:
            age_events.append({
                EventFact.time: row[EventFact.time],
                EventFact.house_id: hid,
                "age": 0.0,
            })

    aged_rows = facts.query(f"{EventFact.event_type} == '{HouseAged.event_name()}'")
    for _, row in aged_rows.iterrows():
        hid = row[EventFact.house_id]
        if hid:
            age_events.append({
                EventFact.time: row[EventFact.time],
                EventFact.house_id: hid,
                "age_increment": True,
            })

    demolished_rows = facts.query(f"{EventFact.event_type} == '{HouseDemolished.event_name()}'")
    demolished_map: dict[str, float] = {}
    for _, row in demolished_rows.iterrows():
        hid = row[EventFact.house_id]
        if hid:
            demolished_map[hid] = row[EventFact.time]

    all_house_ids = {evt[EventFact.house_id] for evt in age_events}

    if not all_house_ids:
        return HouseAgeLog.validate(
            pd.DataFrame(columns=[HouseAgeLog.time, HouseAgeLog.house, HouseAgeLog.age])
        )

    current_age: dict[str, float | None] = {hid: None for hid in all_house_ids}
    sorted_events = sorted(age_events, key=lambda e: e[EventFact.time])
    age_at_time: dict[str, dict[float, float]] = {hid: {} for hid in all_house_ids}

    for evt in sorted_events:
        hid = evt[EventFact.house_id]
        t = evt[EventFact.time]
        if "age" in evt:
            current_age[hid] = evt["age"]
        elif "age_increment" in evt and current_age[hid] is not None:
            current_age[hid] += 1.0  # type: ignore[operator]

        if current_age[hid] is not None:
            age_at_time[hid][t] = current_age[hid]  # type: ignore[assignment]

    rows: list[dict] = []
    for hid in all_house_ids:
        dem_t = demolished_map.get(hid, float("inf"))
        if not age_at_time[hid]:
            continue

        all_times = sorted(age_at_time[hid].keys())
        cur_age = None
        time_idx = 0
        for t in event_times:
            while time_idx < len(all_times) and all_times[time_idx] <= t:
                cur_age = age_at_time[hid][all_times[time_idx]]
                time_idx += 1
            if cur_age is not None and t <= dem_t:
                rows.append({
                    HouseAgeLog.time: t,
                    HouseAgeLog.house: hid,
                    HouseAgeLog.age: cur_age,
                })

    if not rows:
        return HouseAgeLog.validate(
            pd.DataFrame(columns=[HouseAgeLog.time, HouseAgeLog.house, HouseAgeLog.age])
        )

    return HouseAgeLog.validate(pd.DataFrame(rows))
