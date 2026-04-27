import pandas as pd
from pandera.typing import DataFrame

from analytics.bronze.event_facts.schema import EventFact
from analytics.silver.time_to_rent.schema import TimeToRent
from core.events import Evicted, RentExpired, RentStarted
from core.events.construction import HouseBuilt


def project_time_to_rent(
    facts: DataFrame[EventFact],
    house_names: dict[str, str] | None = None,
) -> DataFrame[TimeToRent]:
    if house_names is None:
        house_names = {}

    built = (
        facts.query(f"{EventFact.event_type} == '{HouseBuilt.event_name()}'")[[EventFact.time, EventFact.house_id]]
        .rename(columns={EventFact.time: "start"})
    )

    vacated = pd.concat(
        [
            facts.query(f"{EventFact.event_type} == '{Evicted.event_name()}'")[[EventFact.time, EventFact.house_id]],
            facts.query(f"{EventFact.event_type} == '{RentExpired.event_name()}'")[[EventFact.time, EventFact.house_id]],
        ],
        ignore_index=True,
    ).rename(columns={EventFact.time: "start"})

    vacancy_starts = (
        pd
        .concat([built, vacated], ignore_index=True)
        .sort_values("start")
        .assign(rank=lambda df: df.groupby(EventFact.house_id).cumcount())
    )

    vacancy_ends = (
        facts
        .query(f"{EventFact.event_type} == '{RentStarted.event_name()}'")[[EventFact.time, EventFact.house_id]]
        .rename(columns={EventFact.time: "end"})
        .sort_values("end")
        .assign(rank=lambda df: df.groupby(EventFact.house_id).cumcount())
    )

    merged = vacancy_starts.merge(vacancy_ends, on=[EventFact.house_id, "rank"])
    if merged.empty:
        return TimeToRent.validate(
            pd.DataFrame(columns=[TimeToRent.time, TimeToRent.house, TimeToRent.duration])
        )

    return (
        merged
        .assign(**{
            TimeToRent.time: lambda df: df["end"],
            TimeToRent.house: lambda df: df[EventFact.house_id].map(house_names).fillna(df[EventFact.house_id]),
            TimeToRent.duration: lambda df: df["end"] - df["start"],
        })[[TimeToRent.time, TimeToRent.house, TimeToRent.duration]]
        .pipe(TimeToRent.validate)
    )
