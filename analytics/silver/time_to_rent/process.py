import operator

import pandas as pd
from pandera.typing import DataFrame

from analytics.bronze.event_facts.schema import EventFact
from analytics.silver.time_to_rent.schema import TimeToRent
from core.market import HousingMarket


def project_time_to_rent(
    facts: DataFrame[EventFact],
    initial_market: HousingMarket,
) -> DataFrame[TimeToRent]:
    """Pair each vacancy start (eviction or t=0) with the next rent_started for the same house."""
    initial_vacant = pd.DataFrame({
        "start": [0.0] * len(initial_market.houses),
        EventFact.house_id: [h.id for h in initial_market.houses],
    })

    vacancy_starts = (
        pd
        .concat(
            [
                initial_vacant,
                facts.query(f"{EventFact.event_type} == 'evicted'")[[EventFact.time, EventFact.house_id]].rename(
                    columns={EventFact.time: "start"}
                ),
            ],
            ignore_index=True,
        )
        .sort_values("start")
        .assign(rank=lambda df: df.groupby(EventFact.house_id).cumcount())
    )

    vacancy_ends = (
        facts
        .query(f"{EventFact.event_type} == 'rent_started'")[[EventFact.time, EventFact.house_id]]
        .rename(columns={EventFact.time: "end"})
        .sort_values("end")
        .assign(rank=lambda df: df.groupby(EventFact.house_id).cumcount())
    )

    return (
        vacancy_starts
        .merge(vacancy_ends, on=[EventFact.house_id, "rank"])
        .assign(**{
            TimeToRent.time: operator.itemgetter("end"),
            TimeToRent.house: operator.itemgetter(EventFact.house_id),
            TimeToRent.duration: lambda df: df["end"] - df["start"],
        })[[TimeToRent.time, TimeToRent.house, TimeToRent.duration]]
        .pipe(TimeToRent.validate)
    )
