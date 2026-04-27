import pandas as pd
from pandera.typing import DataFrame

from analytics.bronze.event_facts.schema import EventFact
from analytics.silver.house_supply.schema import HouseSupplyLog
from core.events import HouseBuilt, HouseDemolished


def project_house_supply(
    facts: DataFrame[EventFact],
) -> DataFrame[HouseSupplyLog]:
    event_times: list[float] = sorted(facts[EventFact.time].unique())

    built = (
        facts.query(f"{EventFact.event_type} == '{HouseBuilt.event_name()}'")
        .groupby(EventFact.time)
        .size()
        .reindex(event_times, fill_value=0)
        .astype(int)
    )

    demolished = (
        facts.query(f"{EventFact.event_type} == '{HouseDemolished.event_name()}'")
        .groupby(EventFact.time)
        .size()
        .reindex(event_times, fill_value=0)
        .astype(int)
    )

    net = (built - demolished).cumsum()

    df = pd.DataFrame({
        HouseSupplyLog.time: event_times,
        HouseSupplyLog.count: net.values,
        HouseSupplyLog.built: built.values,
        HouseSupplyLog.demolished: demolished.values,
    })

    return HouseSupplyLog.validate(df)
