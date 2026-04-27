import pandas as pd
from pandera.typing import DataFrame

from analytics.bronze.event_facts.schema import EventFact
from analytics.silver.landlord_population.schema import LandlordPopulationLog
from core.events import LandlordEntered, LandlordLeft


def project_landlord_population(
    facts: DataFrame[EventFact],
) -> DataFrame[LandlordPopulationLog]:
    event_times: list[float] = sorted(facts[EventFact.time].unique())

    entered = (
        facts.query(f"{EventFact.event_type} == '{LandlordEntered.event_name()}'")
        .groupby(EventFact.time)
        .size()
        .reindex(event_times, fill_value=0)
        .astype(int)
    )

    left = (
        facts.query(f"{EventFact.event_type} == '{LandlordLeft.event_name()}'")
        .groupby(EventFact.time)
        .size()
        .reindex(event_times, fill_value=0)
        .astype(int)
    )

    net = (entered - left).cumsum()

    df = pd.DataFrame({
        LandlordPopulationLog.time: event_times,
        LandlordPopulationLog.count: net.values,
        LandlordPopulationLog.entered: entered.values,
        LandlordPopulationLog.left: left.values,
    })

    return LandlordPopulationLog.validate(df)
