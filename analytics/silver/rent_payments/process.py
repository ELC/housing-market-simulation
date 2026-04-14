from pandera.typing import DataFrame

from analytics.bronze.event_facts.schema import EventFact
from analytics.silver.rent_payments.schema import RentLog
from core.events import RentCollected


def project_rent_payments(
    facts: DataFrame[EventFact],
) -> DataFrame[RentLog]:
    return (
        facts
        .query(f"{EventFact.event_type} == '{RentCollected.event_name()}'")[
            [EventFact.time, EventFact.house_id, EventFact.agent_id, EventFact.amount]
        ]
        .rename(
            columns={
                EventFact.house_id: RentLog.house,
                EventFact.agent_id: RentLog.tenant,
            }
        )
        .reset_index(drop=True)
        .pipe(RentLog.validate)
    )
