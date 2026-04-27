import pandas as pd
from pandera.typing import DataFrame

from analytics.bronze.event_facts.schema import EventFact
from analytics.silver.tenure.schema import TenureLog
from core.events import AgentEntered, AgentLeft, LandlordEntered, LandlordLeft


def project_tenure(
    facts: DataFrame[EventFact],
) -> DataFrame[TenureLog]:
    agent_entered = facts.query(
        f"{EventFact.event_type} == '{AgentEntered.event_name()}'"
    )[[EventFact.time, EventFact.agent_id]].rename(columns={EventFact.time: "enter_time"})

    agent_left = facts.query(
        f"{EventFact.event_type} == '{AgentLeft.event_name()}'"
    )[[EventFact.time, EventFact.agent_id]].rename(columns={EventFact.time: "leave_time"})

    ll_entered = facts.query(
        f"{EventFact.event_type} == '{LandlordEntered.event_name()}'"
    )[[EventFact.time, EventFact.agent_id]].rename(columns={EventFact.time: "enter_time"})

    ll_left = facts.query(
        f"{EventFact.event_type} == '{LandlordLeft.event_name()}'"
    )[[EventFact.time, EventFact.agent_id]].rename(columns={EventFact.time: "leave_time"})

    rows: list[dict] = []

    for label, enters, leaves in [("agent", agent_entered, agent_left), ("landlord", ll_entered, ll_left)]:
        merged = enters.merge(leaves, on=EventFact.agent_id, how="inner")
        if not merged.empty:
            merged["duration"] = merged["leave_time"] - merged["enter_time"]
            for _, row in merged.iterrows():
                rows.append({
                    TenureLog.time: row["leave_time"],
                    TenureLog.kind: label,
                    TenureLog.duration: row["duration"],
                })

    if not rows:
        return TenureLog.validate(
            pd.DataFrame(columns=[TenureLog.time, TenureLog.kind, TenureLog.duration])
        )

    return TenureLog.validate(pd.DataFrame(rows))
