from collections.abc import Sequence

import pandas as pd
from pandera.typing import DataFrame

from analytics.bronze.event_facts.schema import EventFact
from analytics.silver.rent_to_income.schema import RentToIncomeLog
from core.events import EventType, RentCollected
from core.events.income import AgentIncomeReceived


def _empty() -> DataFrame[RentToIncomeLog]:
    return RentToIncomeLog.validate(pd.DataFrame(
        columns=[RentToIncomeLog.time, RentToIncomeLog.agent,
                 RentToIncomeLog.rent, RentToIncomeLog.income, RentToIncomeLog.ratio],
    ))


def project_rent_to_income(
    facts: DataFrame[EventFact],
    event_log: Sequence[EventType],
) -> DataFrame[RentToIncomeLog]:
    type_idx = facts[EventFact.event_type]
    rents = (
        facts[type_idx == RentCollected.event_name()]
        [[EventFact.time, EventFact.agent_id, EventFact.amount]]
        .rename(columns={EventFact.agent_id: "agent", EventFact.amount: "rent"})
    )

    income_rows: list[dict] = []
    for evt in event_log:
        if isinstance(evt, AgentIncomeReceived) and evt.gross_income > 0:
            income_rows.append({
                "time": float(evt.time),
                "agent": evt.agent_id,
                "income": float(evt.gross_income),
            })
    if not income_rows or rents.empty:
        return _empty()

    incomes = pd.DataFrame(income_rows)

    # ``merge_asof`` per-agent: for each rent payment, attach the most recent
    # income (``time <= rent.time``) for the same agent in O((R+I) log).
    rents_sorted = rents.sort_values("time", kind="mergesort").reset_index(drop=True)
    incomes_sorted = incomes.sort_values("time", kind="mergesort").reset_index(drop=True)
    rents_sorted["time"] = rents_sorted["time"].astype(float)
    incomes_sorted["time"] = incomes_sorted["time"].astype(float)

    merged = pd.merge_asof(
        rents_sorted,
        incomes_sorted,
        on="time",
        by="agent",
        direction="backward",
    )

    merged = merged.dropna(subset=["income"])
    if merged.empty:
        return _empty()

    out = pd.DataFrame({
        RentToIncomeLog.time: merged["time"].to_numpy(),
        RentToIncomeLog.agent: merged["agent"].to_numpy(),
        RentToIncomeLog.rent: merged["rent"].to_numpy(),
        RentToIncomeLog.income: merged["income"].to_numpy(),
        RentToIncomeLog.ratio: merged["rent"].to_numpy() / merged["income"].to_numpy(),
    })
    return RentToIncomeLog.validate(out)
