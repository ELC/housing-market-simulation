from collections.abc import Sequence

import pandas as pd
from pandera.typing import DataFrame

from analytics.bronze.event_facts.schema import EventFact
from analytics.silver.rent_to_income.schema import RentToIncomeLog
from core.events import EventType, RentCollected
from core.events.income import AgentIncomeReceived


def project_rent_to_income(
    facts: DataFrame[EventFact],
    event_log: Sequence[EventType],
) -> DataFrame[RentToIncomeLog]:
    rents = (
        facts.query(f"{EventFact.event_type} == '{RentCollected.event_name()}'")
        [[EventFact.time, EventFact.agent_id, EventFact.amount]]
        .rename(columns={EventFact.agent_id: "agent", EventFact.amount: "rent"})
    )

    income_rows: list[dict] = []
    for evt in event_log:
        if isinstance(evt, AgentIncomeReceived) and evt.gross_income > 0:
            income_rows.append({
                "time": evt.time,
                "agent": evt.agent_id,
                "income": evt.gross_income,
            })
    if not income_rows or rents.empty:
        return RentToIncomeLog.validate(pd.DataFrame(
            columns=[RentToIncomeLog.time, RentToIncomeLog.agent,
                     RentToIncomeLog.rent, RentToIncomeLog.income, RentToIncomeLog.ratio],
        ))

    incomes = pd.DataFrame(income_rows)
    incomes_at_time = (
        incomes.sort_values("time")
        .groupby("agent")
        .apply(lambda g: g.set_index("time")["income"], include_groups=False)
    )

    rows: list[dict] = []
    for _, rent_row in rents.iterrows():
        t = rent_row["time"]
        aid = rent_row["agent"]
        rent_val = rent_row["rent"]

        if aid in incomes_at_time.index:
            agent_incomes = incomes_at_time.loc[aid]
            valid = agent_incomes[agent_incomes.index <= t]
            if not valid.empty:
                gross = valid.iloc[-1]
                rows.append({
                    RentToIncomeLog.time: t,
                    RentToIncomeLog.agent: aid,
                    RentToIncomeLog.rent: rent_val,
                    RentToIncomeLog.income: gross,
                    RentToIncomeLog.ratio: rent_val / gross,
                })

    if not rows:
        return RentToIncomeLog.validate(pd.DataFrame(
            columns=[RentToIncomeLog.time, RentToIncomeLog.agent,
                     RentToIncomeLog.rent, RentToIncomeLog.income, RentToIncomeLog.ratio],
        ))

    return RentToIncomeLog.validate(pd.DataFrame(rows))
