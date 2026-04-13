from __future__ import annotations

from typing import TYPE_CHECKING

import pandas as pd

from analytics.bronze import build_fact_table
from analytics.silver.asking_rent import project_asking_rent
from analytics.silver.model import Silver, SilverCollectors
from analytics.silver.occupancy import project_occupancy
from analytics.silver.population import project_population
from analytics.silver.rent_duration import project_rent_duration
from analytics.silver.rent_payments import project_rent_payments
from analytics.silver.time_to_rent import project_time_to_rent
from analytics.silver.wealth import project_wealth
from core.entity import Agent
from core.events import AgentEntered

if TYPE_CHECKING:
    from collections.abc import Sequence

    from core.engine import RunResult


def _extract_agent_names(result: RunResult) -> dict[str, str]:
    names: dict[str, str] = {
        a.id: a.name for a in result.market.entities_of_type(Agent)
    }
    for evt in result.event_log:
        if isinstance(evt, AgentEntered):
            names[evt.agent_id] = evt.agent_name
    return names


def _snap_time(df: pd.DataFrame, resolution: float) -> pd.DataFrame:
    """Round the ``time`` column to the nearest multiple of *resolution*.

    Returns
    -------
    pd.DataFrame
        A copy of *df* with snapped ``time`` values.
    """
    df = df.copy()
    df["time"] = (df["time"] / resolution).round() * resolution
    return df


class SilverTransformer:
    def __init__(self, time_resolution: float = 1.0) -> None:
        self._time_resolution = time_resolution

    def __call__(self, runs: Sequence[RunResult]) -> Silver:
        collectors: SilverCollectors = SilverCollectors(
            wealth=[],
            occupancy=[],
            rent_payments=[],
            time_to_rent=[],
            asking_rent=[],
            rent_duration=[],
            population=[],
        )
        landlord_names: set[str] = set()
        res = self._time_resolution

        for run_id, result in enumerate(runs):
            facts = build_fact_table(result.event_log, result.market)
            agent_names = _extract_agent_names(result)

            landlord_names.add(result.landlord_name)

            tag = {"run_id": run_id}
            collectors["wealth"].append(
                _snap_time(project_wealth(facts, result.market, agent_names=agent_names), res).assign(**tag),
            )
            collectors["occupancy"].append(
                _snap_time(project_occupancy(facts, result.market, agent_names=agent_names), res).assign(**tag),
            )
            collectors["rent_payments"].append(
                _snap_time(project_rent_payments(facts), res).assign(**tag),
            )
            collectors["time_to_rent"].append(
                _snap_time(project_time_to_rent(facts, result.market), res).assign(**tag),
            )
            collectors["asking_rent"].append(
                _snap_time(project_asking_rent(facts, result.market), res).assign(**tag),
            )
            collectors["rent_duration"].append(
                _snap_time(project_rent_duration(facts, result.market, agent_names=agent_names), res).assign(**tag),
            )
            collectors["population"].append(
                _snap_time(project_population(facts, result.market), res).assign(**tag),
            )

        return Silver(
            wealth=pd.concat(collectors["wealth"], ignore_index=True),
            occupancy=pd.concat(collectors["occupancy"], ignore_index=True),
            rent_payments=pd.concat(collectors["rent_payments"], ignore_index=True),
            time_to_rent=pd.concat(collectors["time_to_rent"], ignore_index=True),
            asking_rent=pd.concat(collectors["asking_rent"], ignore_index=True),
            rent_duration=pd.concat(collectors["rent_duration"], ignore_index=True),
            owner_names=frozenset(landlord_names),
            population=pd.concat(collectors["population"], ignore_index=True),
        )
