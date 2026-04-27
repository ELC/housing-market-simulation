"""Streaming `SilverTransformer` — per-run projections spilled to disk.

Consumes an iterable of :class:`~core.engine.RunResult` s lazily: each run
is projected, its raw frames spilled to feather shards, and its small gold
aggregates accumulated in memory. The run's in-memory state is dropped
before the next one is processed, so memory is bounded in both total events
and total runs.
"""
from __future__ import annotations

import tempfile
from collections.abc import Iterable
from pathlib import Path
from typing import TYPE_CHECKING

import pandas as pd
from tqdm.auto import tqdm

from analytics.bronze import build_fact_table
from analytics.silver import aggregates as agg
from analytics.silver.asking_rent import project_asking_rent
from analytics.silver.house_age import project_house_age
from analytics.silver.house_supply import project_house_supply
from analytics.silver.landlord_population import project_landlord_population
from analytics.silver.occupancy import project_occupancy
from analytics.silver.population import project_population
from analytics.silver.rent_duration import project_rent_duration
from analytics.silver.rent_payments import project_rent_payments
from analytics.silver.rent_to_income import project_rent_to_income
from analytics.silver.store import SilverStore
from analytics.silver.tenure import project_tenure
from analytics.silver.time_to_rent import project_time_to_rent
from analytics.silver.wealth import project_wealth
from core.events import AgentEntered, HouseBuilt, LandlordEntered

if TYPE_CHECKING:
    from core.engine import RunResult
    from core.settings import SimulationSettings


# Raw silver metrics, written as feather shards for debugging / lazy re-reads.
_RAW_METRICS: tuple[str, ...] = (
    "facts",
    "wealth",
    "occupancy",
    "rent_payments",
    "time_to_rent",
    "asking_rent",
    "rent_duration",
    "population",
    "house_supply",
    "landlord_population",
    "tenure",
    "rent_to_income",
    "house_age",
)


def _extract_agent_names(result: RunResult) -> dict[str, str]:
    return {
        evt.agent_id: evt.agent_name
        for evt in result.event_log
        if isinstance(evt, AgentEntered | LandlordEntered)
    }


def _extract_house_names(result: RunResult) -> dict[str, str]:
    return {
        evt.house_id: evt.house_name
        for evt in result.event_log
        if isinstance(evt, HouseBuilt)
    }


def _extract_owner_names(result: RunResult) -> set[str]:
    return {
        evt.agent_name
        for evt in result.event_log
        if isinstance(evt, LandlordEntered)
    }


def _snap_time(df: pd.DataFrame, resolution: float) -> pd.DataFrame:
    if df.empty or "time" not in df.columns:
        return df
    df = df.copy()
    df["time"] = (df["time"] / resolution).round() * resolution
    return df


def _shard_path(base: Path, metric: str, run_id: int) -> Path:
    metric_dir = base / metric
    metric_dir.mkdir(parents=True, exist_ok=True)
    return metric_dir / f"run_{run_id:05d}.feather"


def _spill(frame: pd.DataFrame, base: Path, metric: str, run_id: int) -> None:
    """Write *frame* as a feather shard. Pandas requires a default index."""
    frame.reset_index(drop=True).to_feather(_shard_path(base, metric, run_id))


class SilverTransformer:
    """Stream per-run silver to disk and pre-aggregate for gold.

    Parameters
    ----------
    time_resolution
        Snap event times to a grid of this resolution before projecting.
        Equivalent to the legacy ``_snap_time`` call-site behaviour.
    shard_dir
        Directory where per-run shards are written. Defaults to a fresh
        ``TemporaryDirectory`` that lives for the lifetime of this
        transformer instance.
    time_bin_duration
        Coarsening bin for time-based bootstraps
        (``time_to_rent_rolling``, ``rent_duration_rolling``).
    time_bin_tenure
        Coarsening bin for the tenure / demolition-age bootstraps.
    settings
        Simulation settings to borrow defaults from. When supplied, the
        per-call ``total`` progress-bar hint falls back to
        ``settings.n_runs`` if the caller does not pass one explicitly.
    """

    def __init__(
        self,
        time_resolution: float = 1.0,
        *,
        shard_dir: Path | None = None,
        time_bin_duration: float = 10.0,
        time_bin_tenure: float = 20.0,
        settings: SimulationSettings | None = None,
    ) -> None:
        self._time_resolution = time_resolution
        self._time_bin_duration = time_bin_duration
        self._time_bin_tenure = time_bin_tenure
        self._settings = settings
        if shard_dir is None:
            self._tmp = tempfile.TemporaryDirectory(prefix="silver-shards-")
            self._shard_dir = Path(self._tmp.name)
        else:
            self._tmp = None
            self._shard_dir = shard_dir
            self._shard_dir.mkdir(parents=True, exist_ok=True)

    @property
    def shard_dir(self) -> Path:
        return self._shard_dir

    def __call__(
        self,
        runs: Iterable[RunResult],
        *,
        total: int | None = None,
    ) -> SilverStore:
        res = self._time_resolution
        all_owner_names: set[str] = set()
        aggregate_buffers: dict[str, list[pd.DataFrame]] = {}

        if total is None and self._settings is not None:
            total = self._settings.n_runs
        iterator = tqdm(runs, desc="Silver", total=total, leave=True)
        for run_id, result in enumerate(iterator):
            facts = build_fact_table(result.event_log)
            owner_names = _extract_owner_names(result)
            all_owner_names |= owner_names
            agent_names = _extract_agent_names(result)
            house_names = _extract_house_names(result)

            wealth = _snap_time(
                project_wealth(facts, agent_names=agent_names), res,
            )
            occupancy = _snap_time(
                project_occupancy(facts, agent_names=agent_names, house_names=house_names), res,
            )
            rent_payments = _snap_time(project_rent_payments(facts), res)
            time_to_rent = _snap_time(
                project_time_to_rent(facts, house_names=house_names), res,
            )
            asking_rent = _snap_time(
                project_asking_rent(facts, result.settings, house_names=house_names), res,
            )
            rent_duration = _snap_time(
                project_rent_duration(facts, agent_names=agent_names, house_names=house_names), res,
            )
            population = _snap_time(project_population(facts), res)
            house_supply = _snap_time(project_house_supply(facts), res)
            landlord_population = _snap_time(project_landlord_population(facts), res)
            tenure = _snap_time(project_tenure(facts), res)
            rent_to_income = _snap_time(project_rent_to_income(facts, result.event_log), res)
            house_age = _snap_time(project_house_age(facts), res)

            raw_frames: dict[str, pd.DataFrame] = {
                "facts": facts,
                "wealth": wealth,
                "occupancy": occupancy,
                "rent_payments": rent_payments,
                "time_to_rent": time_to_rent,
                "asking_rent": asking_rent,
                "rent_duration": rent_duration,
                "population": population,
                "house_supply": house_supply,
                "landlord_population": landlord_population,
                "tenure": tenure,
                "rent_to_income": rent_to_income,
                "house_age": house_age,
            }
            for metric, frame in raw_frames.items():
                _spill(frame.assign(run_id=run_id), self._shard_dir, metric, run_id)

            # Drop result ASAP — the event log is the largest per-run payload
            # and only rent_to_income needs it (already consumed above).
            del result

            per_run_aggregates = self._compute_aggregates(
                run_id=run_id,
                owner_names=frozenset(owner_names),
                wealth=wealth,
                occupancy=occupancy,
                rent_payments=rent_payments,
                asking_rent=asking_rent,
                time_to_rent=time_to_rent,
                rent_duration=rent_duration,
                population=population,
                house_supply=house_supply,
                landlord_population=landlord_population,
                tenure=tenure,
                rent_to_income=rent_to_income,
                house_age=house_age,
                facts=facts,
            )
            for metric, frame in per_run_aggregates.items():
                aggregate_buffers.setdefault(metric, []).append(frame)

            del raw_frames, wealth, occupancy, rent_payments, asking_rent
            del time_to_rent, rent_duration, population, house_supply
            del landlord_population, tenure, rent_to_income, house_age, facts

        aggregates = {
            metric: (pd.concat(frames, ignore_index=True) if frames else pd.DataFrame())
            for metric, frames in aggregate_buffers.items()
        }
        return SilverStore(
            base_dir=self._shard_dir,
            owner_names=frozenset(all_owner_names),
            aggregates=aggregates,
        )

    def _compute_aggregates(
        self,
        *,
        run_id: int,
        owner_names: frozenset[str],
        wealth: pd.DataFrame,
        occupancy: pd.DataFrame,
        rent_payments: pd.DataFrame,
        asking_rent: pd.DataFrame,
        time_to_rent: pd.DataFrame,
        rent_duration: pd.DataFrame,
        population: pd.DataFrame,
        house_supply: pd.DataFrame,
        landlord_population: pd.DataFrame,
        tenure: pd.DataFrame,
        rent_to_income: pd.DataFrame,
        house_age: pd.DataFrame,
        facts: pd.DataFrame,
    ) -> dict[str, pd.DataFrame]:
        return {
            "housed_renter_wealth": agg.housed_renter_wealth_aggregate(
                wealth, occupancy, owner_names, run_id=run_id,
            ),
            "wealth_quartiles": agg.wealth_quartiles_aggregate(
                wealth, occupancy, owner_names, run_id=run_id,
            ),
            "wealth_spread": agg.wealth_spread_aggregate(
                wealth, occupancy, owner_names, run_id=run_id,
            ),
            "rent_comparison": agg.rent_comparison_aggregate(
                rent_payments, asking_rent, run_id=run_id,
            ),
            "time_to_rent_rolling": agg.time_to_rent_rolling_aggregate(
                time_to_rent, run_id=run_id, time_bin=self._time_bin_duration,
            ),
            "rent_duration_rolling": agg.rent_duration_rolling_aggregate(
                rent_duration, run_id=run_id, time_bin=self._time_bin_duration,
            ),
            "agent_population": agg.agent_population_aggregate(population, run_id=run_id),
            "house_supply": agg.house_supply_aggregate(house_supply, run_id=run_id),
            "landlord_population": agg.landlord_population_aggregate(
                landlord_population, run_id=run_id,
            ),
            "houses_per_landlord": agg.houses_per_landlord_aggregate(
                house_supply, landlord_population, run_id=run_id,
            ),
            "landlord_proportion": agg.landlord_proportion_aggregate(
                landlord_population, population, run_id=run_id,
            ),
            "tenure": agg.tenure_aggregate(
                tenure, run_id=run_id, time_bin=self._time_bin_tenure,
            ),
            "landlord_wealth": agg.landlord_wealth_aggregate(
                wealth, owner_names, run_id=run_id,
            ),
            "market_balance": agg.market_balance_aggregate(
                occupancy, population, owner_names, run_id=run_id,
            ),
            "rent_to_income": agg.rent_to_income_aggregate(rent_to_income, run_id=run_id),
            "house_age": agg.house_age_aggregate(house_age, run_id=run_id),
            "demolition_age": agg.demolition_age_aggregate(
                facts, run_id=run_id, time_bin=self._time_bin_tenure,
            ),
        }
