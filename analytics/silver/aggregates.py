"""Per-run aggregate builders for each gold metric.

Given per-run silver frames (plus owner names for the run), each helper
emits the small ``(run_id, group_cols, value)`` frame that gold's
bootstrap only needs — skipping the wasteful "merge raw rows back into
the gold output" pattern of the legacy
:mod:`analytics.gold.*.process` implementations.

Every frame these helpers return is sized ``O(n_group_keys)`` and
therefore safe to accumulate in memory across all runs.
"""
from __future__ import annotations

import pandas as pd

from analytics.bronze.event_facts.schema import EventFact
from analytics.silver.asking_rent.schema import HouseRentLog
from analytics.silver.house_age.schema import HouseAgeLog
from analytics.silver.house_supply.schema import HouseSupplyLog
from analytics.silver.landlord_population.schema import LandlordPopulationLog
from analytics.silver.occupancy.schema import OccupancyLog
from analytics.silver.population.schema import PopulationLog
from analytics.silver.rent_duration.schema import RentDuration
from analytics.silver.rent_payments.schema import RentLog
from analytics.silver.rent_to_income.schema import RentToIncomeLog
from analytics.silver.tenure.schema import TenureLog
from analytics.silver.time_to_rent.schema import TimeToRent
from analytics.silver.wealth.schema import WealthLog
from core.events import HouseDemolished

_NON_OCCUPIED = frozenset({"vacant", "construction", "demolished"})
_OFF_MARKET = frozenset({"demolished", "construction"})


def _stamp(df: pd.DataFrame, run_id: int) -> pd.DataFrame:
    return df.assign(run_id=run_id)


def _group_mean(
    df: pd.DataFrame,
    *,
    run_id: int,
    group_cols: list[str],
    value_col: str,
) -> pd.DataFrame:
    """Return per-run-per-group mean of ``value_col`` with ``run_id`` column.

    Empty input yields an empty frame with the expected schema.
    """
    if df.empty:
        return pd.DataFrame(columns=["run_id", *group_cols, value_col])
    agg = (
        df.groupby(group_cols, sort=False, observed=True)[value_col]
        .mean()
        .reset_index()
    )
    agg.insert(0, "run_id", run_id)
    return agg


def _housed_renter_wealth(
    wealth: pd.DataFrame,
    occupancy: pd.DataFrame,
    owner_names: frozenset[str],
) -> pd.DataFrame:
    """Wealth rows for renters that are currently occupying a house."""
    housed = occupancy[~occupancy[OccupancyLog.occupant].isin(_NON_OCCUPIED)][
        [OccupancyLog.time, OccupancyLog.occupant]
    ].rename(columns={OccupancyLog.occupant: WealthLog.agent}).drop_duplicates()

    renter_wealth = wealth[~wealth[WealthLog.agent].isin(owner_names)]
    return renter_wealth.merge(housed, on=[WealthLog.time, WealthLog.agent])


def housed_renter_wealth_aggregate(
    wealth: pd.DataFrame,
    occupancy: pd.DataFrame,
    owner_names: frozenset[str],
    *,
    run_id: int,
) -> pd.DataFrame:
    """Mean money per time for renters that are currently housed."""
    return _group_mean(
        _housed_renter_wealth(wealth, occupancy, owner_names),
        run_id=run_id,
        group_cols=[WealthLog.time],
        value_col=WealthLog.money,
    )


def wealth_quartiles_aggregate(
    wealth: pd.DataFrame,
    occupancy: pd.DataFrame,
    owner_names: frozenset[str],
    *,
    run_id: int,
) -> pd.DataFrame:
    """Mean money per (time, quartile) — quartiles assigned within-run.

    Restricted to renters that are currently occupying a house, mirroring
    :func:`housed_renter_wealth_aggregate` so both metrics share the same
    population and time coverage.
    """
    renter = _housed_renter_wealth(wealth, occupancy, owner_names)
    if renter.empty:
        return pd.DataFrame(columns=["run_id", WealthLog.time, "quartile", WealthLog.money])
    pct_rank = renter.groupby(WealthLog.time, sort=False)[WealthLog.money].rank(
        method="first", pct=True,
    )
    labeled = renter.assign(
        quartile=pd.cut(
            pct_rank,
            bins=[0, 0.25, 0.5, 0.75, 1.0],
            labels=["Q1", "Q2", "Q3", "Q4"],
            include_lowest=True,
        ),
    )
    return _group_mean(
        labeled,
        run_id=run_id,
        group_cols=[WealthLog.time, "quartile"],
        value_col=WealthLog.money,
    )


def wealth_spread_aggregate(
    wealth: pd.DataFrame,
    occupancy: pd.DataFrame,
    owner_names: frozenset[str],
    *,
    run_id: int,
) -> pd.DataFrame:
    """Q4 - Q1 spread per time (paired by within-quartile money rank).

    Restricted to renters that are currently occupying a house so the
    spread is comparable to the housed-renter wealth and quartile series.
    """
    renter = _housed_renter_wealth(wealth, occupancy, owner_names)
    if renter.empty:
        return pd.DataFrame(columns=["run_id", WealthLog.time, "spread"])
    pct_rank = renter.groupby(WealthLog.time, sort=False)[WealthLog.money].rank(
        method="first", pct=True,
    )
    labeled = renter.assign(
        quartile=pd.cut(
            pct_rank,
            bins=[0, 0.25, 0.5, 0.75, 1.0],
            labels=["Q1", "Q2", "Q3", "Q4"],
            include_lowest=True,
        ),
    )

    sort_cols = [WealthLog.time, WealthLog.money]
    group_cols = [WealthLog.time]
    q1 = (
        labeled[labeled["quartile"] == "Q1"]
        .sort_values(sort_cols)
        .assign(rank=lambda df: df.groupby(group_cols).cumcount())
    )
    q4 = (
        labeled[labeled["quartile"] == "Q4"]
        .sort_values(sort_cols)
        .assign(rank=lambda df: df.groupby(group_cols).cumcount())
    )
    paired = q1.merge(q4, on=[WealthLog.time, "rank"], suffixes=("_q1", "_q4"))
    if paired.empty:
        return pd.DataFrame(columns=["run_id", WealthLog.time, "spread"])
    paired["spread"] = paired[f"{WealthLog.money}_q4"] - paired[f"{WealthLog.money}_q1"]
    return _group_mean(
        paired,
        run_id=run_id,
        group_cols=[WealthLog.time],
        value_col="spread",
    )


def rent_comparison_aggregate(
    rent_payments: pd.DataFrame,
    asking_rent: pd.DataFrame,
    *,
    run_id: int,
) -> pd.DataFrame:
    """Mean rent per (time, kind) — paid and asked concatenated."""
    paid = rent_payments[[RentLog.time, RentLog.amount]].assign(kind="paid")
    asked = asking_rent[[HouseRentLog.time, HouseRentLog.rent]].rename(
        columns={HouseRentLog.rent: RentLog.amount},
    ).assign(kind="asked")
    combined = pd.concat([paid, asked], ignore_index=True)
    return _group_mean(
        combined,
        run_id=run_id,
        group_cols=[RentLog.time, "kind"],
        value_col=RentLog.amount,
    )


def time_to_rent_rolling_aggregate(
    time_to_rent: pd.DataFrame,
    *,
    run_id: int,
    time_bin: float = 10.0,
) -> pd.DataFrame:
    """Mean vacancy duration per coarsened time bin."""
    if time_to_rent.empty:
        return pd.DataFrame(columns=["run_id", TimeToRent.time, TimeToRent.duration])
    binned = time_to_rent.assign(
        **{TimeToRent.time: (time_to_rent[TimeToRent.time] / time_bin).round() * time_bin},
    )
    return _group_mean(
        binned,
        run_id=run_id,
        group_cols=[TimeToRent.time],
        value_col=TimeToRent.duration,
    )


def rent_duration_rolling_aggregate(
    rent_duration: pd.DataFrame,
    *,
    run_id: int,
    time_bin: float = 10.0,
) -> pd.DataFrame:
    """Mean lease duration per coarsened time bin."""
    if rent_duration.empty:
        return pd.DataFrame(columns=["run_id", RentDuration.time, RentDuration.duration])
    binned = rent_duration.assign(
        **{RentDuration.time: (rent_duration[RentDuration.time] / time_bin).round() * time_bin},
    )
    return _group_mean(
        binned,
        run_id=run_id,
        group_cols=[RentDuration.time],
        value_col=RentDuration.duration,
    )


def agent_population_aggregate(population: pd.DataFrame, *, run_id: int) -> pd.DataFrame:
    return _group_mean(
        population,
        run_id=run_id,
        group_cols=[PopulationLog.time],
        value_col="count",
    )


def house_supply_aggregate(house_supply: pd.DataFrame, *, run_id: int) -> pd.DataFrame:
    return _group_mean(
        house_supply,
        run_id=run_id,
        group_cols=[HouseSupplyLog.time],
        value_col="count",
    )


def landlord_population_aggregate(landlord_pop: pd.DataFrame, *, run_id: int) -> pd.DataFrame:
    return _group_mean(
        landlord_pop,
        run_id=run_id,
        group_cols=[LandlordPopulationLog.time],
        value_col="count",
    )


def houses_per_landlord_aggregate(
    house_supply: pd.DataFrame,
    landlord_pop: pd.DataFrame,
    *,
    run_id: int,
) -> pd.DataFrame:
    hs = house_supply[[HouseSupplyLog.time, "count"]].rename(columns={"count": "n_houses"})
    lp = landlord_pop[[LandlordPopulationLog.time, "count"]].rename(columns={"count": "n_landlords"})
    merged = hs.merge(lp, on=HouseSupplyLog.time)
    merged["ratio"] = merged["n_houses"] / merged["n_landlords"].replace(0, float("nan"))
    merged = merged.dropna(subset=["ratio"])
    return _group_mean(
        merged,
        run_id=run_id,
        group_cols=[HouseSupplyLog.time],
        value_col="ratio",
    )


def landlord_proportion_aggregate(
    landlord_pop: pd.DataFrame,
    population: pd.DataFrame,
    *,
    run_id: int,
) -> pd.DataFrame:
    lp = landlord_pop[[LandlordPopulationLog.time, "count"]].rename(columns={"count": "n_landlords"})
    ap = population[[PopulationLog.time, "count"]].rename(columns={"count": "n_agents"})
    merged = lp.merge(ap, on=LandlordPopulationLog.time)
    total = merged["n_landlords"] + merged["n_agents"]
    merged["proportion"] = merged["n_landlords"] / total.replace(0, float("nan"))
    merged = merged.dropna(subset=["proportion"])
    return _group_mean(
        merged,
        run_id=run_id,
        group_cols=[LandlordPopulationLog.time],
        value_col="proportion",
    )


def tenure_aggregate(
    tenure: pd.DataFrame,
    *,
    run_id: int,
    time_bin: float = 20.0,
) -> pd.DataFrame:
    if tenure.empty:
        return pd.DataFrame(columns=["run_id", TenureLog.time, TenureLog.kind, TenureLog.duration])
    binned = tenure.assign(
        **{TenureLog.time: (tenure[TenureLog.time] / time_bin).round() * time_bin},
    )
    return _group_mean(
        binned,
        run_id=run_id,
        group_cols=[TenureLog.time, TenureLog.kind],
        value_col=TenureLog.duration,
    )


def landlord_wealth_aggregate(
    wealth: pd.DataFrame,
    owner_names: frozenset[str],
    *,
    run_id: int,
) -> pd.DataFrame:
    landlords = wealth[wealth[WealthLog.agent].isin(owner_names)]
    return _group_mean(
        landlords,
        run_id=run_id,
        group_cols=[WealthLog.time],
        value_col=WealthLog.money,
    )


def market_balance_aggregate(
    occupancy: pd.DataFrame,
    population: pd.DataFrame,
    owner_names: frozenset[str],
    *,
    run_id: int,
) -> pd.DataFrame:
    """Vacancy and homelessness rates per time.

    Mirrors the legacy gold implementation: vacancy is vacant / on-market
    houses; homelessness is homeless / renters.
    """
    on_market = occupancy[~occupancy[OccupancyLog.occupant].isin(_OFF_MARKET)]

    total_houses = on_market.groupby(OccupancyLog.time).size().rename("total_houses")
    vacant_houses = (
        on_market[on_market[OccupancyLog.occupant] == "vacant"]
        .groupby(OccupancyLog.time)
        .size()
        .rename("vacant_houses")
    )
    vacancy = pd.concat([total_houses, vacant_houses], axis=1).fillna(0)
    vacancy["rate"] = vacancy["vacant_houses"] / vacancy["total_houses"].replace(0, float("nan"))
    vacancy = vacancy.dropna(subset=["rate"]).reset_index()
    vacancy["metric"] = "vacancy"

    renter_pop = population[[PopulationLog.time, "count"]].rename(columns={"count": "n_renters"})
    housed_counts = (
        on_market[~on_market[OccupancyLog.occupant].isin({"vacant"} | owner_names)]
        .groupby(OccupancyLog.time)
        .size()
        .rename("n_housed")
        .reset_index()
    )
    homeless = renter_pop.merge(housed_counts, on=PopulationLog.time, how="left").fillna(0)
    homeless["n_homeless"] = (homeless["n_renters"] - homeless["n_housed"]).clip(lower=0)
    homeless["rate"] = homeless["n_homeless"] / homeless["n_renters"].replace(0, float("nan"))
    homeless = homeless.dropna(subset=["rate"])
    homeless["metric"] = "homelessness"

    combined = pd.concat(
        [
            vacancy[[OccupancyLog.time, "metric", "rate"]],
            homeless[[PopulationLog.time, "metric", "rate"]],
        ],
        ignore_index=True,
    )
    if combined.empty:
        return pd.DataFrame(columns=["run_id", OccupancyLog.time, "metric", "rate"])
    combined.insert(0, "run_id", run_id)
    return combined


def rent_to_income_aggregate(rent_to_income: pd.DataFrame, *, run_id: int) -> pd.DataFrame:
    return _group_mean(
        rent_to_income,
        run_id=run_id,
        group_cols=[RentToIncomeLog.time],
        value_col=RentToIncomeLog.ratio,
    )


def house_age_aggregate(house_age: pd.DataFrame, *, run_id: int) -> pd.DataFrame:
    return _group_mean(
        house_age,
        run_id=run_id,
        group_cols=[HouseAgeLog.time],
        value_col=HouseAgeLog.age,
    )


def demolition_age_aggregate(
    facts: pd.DataFrame,
    *,
    run_id: int,
    time_bin: float = 20.0,
) -> pd.DataFrame:
    demolished = facts[facts[EventFact.event_type] == HouseDemolished.event_name()]
    if demolished.empty:
        return pd.DataFrame(columns=["run_id", EventFact.time, "age"])
    binned = demolished.assign(
        **{
            EventFact.time: (demolished[EventFact.time] / time_bin).round() * time_bin,
            "age": demolished[EventFact.amount],
        },
    )
    return _group_mean(
        binned,
        run_id=run_id,
        group_cols=[EventFact.time],
        value_col="age",
    )
