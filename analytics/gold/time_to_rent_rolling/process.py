from pandera.typing import DataFrame

from analytics.gold.bootstrap import bootstrap_mean_ci_by_group
from analytics.gold.time_to_rent_rolling.schema import TimeToRentRolling
from analytics.silver.time_to_rent.schema import TimeToRent


def build_time_to_rent_rolling(
    ttr: DataFrame[TimeToRent],
    *,
    time_bin: float = 10.0,
    n_boot: int = 500,
    ci: float = 95.0,
    seed: int = 0,
    max_resample_size: int = 500,
) -> DataFrame[TimeToRentRolling]:
    df = ttr.reset_index(drop=True).copy()
    # Coarsen event times into wider bins so each bin collects enough
    # events across runs for meaningful cross-run bootstrap statistics.
    df["time"] = (df["time"] / time_bin).round() * time_bin
    stats = bootstrap_mean_ci_by_group(
        df,
        group_cols=[TimeToRent.time],
        value_col="duration",
        run_col="run_id",
        n_boot=n_boot,
        ci=ci,
        seed=seed,
        max_resample_size=max_resample_size,
    )
    return (
        df.merge(stats, on=[TimeToRent.time], how="left")
        .drop(columns=["run_id", "n"], errors="ignore")
        .pipe(TimeToRentRolling.validate)
    )
