from pandera.typing import DataFrame

from analytics.gold.bootstrap import bootstrap_mean_ci_by_group
from analytics.gold.rent_duration_rolling.schema import RentDurationRolling
from analytics.silver.rent_duration.schema import RentDuration


def build_rent_duration_rolling(
    rd: DataFrame[RentDuration],
    *,
    time_bin: float = 10.0,
    n_boot: int = 500,
    ci: float = 95.0,
    seed: int = 0,
    max_resample_size: int = 500,
) -> DataFrame[RentDurationRolling]:
    df = rd.reset_index(drop=True).copy()
    # Coarsen event times into wider bins so each bin collects enough
    # events across runs for meaningful cross-run bootstrap statistics.
    df["time"] = (df["time"] / time_bin).round() * time_bin
    stats = bootstrap_mean_ci_by_group(
        df,
        group_cols=[RentDuration.time],
        value_col="duration",
        run_col="run_id",
        n_boot=n_boot,
        ci=ci,
        seed=seed,
        max_resample_size=max_resample_size,
    )
    return (
        df.merge(stats, on=[RentDuration.time], how="left")
        .drop(columns=["run_id", "n"], errors="ignore")
        .pipe(RentDurationRolling.validate)
    )
