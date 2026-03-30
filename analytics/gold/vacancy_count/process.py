from pandera.typing import DataFrame

from analytics.gold.vacancy_count.schema import VacancyCount
from analytics.silver.occupancy.schema import OccupancyLog


def build_vacancy_count(
    occupancy: DataFrame[OccupancyLog],
) -> DataFrame[VacancyCount]:
    return (
        occupancy
        .assign(is_vacant=lambda df: (df[OccupancyLog.occupant] == "vacant").astype(int))
        .groupby(OccupancyLog.time)["is_vacant"]
        .sum()
        .rename(VacancyCount.n_vacant)
        .reset_index()
        .pipe(VacancyCount.validate)
    )
