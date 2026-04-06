from pandera.typing import DataFrame

from analytics.gold.vacancy_count.schema import VacancyCount
from analytics.silver.occupancy.schema import OccupancyLog


def build_vacancy_count(
    occupancy: DataFrame[OccupancyLog],
) -> DataFrame[VacancyCount]:
    return (
        occupancy
        .assign(
            is_vacant=lambda df: (df[OccupancyLog.occupant] == "vacant").astype(int),
            is_construction=lambda df: (df[OccupancyLog.occupant] == "construction").astype(int),
            is_demolished=lambda df: (df[OccupancyLog.occupant] == "demolished").astype(int),
        )
        .groupby(OccupancyLog.time)[["is_vacant", "is_construction", "is_demolished"]]
        .sum()
        .rename(columns={
            "is_vacant": VacancyCount.n_vacant,
            "is_construction": VacancyCount.n_construction,
            "is_demolished": VacancyCount.n_demolished,
        })
        .reset_index()
        .pipe(VacancyCount.validate)
    )
