from pandera.typing import DataFrame

from analytics.gold.renter_wealth.schema import RenterWealth
from analytics.silver.wealth.schema import WealthLog


def build_renter_wealth(
    wealth: DataFrame[WealthLog],
    owner_names: frozenset[str] = frozenset(),
) -> DataFrame[RenterWealth]:
    mask = ~wealth[WealthLog.agent].isin(list(owner_names))
    return wealth[mask].reset_index(drop=True).pipe(RenterWealth.validate)
