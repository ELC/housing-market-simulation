from pandera.typing import DataFrame

from analytics.gold.renter_wealth.schema import RenterWealth
from analytics.silver.wealth.schema import WealthLog


def build_renter_wealth(
    wealth: DataFrame[WealthLog],
) -> DataFrame[RenterWealth]:
    """Filter wealth to renters only (exclude landlord)."""
    return (
        wealth.query(f"{WealthLog.agent} != 'landlord'")
        .reset_index(drop=True)
        .pipe(RenterWealth.validate)
    )
