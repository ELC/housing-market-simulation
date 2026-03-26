from pandera.typing import DataFrame

from analytics.gold.rent_payments.schema import RentPayments
from analytics.silver.rent_payments.schema import RentLog


def build_rent_payments(
    rent: DataFrame[RentLog],
) -> DataFrame[RentPayments]:
    """Pass-through: silver rent payments validated as gold."""
    return RentPayments.validate(rent.reset_index(drop=True))
