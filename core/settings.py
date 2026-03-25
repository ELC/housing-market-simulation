from core.base import FrozenModel


class SimulationSettings(FrozenModel):
    base_rent: float = 10
    rent_sensitivity: float = 2.0
    tax_per_house: float = 5
    maintenance_base: float = 1
    maintenance_slope: float = 0.1
    construction_time: int = 5
    vacancy_decay_rate: float = 0.05
