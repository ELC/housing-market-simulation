from core.base import FrozenModel


class SimulationSettings(FrozenModel):
    base_rent: float = 10
    rent_sensitivity: float = 2.0
    tax_per_house: float = 5
    maintenance_base: float = 1
    maintenance_slope: float = 0.1
    min_construction_time: int = 5
    max_construction_time: int = 30
    min_lease_duration: int = 6
    max_lease_duration: int = 24
    vacancy_decay_rate: float = 0.05
    aging_interval: float = 12
    reconstruction_delay: int = 5
    migration_interval: float = 50
