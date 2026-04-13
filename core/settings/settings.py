from pydantic_settings import BaseSettings


class SimulationSettings(BaseSettings, frozen=True):
    """All simulation parameters, configurable via environment variables.

    Environment variable names match the field names in upper-case
    (e.g. ``N_RUNS=10``, ``LOWESS_SMOOTH=false``).
    """

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
    migration_interval: float = 10

    n_runs: int = 5
    n_steps: int = 1_000_000
    max_t: float = 500
    n_houses: int = 5
    smooth_lowess: bool = False
    time_resolution: float = 1.0
