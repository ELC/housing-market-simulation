from pydantic_settings import BaseSettings


class SimulationSettings(BaseSettings, frozen=True):
    """All simulation parameters, configurable via environment variables.

    Environment variable names match the field names in upper-case
    (e.g. ``N_RUNS=10``, ``LOWESS_SMOOTH=false``).
    """

    base_rent: float = 10
    rent_sensitivity: float = 2.0
    tax_per_house: float = 5
    maintenance_base: float = 0.01
    maintenance_slope: float = 0.002
    min_construction_time: int = 5
    max_construction_time: int = 30
    min_lease_duration: int = 6
    max_lease_duration: int = 24
    wealth_tax_rate: float = 0.02
    min_taxable_wealth: float = 50.0
    vacancy_decay_rate: float = 0.05
    aging_interval: float = 12
    reconstruction_delay: int = 5
    migration_interval: float = 10
    landlord_seed_capital: float = 500.0
    construction_check_interval: float = 5.0
    landlord_migration_interval: float = 50.0
    construction_speed: float = 10.0

    n_runs: int = 20
    n_steps: int = 1_000_000
    max_t: float = 100
    n_houses: int = 5
    n_initial_landlords: int = 1
    smooth_lowess: bool = False
    time_resolution: float = 1.0
