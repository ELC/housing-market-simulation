import math
import random
from dataclasses import dataclass


@dataclass(frozen=True, slots=True)
class Stochastic:
    sigma: float = 0.1
    low: float | None = None
    high: float | None = None

    def sample(self, base: float) -> float:
        if base <= 0:
            return base
        mu = math.log(base) - self.sigma**2 / 2
        value = random.lognormvariate(mu, self.sigma)
        if self.low is not None:
            value = max(value, self.low)
        if self.high is not None:
            value = min(value, self.high)
        return value
