from abc import ABC, abstractmethod

from analytics.types import AnyDataFrame


class Smoother(ABC):
    @abstractmethod
    def smooth(self, df: AnyDataFrame, *, x_col: str, y_col: str) -> AnyDataFrame: ...
