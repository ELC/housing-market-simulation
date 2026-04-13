from analytics.gold.smooth import lowess_smooth_xy
from analytics.gold.smoother.base import Smoother
from analytics.types import AnyDataFrame


class LOWESSSmoother(Smoother):
    def __init__(self, frac: float = 0.15, it: int = 0) -> None:
        self._frac = frac
        self._it = it

    def smooth(self, df: AnyDataFrame, *, x_col: str, y_col: str) -> AnyDataFrame:
        out = df.copy()
        x = out[x_col].to_numpy(dtype=float)
        y = out[y_col].to_numpy(dtype=float)
        out[y_col] = lowess_smooth_xy(x, y, frac=self._frac, it=self._it)
        return out
