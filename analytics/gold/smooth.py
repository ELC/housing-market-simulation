from __future__ import annotations

from typing import TYPE_CHECKING

import numpy as np
import pandas as pd

if TYPE_CHECKING:
    from collections.abc import Iterable


try:
    from statsmodels.nonparametric.smoothers_lowess import lowess  # type: ignore[import-untyped]
except Exception as e:  # pragma: no cover
    lowess = None  # type: ignore[assignment]
    _LOWESS_IMPORT_ERROR = e
else:
    _LOWESS_IMPORT_ERROR = None


def lowess_smooth_xy(
    x: np.ndarray,
    y: np.ndarray,
    *,
    frac: float = 0.15,
    it: int = 0,
) -> np.ndarray:
    if lowess is None:  # pragma: no cover
        msg = f"statsmodels is required for LOWESS smoothing: {_LOWESS_IMPORT_ERROR}"
        raise RuntimeError(msg)

    xx = np.asarray(x, dtype=float)
    yy = np.asarray(y, dtype=float)
    if xx.shape != yy.shape:
        msg = "x and y must have the same shape"
        raise ValueError(msg)
    if xx.size < 3:
        return yy

    mask = np.isfinite(xx) & np.isfinite(yy)
    if mask.sum() < 3:
        return yy

    # LOWESS expects x sorted for stability; we sort and then unsort.
    order = np.argsort(xx[mask], kind="stable")
    x_sorted = xx[mask][order]
    y_sorted = yy[mask][order]

    yhat_sorted = lowess(y_sorted, x_sorted, frac=frac, it=it, return_sorted=False)
    yhat = yy.copy()
    yhat_masked = yhat[mask]
    yhat_masked[order] = yhat_sorted
    yhat[mask] = yhat_masked
    return yhat


def lowess_smooth_stats(
    stats: pd.DataFrame,
    *,
    x_col: str,
    group_cols: list[str] | None = None,
    mean_col: str = "mean",
    ci_low_col: str = "ci_low",
    ci_high_col: str = "ci_high",
    frac: float = 0.15,
    it: int = 0,
    smooth_band: bool = True,
) -> pd.DataFrame:
    out = stats.copy()
    cols: list[str] = [mean_col]
    if smooth_band:
        cols.extend([ci_low_col, ci_high_col])

    if group_cols:
        for _, g in out.groupby(group_cols, sort=False):
            idx = g.index
            x = g[x_col].to_numpy()
            for c in cols:
                out.loc[idx, c] = lowess_smooth_xy(x, g[c].to_numpy(), frac=frac, it=it)
    else:
        x = out[x_col].to_numpy()
        for c in cols:
            out[c] = lowess_smooth_xy(x, out[c].to_numpy(), frac=frac, it=it)

    if smooth_band:
        lo = out[ci_low_col].to_numpy(dtype=float)
        hi = out[ci_high_col].to_numpy(dtype=float)
        mean = out[mean_col].to_numpy(dtype=float)

        lo2 = np.minimum(lo, hi)
        hi2 = np.maximum(lo, hi)
        lo3 = np.minimum(lo2, mean)
        hi3 = np.maximum(hi2, mean)

        out[ci_low_col] = lo3
        out[ci_high_col] = hi3

    return out

