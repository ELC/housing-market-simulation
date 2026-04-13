from __future__ import annotations

import numpy as np
import pandas as pd


def downsample_evenly(
    df: pd.DataFrame,
    *,
    max_rows: int,
    sort_by: str | list[str] | None = None,
) -> pd.DataFrame:
    if max_rows <= 0:
        raise ValueError("max_rows must be > 0")
    if len(df) <= max_rows:
        return df
    if sort_by is not None:
        df = df.sort_values(sort_by, kind="stable")

    idx = np.linspace(0, len(df) - 1, num=max_rows, dtype=np.int64)
    # Ensure first/last are included even with rounding quirks.
    idx[0] = 0
    idx[-1] = len(df) - 1
    return df.iloc[idx]


def downsample_per_group(
    df: pd.DataFrame,
    *,
    group_col: str,
    max_rows_per_group: int,
    sort_by: str | list[str] | None = None,
) -> pd.DataFrame:
    if max_rows_per_group <= 0:
        raise ValueError("max_rows_per_group must be > 0")
    if group_col not in df.columns:
        raise KeyError(group_col)
    if sort_by is not None:
        df = df.sort_values(sort_by, kind="stable")
    # Fast path: if nothing is big, avoid groupby overhead.
    counts = df[group_col].value_counts(dropna=False)
    if int(counts.max()) <= max_rows_per_group:
        return df

    parts: list[pd.DataFrame] = []
    for _, g in df.groupby(group_col, sort=False):
        parts.append(downsample_evenly(g, max_rows=max_rows_per_group, sort_by=None))
    return pd.concat(parts, ignore_index=False).sort_index(kind="stable")


def evenly_spaced_tick_positions(n: int, max_ticks: int) -> list[int]:
    if n <= 0:
        return []
    if max_ticks <= 0:
        return []
    if n <= max_ticks:
        return list(range(n))
    step = (n - 1) / (max_ticks - 1)
    idx = [int(round(i * step)) for i in range(max_ticks)]
    # De-duplicate while preserving order.
    out: list[int] = []
    seen: set[int] = set()
    for i in idx:
        if i not in seen:
            out.append(i)
            seen.add(i)
    if 0 not in seen:
        out.insert(0, 0)
    if (n - 1) not in seen:
        out.append(n - 1)
    return out

