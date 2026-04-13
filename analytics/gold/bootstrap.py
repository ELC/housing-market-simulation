from __future__ import annotations

from typing import TYPE_CHECKING

import numpy as np
import pandas as pd
from pandera.typing import DataFrame

if TYPE_CHECKING:
    from collections.abc import Callable


def bootstrap_mean_ci(
    values: np.ndarray,
    *,
    stat_fn: Callable[[np.ndarray], float] = np.mean,
    n_boot: int = 500,
    ci: float = 95.0,
    seed: int = 0,
    max_resample_size: int = 500,
) -> tuple[float, float, float]:  # noqa: PLR0913
    """Bootstrap CI for a statistic.

    Returns `(statistic, ci_low, ci_high)` where the CI is the central `ci` percent interval.

    Notes
    - Uses an m-out-of-n bootstrap when `len(values) > max_resample_size` to keep runtime bounded.
    - Deterministic given `seed`.
    """
    x = np.asarray(values, dtype=float)
    x = x[np.isfinite(x)]
    if x.size == 0:
        return (float("nan"), float("nan"), float("nan"))

    stat = float(stat_fn(x))
    if n_boot <= 0 or x.size == 1:
        return (stat, stat, stat)

    alpha = (100.0 - float(ci)) / 200.0
    rng = np.random.default_rng(seed)

    n = int(x.size)
    m = min(n, int(max_resample_size))
    # Generate indices in one shot for speed.
    idx = rng.integers(0, n, size=(int(n_boot), m), dtype=np.int64)
    samples = x[idx]

    # Fast paths for common statistics.
    if stat_fn is np.mean:
        boot_stats = samples.mean(axis=1)
    elif stat_fn is np.median:
        boot_stats = np.median(samples, axis=1)
    else:
        boot_stats = np.apply_along_axis(lambda row: float(stat_fn(row)), 1, samples)

    lo = float(np.quantile(boot_stats, alpha))
    hi = float(np.quantile(boot_stats, 1.0 - alpha))
    return (stat, lo, hi)


def aggregate_across_runs(
    df: DataFrame,
    *,
    run_col: str,
    group_cols: list[str],
    value_col: str,
) -> DataFrame:
    """Pre-aggregate per ``(run, group)`` before bootstrapping across runs.

    Returns a dataframe with one row per ``(run, group)`` holding the
    within-run mean of *value_col*.
    """
    return (
        df.groupby([run_col, *group_cols], sort=True, observed=True)[value_col]
        .mean()
        .reset_index()
    )


def _bootstrap_groups(
    df: DataFrame,
    *,
    group_cols: list[str],
    value_col: str,
    stat_fn: Callable[[np.ndarray], float] = np.mean,
    stat_col: str = "mean",
    n_boot: int = 500,
    ci: float = 95.0,
    seed: int = 0,
    max_resample_size: int = 500,
) -> DataFrame:  # noqa: PLR0913
    """Core per-group bootstrap (no run-level awareness)."""
    rows: list[dict[str, object]] = []
    grouped = df.groupby(group_cols, sort=True, observed=True)[value_col]
    for group_key, series in grouped:
        key_tuple = group_key if isinstance(group_key, tuple) else (group_key,)
        stat, lo, hi = bootstrap_mean_ci(
            series.to_numpy(),
            stat_fn=stat_fn,
            n_boot=n_boot,
            ci=ci,
            seed=seed,
            max_resample_size=max_resample_size,
        )
        row: dict[str, object] = dict(zip(group_cols, key_tuple, strict=True))
        row.update({stat_col: stat, "ci_low": lo, "ci_high": hi, "n": int(series.size)})
        rows.append(row)
        seed += 1

    return pd.DataFrame(rows)


def bootstrap_mean_ci_by_group(
    df: DataFrame,
    *,
    group_cols: list[str],
    value_col: str,
    run_col: str | None = None,
    stat_fn: Callable[[np.ndarray], float] = np.mean,
    stat_col: str = "mean",
    n_boot: int = 500,
    ci: float = 95.0,
    seed: int = 0,
    max_resample_size: int = 500,
) -> DataFrame:  # noqa: PLR0913
    """Return one row per group with statistic/CI columns.

    When *run_col* is given, the data is first aggregated per
    ``(run, group)`` using :func:`aggregate_across_runs`, then the CI is
    bootstrapped **across runs** for each group.

    Returns a dataframe with ``group_cols``, ``stat_col``, ``ci_low``,
    ``ci_high``, and ``n``.
    """
    if not group_cols:
        msg = "group_cols must be non-empty"
        raise ValueError(msg)
    if value_col not in df.columns:
        raise KeyError(value_col)

    if run_col and run_col in df.columns:
        per_run = aggregate_across_runs(
            df, run_col=run_col, group_cols=group_cols, value_col=value_col,
        )
        return _bootstrap_groups(
            per_run,
            group_cols=group_cols,
            value_col=value_col,
            stat_fn=np.mean,
            stat_col=stat_col,
            n_boot=n_boot,
            ci=ci,
            seed=seed,
            max_resample_size=max_resample_size,
        )

    return _bootstrap_groups(
        df,
        group_cols=group_cols,
        value_col=value_col,
        stat_fn=stat_fn,
        stat_col=stat_col,
        n_boot=n_boot,
        ci=ci,
        seed=seed,
        max_resample_size=max_resample_size,
    )


def bootstrap_rolling_stat_ci(
    values: np.ndarray,
    *,
    window: int,
    stat_fn: Callable[[np.ndarray], float] = np.mean,
    n_boot: int = 500,
    ci: float = 95.0,
    seed: int = 0,
    max_resample_size: int = 500,
) -> tuple[np.ndarray, np.ndarray, np.ndarray]:  # noqa: PLR0913
    """Bootstrap CI for a rolling statistic.

    Returns `(stat, ci_low, ci_high)` arrays aligned to `values`, where each index `i`
    uses the window `values[max(0, i-window+1):i+1]`.
    """
    x = np.asarray(values, dtype=float)
    n = int(x.size)
    stat = np.full(n, np.nan, dtype=float)
    lo = np.full(n, np.nan, dtype=float)
    hi = np.full(n, np.nan, dtype=float)
    if n == 0:
        return stat, lo, hi
    if window <= 0:
        msg = "window must be > 0"
        raise ValueError(msg)

    alpha = (100.0 - float(ci)) / 200.0
    rng = np.random.default_rng(seed)

    def bootstrap_1d(arr: np.ndarray) -> tuple[float, float, float]:
        arr = arr[np.isfinite(arr)]
        if arr.size == 0:
            return float("nan"), float("nan"), float("nan")
        s = float(stat_fn(arr))
        if n_boot <= 0 or arr.size == 1:
            return s, s, s
        nn = int(arr.size)
        m = min(nn, int(max_resample_size))
        idx = rng.integers(0, nn, size=(int(n_boot), m), dtype=np.int64)
        samples = arr[idx]
        if stat_fn is np.mean:
            boot = samples.mean(axis=1)
        elif stat_fn is np.median:
            boot = np.median(samples, axis=1)
        else:
            boot = np.apply_along_axis(lambda row: float(stat_fn(row)), 1, samples)
        return s, float(np.quantile(boot, alpha)), float(np.quantile(boot, 1.0 - alpha))

    w = int(window)
    warm = min(n, w - 1)
    for i in range(warm):
        s, l, h = bootstrap_1d(x[: i + 1])
        stat[i] = s
        lo[i] = l
        hi[i] = h

    if n < w:
        return stat, lo, hi

    windows = np.lib.stride_tricks.sliding_window_view(x, w)  # shape: (n-w+1, w)
    if stat_fn is np.mean:
        stat_w = windows.mean(axis=1)
    elif stat_fn is np.median:
        stat_w = np.median(windows, axis=1)
    else:
        stat_w = np.apply_along_axis(lambda row: float(stat_fn(row)), 1, windows)
    stat[w - 1 :] = stat_w

    if n_boot <= 0:
        lo[w - 1 :] = stat_w
        hi[w - 1 :] = stat_w
        return stat, lo, hi

    m = min(w, int(max_resample_size))
    idx = rng.integers(0, w, size=(int(n_boot), m), dtype=np.int64)
    boot_stats = np.empty((windows.shape[0], int(n_boot)), dtype=np.float32)
    for b in range(int(n_boot)):
        sample = windows[:, idx[b]]
        if stat_fn is np.mean:
            boot_stats[:, b] = sample.mean(axis=1)
        elif stat_fn is np.median:
            boot_stats[:, b] = np.median(sample, axis=1)
        else:
            boot_stats[:, b] = np.apply_along_axis(lambda row: float(stat_fn(row)), 1, sample)

    lo[w - 1 :] = np.quantile(boot_stats, alpha, axis=1)
    hi[w - 1 :] = np.quantile(boot_stats, 1.0 - alpha, axis=1)
    return stat, lo, hi

