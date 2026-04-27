"""`SilverStore` — lazy on-disk silver with in-memory cross-run aggregates.

The store replaces the monolithic :class:`~analytics.silver.model.Silver`
as the output of :class:`~analytics.silver.transformer.SilverTransformer`.
It keeps per-run raw silver frames on disk as feather shards and carries
only small pre-aggregated ``(run_id, group_cols, value)`` frames in
memory for gold to consume. This makes total memory independent of the
number of runs and the total event count.
"""
from __future__ import annotations

from collections.abc import Iterator
from pathlib import Path

import pandas as pd
from pydantic import BaseModel, ConfigDict, Field

_SHARD_FILE_PATTERN = "run_*.feather"


class SilverStore(BaseModel):
    """Handle to a streamed silver dataset.

    Attributes
    ----------
    base_dir
        Directory under which per-metric shard folders live:
        ``<base_dir>/<metric>/run_<id>.feather``.
    owner_names
        Union of landlord names across all simulated runs — needed by
        gold transforms that filter owners out of renter statistics.
    aggregates
        Dict of ``metric -> DataFrame`` where each DataFrame holds
        ``(run_id, *group_cols, value)`` rows, concatenated across runs.
        Size is ``O(n_runs * n_group_keys)`` — independent of total
        events.
    """

    model_config = ConfigDict(arbitrary_types_allowed=True)

    base_dir: Path
    owner_names: frozenset[str] = frozenset()
    aggregates: dict[str, pd.DataFrame] = Field(default_factory=dict)

    def iter_shards(self, metric: str) -> Iterator[pd.DataFrame]:
        """Yield per-run shards for *metric* in ``run_id`` order.

        Does not load all shards into memory at once — callers that can
        stream should iterate lazily. Use :meth:`load_all` only when the
        full concatenation fits in RAM (tests, debugging, visualization).
        """
        metric_dir = self.base_dir / metric
        if not metric_dir.exists():
            return
        for shard in sorted(metric_dir.glob(_SHARD_FILE_PATTERN)):
            yield pd.read_feather(shard)

    def load_all(self, metric: str) -> pd.DataFrame:
        """Concatenate every shard for *metric*.

        Convenience for exploratory use; memory usage scales with total
        silver row count for the metric, so this should not be called
        from the main gold pipeline.
        """
        frames = list(self.iter_shards(metric))
        if not frames:
            return pd.DataFrame()
        return pd.concat(frames, ignore_index=True)

    def get_aggregate(self, metric: str) -> pd.DataFrame:
        """Return the small per-run aggregate for *metric*.

        Returns an empty DataFrame if no aggregate was collected.
        """
        return self.aggregates.get(metric, pd.DataFrame())

    def has_metric(self, metric: str) -> bool:
        return (self.base_dir / metric).exists()
