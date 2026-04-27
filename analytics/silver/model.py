"""Silver output model.

The legacy batched ``Silver`` pydantic model was replaced by the
streaming :class:`analytics.silver.store.SilverStore`, which spills
per-run silver to disk and carries only small cross-run aggregates in
memory. This module re-exports the new store under the legacy name so
existing imports keep working.
"""
from analytics.silver.store import SilverStore

Silver = SilverStore

__all__ = ["Silver", "SilverStore"]
