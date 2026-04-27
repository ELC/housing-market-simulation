# TODO

## Architecture

### Introduce a `FactEvent` base class for receipt-only events

Several events in `core/events/` exist purely as log entries: their `apply`
is a no-op because the state mutation already happened atomically inside
the trigger event that emitted them (e.g. `WealthTaxDeducted` is emitted
by `WealthTaxDue.apply`). Today each one re-implements an empty `apply`,
which is incidental rather than intentional.

Promote the pattern into the type system:

```python
class FactEvent(Event):
    """Marker base for receipt-only events; ``apply`` is a no-op."""

    def apply(self, market, context):
        return market, context, []
```

Then refactor existing receipt events to inherit from it:

- `core/events/tax.py::WealthTaxDeducted`
- audit other events for the same shape (anything whose `apply` returns
  `market, context, []` unchanged is a candidate).

Benefits:

- The "receipt vs trigger" distinction becomes a property of the event
  kind, not an accident of an empty method body.
- Bronze/silver layers can rely on the marker (e.g. `isinstance(e, FactEvent)`)
  to special-case logging.
- Documents the H1/H4 invariant: a `FactEvent`'s `amount` is guaranteed
  to equal what was actually mutated, because the mutation happened in
  the trigger's `apply` together with emitting this receipt.

Out of scope: making the `EventQueue` enforce atomicity of `[trigger,
receipt]` pairs at the same `t` (would let us move mutation out of
triggers, but that's a separate, bigger change).
