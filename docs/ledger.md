# ledger

Read when: verifying projected views, rebuilding views from the event ledger, comparing shadow views with active views, or switching views atomically.

`wacli ledger` manages the append-only event ledger and the checkpoint projectors that build the local views (`chats`, `messages`, `polls`, `calls`, FTS5 content, and the other materialized tables). Events are never mutated: erasure appends a scrub event, and state changes append new events.

## Commands

```bash
wacli ledger status
wacli ledger rebuild
wacli ledger verify
wacli ledger diff
wacli ledger promote [--force]
wacli ledger rollback
```

## Safe workflow

1. `wacli ledger rebuild` — reset the shadow views and replay the entire ledger from seq 0 with the current projector logic.
2. `wacli ledger verify` — run cross-view invariant checks against active views.
3. `wacli ledger diff` — compare active and rebuilt shadow views, table by table.
4. `wacli ledger promote` — atomically rename shadow views to active; the previous active views are retained as a backup set.
5. If needed, `wacli ledger rollback` — atomically restore the previous active views (only before a new rebuild).

## Notes

- `status`, `verify`, and `diff` are read-only and work under `--read-only` (or `WACLI_READONLY=1`). `rebuild`, `promote`, and `rollback` modify the store, require the store lock, and are refused in read-only mode.
- `promote` always requires the shadow invariant check to pass. `--force` only bypasses the diff policy (shadow differs from active); it never bypasses invariant violations.
- A second `promote` is refused while a backup set exists. Roll back first, or resolve the diffs and rebuild.
- The switch happens in a single transaction, so concurrent readers see either the old views or the new views, never a mix. A failure mid-switch leaves the active views exactly as they were.
- `status` reports head seq, per-view checkpoint seq and lag, retained raw/scrub counts, and whether shadow or backup sets are present.
- `verify` and `diff` exit non-zero when violations or differing rows are found; the report is still printed.
- Every diff row carries the responsible ledger `seq` and `event_id` when it can be attributed, so discrepancies can be traced back to the originating event.
- Use `--json` for machine-readable output.
