# Handoff: UnalignedCheckpointRescaleITCase data loss — PR #28517 (FLINK-38544 / FLIP-547)

For whoever owns the PR. **The PR introduces a rare, race-y in-flight data loss on unaligned-checkpoint
rescale, reproducible with the feature flag OFF. Needs root-cause + a regression test before merge.**

## TL;DR
- Failing test: `UnalignedCheckpointRescaleITCase.shouldRescaleUnalignedCheckpoint[downscale keyed_broadcast from 7 to 2]`.
- `[NUM_OUTPUTS = NUM_INPUTS] expected: X but was: Y`, `Y < X` (records lost). Flag
  `execution.checkpointing.during-recovery.enabled` is **off** → the legacy/always-on path.
- Confirmed PR-caused: pre-PR base **0 losses / 3141 runs** vs PR tip **≥4 losses**; p < 0.2%.
- **Not** a localized logic bug — every flag-off logic path matches base. Evidence points to a
  **timing/orchestration race** added by the recovery rewiring.

## Reproduce (NOT visible single-threaded)
Green single-threaded (121/121); needs CPU contention to widen the race. Ready harness beside this file:
`repro/eval_commit.sh` + `repro/narrow.py` (narrows to the single param; N parallel workers; per-worker
surefire `tempDir`; 180s per-run timeout; classifies LOSS `Y<X` vs DUP `Y>X`). Rate ≈ 0.1–0.3%/run, so
budget ~1000+ runs per data point. Ignore `LocalTransportException` crashes and `target/surefire`
temp-file errors — contention artifacts, not the bug.

## Evidence
| Tree | Runs | LOSS | DUP | Hangs / notes |
|------|-----:|-----:|----:|------|
| BASE `d1914c63c95` | 3141 | 0 | 0 | clean |
| c1 `46d4b743b90` (queue-split) | 1494 | 1 | 1 | 3 hangs — **separate path-A bug** |
| phase4 `abe52fed9fa` | 256 | 0 | 0 | **~80% crash, deterministic** (broken intermediate) |
| HEAD `d9fc48e9946` | ~2000 | ≥4 | 0 | few hangs |

### Ruled out (don't re-chase these)
- `LocalInputChannel.checkpointStarted` `emptyList()` — restoring the scan changed nothing (0 effect, 0 dups).
- Delivery drop race (`onRecoveredStateBuffer` on `isReleased`; `releaseAllResources` discarding pending) —
  instrumented, **0 drops** in 659 passes + the loss run.
- `NoSpillingHandler.recover` / `getBuffer` — **byte-identical to base**.
- `RemoteInputChannel.checkpointStarted` / `checkReadability` / `onBuffer` flag-off — **= base**.

### Why it's confounded for commit bisection
Non-monotonic: c1/phase1 use an always-on `LocalInputChannel.recoveredBuffers` migration (path A);
phase 2 replaces it with the `needsRecovery`-gated push path, so HEAD flag-off uses `RecoveredInputChannel`
(path B). c1's bug ≠ HEAD's bug. And phases 2–3 don't compile standalone; phase 4 is ~80%-crash broken —
so HEAD's path-B loss localizes only to the **phase 2–5** refactor.

## Top suspects for the HEAD bug (recovery orchestration timing)
1. **`SingleInputGate.convertRecoveredInputChannels`** — the 2-phase lock reorder ("convert +
   `releaseAllResources` outside the lock, then swap under the lock"). A swap-ordering race here matches
   all symptoms (rare, contention-sensitive, variable size, no per-path drop).
2. **`StreamTask` recovery wiring** — channel-IO executor, `recoverySetupCompleteFuture`, lazy
   `getRecoveryCheckpointTrigger`.

## Asks
1. Audit/instrument the recovery **orchestration** timing above (you'll find it faster than black-box did).
2. Add a **regression test** for recovered-buffer conservation under rescale + concurrent checkpoint
   (the queue-split commit shipped with none; a deterministic unit test would also catch c1's path-A bug).
3. Make the **intermediate commits compile/function** (phases 2–3 don't build; phase 4 crashes ~80%) — and
   investigate the recovery **hangs** (the `upstreamReady` drain-liveness / B1 concern).

Full investigation: `AI_CONCLUSION_FLINK-38544-dataloss.md` in the main checkout.
