---
title: "Regional Checkpoints"
weight: 10
type: docs
aliases:
  - /ops/state/regional_checkpoints.html
---
<!--
Licensed to the Apache Software Foundation (ASF) under one
or more contributor license agreements.  See the NOTICE file
distributed with this work for additional information
regarding copyright ownership.  The ASF licenses this file
to you under the Apache License, Version 2.0 (the
"License"); you may not use this file except in compliance
with the License.  You may obtain a copy of the License at

  http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing,
software distributed under the License is distributed on an
"AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
KIND, either express or implied.  See the License for the
specific language governing permissions and limitations
under the License.
-->

# Regional Checkpoints

## Overview

Regional Checkpoint is an experimental feature that improves checkpoint availability for high-parallelism jobs, particularly those running on expensive GPU resources. When a partial region failure occurs during a checkpoint, the framework generates a logically complete Completed Checkpoint by combining the historical state of the failed regions with the current state of the healthy regions, rather than aborting the entire checkpoint.

This is especially useful for AI data processing workloads (e.g., model inference, multimodal processing) where unstable checkpoints can cause full-graph rollbacks that waste significant computational resources.

## How It Works

Flink's existing checkpoint mechanism requires all tasks to successfully acknowledge before a checkpoint can be completed. With Regional Checkpoint enabled:

1. When a task declines a checkpoint, the CheckpointCoordinator buffers the decline instead of immediately aborting.
2. After all tasks have responded (or the checkpoint timeout fires), the coordinator determines which Pipeline Regions have failed.
3. If the failure ratio is within the configured threshold, the coordinator assembles a combined checkpoint:
   - **Healthy regions**: use the current checkpoint state
   - **Failed regions**: use state from the last successful checkpoint
4. OperatorCoordinators that support Regional Checkpoint perform state correction to maintain consistency.
5. Healthy region tasks receive `notifyRegionalCheckpointComplete` (which by default delegates to `notifyCheckpointComplete`).
6. Failed region tasks receive `notifyRegionalCheckpointFallback` so they can clean up stale local state from the failed attempt.

### Per-Region Timeout Handling

When a task neither acknowledges nor declines within the checkpoint timeout, the timeout fires and the unacknowledged task's region is treated as failed (identical to the decline path). If the failure ratio is within `max-failure-ratio`, the coordinator proceeds with Regional Checkpoint. This counts toward `max-consecutive-failures`. The standard `execution.checkpointing.timeout` applies uniformly; no per-region timeout configuration is introduced.

## Prerequisites

Regional Checkpoint only works with **POINTWISE** topologies (forward, rescale connections). Topologies containing `keyBy`, `rebalance`, `hash`, or `shuffle` edges merge the entire graph into a single Pipeline Region, causing Regional Checkpoint to automatically fall back to standard checkpoint behavior.

## Configuration

| Key | Type | Default | Description |
|-----|------|---------|-------------|
| `execution.checkpointing.region.enabled` | Boolean | `false` | Global switch for Regional Checkpoint. |
| `execution.checkpointing.region.max-failure-ratio` | Double | `0.3` | Maximum ratio of failed regions allowed in a single checkpoint. If exceeded, the checkpoint aborts. |
| `execution.checkpointing.region.max-consecutive-failures` | Integer | `2` | Maximum number of consecutive checkpoints that may reference historical state. When the limit is reached, the current regional checkpoint still completes, but the **next** checkpoint is forced to be global (Tier 1). If the forced global checkpoint also fails, it aborts and the counter resets (Tier 2). |

### Example

```yaml
execution.checkpointing.region.enabled: true
execution.checkpointing.region.max-failure-ratio: 0.5
execution.checkpointing.region.max-consecutive-failures: 3
```

## Monitoring

### Metrics

| Metric | Description |
|--------|-------------|
| `regional_checkpoint_count` | Cumulative count of completed Regional Checkpoints (Counter). |

### REST API

The `/jobs/{jobid}/checkpoints/config` endpoint includes:
- `regional_checkpoint_enabled`
- `regional_max_failure_ratio`
- `regional_max_consecutive_failures`

The `/jobs/{jobid}/checkpoints` endpoint includes:
- `ref_checkpoint_id` per subtask — which historical checkpoint the subtask's state references
- `oldest_ref_checkpoint_id` per task/checkpoint — the oldest referenced checkpoint

### Web UI

The Checkpoint configuration page displays Regional Checkpoint settings. The Checkpoint detail page shows which subtasks reference historical checkpoints.

## Detecting Regional Checkpoints in User Code

A regional checkpoint is still a logically complete, committed checkpoint, so it is delivered through the
regular checkpoint-complete path. User code that needs to distinguish a global checkpoint from a
regional one (for example, an exactly-once sink that wants to know whether some subtasks fell back to
historical state) can observe this context via the `RegionalCheckpointInfo` provided by the framework.

`RegionalCheckpointInfo` is intentionally exposed at the user-facing API boundary (`flink-core`); the
runtime does not require any operator to consume it. Per FLIP-600, there are two notification methods
on `CheckpointListener`:

* **`notifyRegionalCheckpointComplete(long checkpointId, RegionalCheckpointInfo info)`** — called on
  **healthy-region tasks** when a checkpoint completes. The default implementation delegates to
  `notifyCheckpointComplete(long)`, so existing implementations are unaffected and only opt in when
  they override it.

  ```java
  @Override
  public void notifyRegionalCheckpointComplete(long checkpointId, RegionalCheckpointInfo info) {
      if (info.isGlobalCheckpoint()) {
          // all tasks acknowledged the current checkpoint
      } else {
          // some subtasks reused state from these historical checkpoints
          Set<Long> fallbackIds = info.getFallbackCheckpointIds();
      }
  }
  ```

* **`notifyRegionalCheckpointFallback(long checkpointId, long fallbackCheckpointId)`** — called on
  **failed-region tasks** when a regional checkpoint completes but this task's region fell back to
  a historical checkpoint. This is delivered via the same task-side checkpoint-complete RPC path so
  it survives task restarts and is applied after the task recovers. Implementations that maintain
  local checkpoint state (e.g. `TaskLocalStateStore`) should override this method to discard the
  stale local state of the failed checkpoint attempt. The default implementation is no-op.

  ```java
  @Override
  public void notifyRegionalCheckpointFallback(long checkpointId, long fallbackCheckpointId) {
      // clean up stale local state from the failed checkpoint attempt
  }
  ```

* **`OperatorCoordinator`** — coordinators in healthy regions receive
  `notifyRegionalCheckpointComplete` with the same `RegionalCheckpointInfo`; coordinators in failed
  regions receive `notifyRegionalCheckpointFallback` so they can clean up coordinator-side state
  from the failed attempt (in addition to the state correction already performed via
  `checkpointCoordinatorForRegionFallback`).

For a regional checkpoint, `RegionalCheckpointInfo#isGlobalCheckpoint()` returns `false` and
`getFallbackCheckpointIds()` lists the historical checkpoint ids whose state was reused. For a global
checkpoint it returns `true` with an empty fallback set.

## Limitations

1. **POINTWISE topologies only**: Jobs with ALL_TO_ALL edges (keyBy, rebalance, hash, shuffle) form a single Pipeline Region and cannot benefit from this feature. Regional Checkpoint automatically falls back to standard checkpoint behavior in this case.
2. **OperatorCoordinator support**: If an OperatorCoordinator in a failed region does not declare `supportsRegionCheckpoint() = true`, the checkpoint will abort. Flink's built-in `SourceCoordinator` supports Regional Checkpoint.
3. **Consecutive limit (two-tier semantics)**: To prevent unbounded historical checkpoint accumulation, the system forces a global checkpoint after `max-consecutive-failures` consecutive regional checkpoints. The two-tier behavior is:
   - **Tier 1**: When the consecutive count reaches the limit, the current regional checkpoint still completes, but the next checkpoint is forced to be global.
   - **Tier 2**: If the forced global checkpoint also fails (has declined/timeout tasks), it aborts and the counter resets. A successful global checkpoint (whether forced or not) resets the counter to 0.
4. **Savepoints**: Savepoint semantics are unchanged — they always require a full-graph snapshot regardless of this setting.
5. **Bounded sources**: When all sources are finished, the next checkpoint is forced to be global to ensure side effects (e.g., Kafka transactions) are committed. If this forced global checkpoint fails, the job retries rather than terminating with a partial snapshot.

## Checkpoint Cleanup

When Regional Checkpoint is enabled, the checkpoint cleaner ensures that historical checkpoints referenced via `ref_checkpoint_id` are not deleted. The effective retention set includes the most recent N checkpoints plus all transitively referenced historical checkpoints. Once a global checkpoint succeeds (no references), previously referenced checkpoints become eligible for cleanup.

## Troubleshooting

| Symptom | Possible Cause | Resolution |
|---------|---------------|------------|
| Regional Checkpoint never triggers | ALL_TO_ALL topology (single region) | Change to POINTWISE connections or accept standard behavior |
| Checkpoint aborts despite regional enabled | Failure ratio exceeds `max-failure-ratio` | Increase ratio threshold or investigate widespread failures |
| Checkpoint aborts after working initially | Consecutive limit reached | Increase `max-consecutive-failures` or investigate persistent region failures |
| OperatorCoordinator causes abort | Custom coordinator doesn't support regional | Implement `supportsRegionCheckpoint()` in your coordinator |
| Checkpoint storage growing | Referenced historical checkpoints kept | Expected behavior; tune `max-consecutive-failures` to limit depth |
