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

# flink-table-runtime

Contains classes required by TaskManagers for execution of table programs. Implements runtime operators, built-in functions, and code generation support. Bundles janino (Java compiler for code generation) and flink-shaded-jsonpath.

## Key Directory Structure

- `functions/scalar/` — Scalar function implementations (47+)
- `functions/aggregate/` — Aggregate function implementations
- `functions/table/` — Table function implementations
- `functions/ptf/` — Process table function implementations
- `functions/ml/` — Machine learning function implementations
- `operators/` — Runtime operators organized by type:
  - `join/` (hash, sort-merge, lookup, temporal, interval, delta, adaptive, stream/multi-join)
  - `aggregate/` (group, window)
  - `window/` (TVF windows, group windows)
  - `deduplicate/`, `rank/`, `sort/`
  - `sink/`, `source/`
  - `correlate/` (including `async/` for async table functions)
  - `calc/`, `match/`, `over/`, `process/`, `ml/`, `search/`

## Common Change Patterns

### Adding a built-in function

Base classes by function type:

- **Scalar:** Extend `BuiltInScalarFunction` in `functions/scalar/`
- **Table:** Extend `BuiltInTableFunction` in `functions/table/`
- **Aggregate:** Extend `BuiltInAggregateFunction` in `functions/aggregate/`
- **Process Table Function:** Extend `BuiltInProcessTableFunction` in `functions/ptf/`

All are constructed from `BuiltInFunctionDefinition#specialize(SpecializedContext)` and work on internal data structures by default.

Some functions also require custom code generators in the planner (e.g., `JsonCallGen.scala` for JSON functions). Simple scalar functions typically don't need planner changes; the planner handles them uniformly through the function definition.

### Adding a runtime operator

- **1 or 2 inputs:** Extend `TableStreamOperator<RowData>` (which extends `AbstractStreamOperator<OUT>`) and implement `OneInputStreamOperator<RowData, RowData>` or `TwoInputStreamOperator<RowData, RowData, RowData>`
- **3+ inputs:** Extend `AbstractStreamOperatorV2` and implement `MultipleInputStreamOperator` (see `StreamingMultiJoinOperator`)
- `TableStreamOperator` provides watermark tracking (`currentWatermark`), memory size computation, and a `ContextImpl` for timer services

### Async operators and runners

- **Key-ordered async execution:** `operators/join/lookup/keyordered/` contains async execution controller infrastructure (`AecRecord`, `Epoch`, `EpochManager`, `KeyAccountingUnit`, `RecordsBuffer`) for ordering guarantees in async lookup joins
- **Async correlate:** `operators/correlate/async/` for async table function support
- **Runner abstraction:** `AbstractFunctionRunner` and `AbstractAsyncFunctionRunner` provide base classes for code-generated function invocations (used by lookup join, ML predict, vector search runners)

### State serializer migrations

- When modifying state serializers, create a `TypeSerializerSnapshot` with version bumping
- Migration test resources follow naming: `migration-flink-<version>-<backend>-<variant>-snapshot`
  - Rescaling tests verify state redistribution across parallelism changes (see `SinkUpsertMaterializerMigrationTest`, `SinkUpsertMaterializerRescalingTest`)

      ## Changelog Processing

Runtime operators carry the `RowKind` contract that the planner's trait inference assumed. Get it wrong and state diverges from intent: the operator may double-count, leak rows, or emit inconsistent updates. See [flink-table-planner AGENTS.md](../flink-table-planner/AGENTS.md#changelog-processing) for the planner-side picture.

### `RowKind` semantics

`+I` (INSERT), `-U` (UPDATE_BEFORE), `+U` (UPDATE_AFTER), `-D` (DELETE). Stateful operators that buffer rows must apply `-U`/`-D` as retractions; emitting `-U`/`+U` pairs vs. only `+U` depends on the operator's declared `ChangelogMode` (negotiated by the planner). Defined in [`RowKind.java`](../../flink-core/src/main/java/org/apache/flink/types/RowKind.java).

### Time signals: row column vs `StreamRecord.timestamp`

Two distinct time concepts coexist on the streaming runtime, and they behave very differently in the SQL/Table layer than in the DataStream API:

- **Row event-time column** - a regular column inside the `RowData` payload, declared via `WATERMARK FOR <col> AS ...`. This is the **canonical event-time signal in SQL**. It is data, propagates with the row through every operator, and is what built-in time-aware operators (window aggregates, temporal joins, FLIP-558 V2 SUM compaction, etc.) actually read. For a CDC `-U`/`-D`, this column carries the *prior version's* logical time - not when the deletion happened.
- **`StreamRecord.timestamp`** - the optional `long` (millis since epoch) attached to the value envelope ([`StreamRecord.java`](../../flink-runtime/src/main/java/org/apache/flink/streaming/runtime/streamrecord/StreamRecord.java); semantic contract on [`TimestampAssigner`](../../flink-core/src/main/java/org/apache/flink/api/common/eventtime/TimestampAssigner.java): "event time, used by all functions that operate on event time"). Sentinel `Long.MIN_VALUE` means "no timestamp set". This is the DataStream API's native event-time channel.

**The envelope timestamp is not reliably propagated through the SQL pipeline.** It is set at the source (e.g. Kafka emits with `consumerRecord.timestamp()` in [`KafkaRecordEmitter.java:46-58`](https://github.com/apache/flink-connector-kafka/blob/main/flink-connector-kafka/src/main/java/org/apache/flink/connector/kafka/source/reader/KafkaRecordEmitter.java#L46-L58); a pushed `WatermarkStrategy.withTimestampAssigner(...)` via `SupportsWatermarkPushDown` may overwrite it from a row column). After that, most table operators **don't preserve it**:

- [`TimestampedCollector`](../../flink-runtime/src/main/java/org/apache/flink/streaming/api/operators/TimestampedCollector.java) - the wrapper used by many table operators - reuses a `StreamRecord` whose timestamp starts unset and is only attached if the operator explicitly calls `setTimestamp(...)` / `setAbsoluteTimestamp(...)`. `SinkUpsertMaterializer` (V1 and V2) creates a `TimestampedCollector` and never sets a timestamp, so emitted records leave with `hasTimestamp=false`. `WindowAggOperator` explicitly calls `eraseTimestamp()` before assigning the window's emit time. Outcome: relying on the envelope timestamp mid-SQL-pipeline is unsafe.
- [`WatermarkAssignerOperator`](src/main/java/org/apache/flink/table/runtime/operators/wmassigners/WatermarkAssignerOperator.java) reads the row's rowtime column to emit `Watermark` events but does **not** set `StreamRecord.timestamp`.

The only place the SQL runtime deliberately writes the envelope from a row column is [`StreamRecordTimestampInserter`](src/main/java/org/apache/flink/table/runtime/operators/sink/StreamRecordTimestampInserter.java) - inserted right before sinks so connectors (e.g. Kafka producer) can stamp the outgoing record with the row's logical event-time.

**Practical rule for SQL/Table code:**

- Treat the row's event-time column as the source of truth.
- Treat `Watermark` events (separate from records) as the time-progress signal - they are propagated by the framework regardless of envelope timestamps.
- Don't read `element.getTimestamp()` in a new table operator unless you know the upstream chain set it (window operators, source, FLIP-558 V2 input where the operator does use `element.getTimestamp()` internally for ordering). Use a row column index instead.
- DataStream API users see the envelope timestamp directly and can rely on it; the table runtime cannot.

**`-U` and `-D` carry the prior version's row-column timestamp.** Conceptually a `-U`/`-D` is "the row that was previously visible is gone", so it carries the row being removed - including its event-time column. For Debezium-style CDC, the `before` image's `update_time` is exactly the time the deleted version was last valid. For partial-delete tombstones expanded by `ChangelogNormalize`, the emitted `-D` payload comes from state (the prior `+I`/`+U` row contents), so its row column carries the prior version's time.

Confusing the row event-time column with the envelope timestamp is the source of most "watermark stuck on deletes" / "temporal join sees stale dim after delete" / "SUM state grows unbounded" bugs. See FLIP-558 motivation for the broader framing.

### `ChangelogNormalize` - what it does

The runtime side is [`ProcTimeDeduplicateKeepLastRowFunction`](src/main/java/org/apache/flink/table/runtime/operators/deduplicate/ProcTimeDeduplicateKeepLastRowFunction.java) (or its mini-batch sibling), called from `processLastRowOnChangelog` in [`DeduplicateFunctionHelper`](src/main/java/org/apache/flink/table/runtime/operators/deduplicate/utils/DeduplicateFunctionHelper.java). It keeps the latest row per unique key in `ValueState<RowData>` and performs four jobs in one operator:

1. **Deduplicate by key** - if a `+U`/`+I` arrives for an already-stored row with identical content (and TTL is disabled), the duplicate is swallowed.
2. **Synthesize `-U` (UPDATE_BEFORE)** - when `generateUpdateBefore=true`, an incoming `+U` is paired with a `-U` built from the previous state value. This converts a partial-image upsert stream `{+I, +U, -D}` into a full retract stream `{+I, -U, +U, -D}` for downstream operators that need it.
3. **Expand partial deletes into full deletes** - upsert sources (e.g. Kafka tombstones) often deliver `-D` with only key columns. ChangelogNormalize emits the *stored* full row as `-D` instead, so downstream operators see the complete row content. See the comment at [`DeduplicateFunctionHelper.java:140-148`](src/main/java/org/apache/flink/table/runtime/operators/deduplicate/utils/DeduplicateFunctionHelper.java#L140-L148).
4. **Normalize `RowKind`** - rewrites the in-state row to `+I` and emits `+I` (first occurrence) or `+U` (subsequent updates), so the downstream sees a clean retract changelog regardless of upstream sloppiness.

Optionally pushes a filter into the same operator (`filterCondition`) and supports mini-batching. **Note:** ChangelogNormalize does *not* fix changelog disorder across keys - it operates per key and assumes ordering inside a key. Cross-key disorder is `SinkUpsertMaterializerV2`'s job (FLIP-558).

Concrete example for an upsert source `{+I, +U, -D}` with `generateUpdateBefore=true`:

```
input:  +I(k=1, v=A)              +U(k=1, v=B)              -D(k=1, [tombstone])
state:  -                         +I(k=1, v=A)              +I(k=1, v=B)
emit:   +I(k=1, v=A)              -U(k=1, v=A) +U(k=1, v=B)  -D(k=1, v=B)
```

### `SinkUpsertMaterializer` (SUM) - what it does

SUM resolves PK conflicts when the *upstream upsert key differs from the sink's primary key*. Imagine two source rows with different upsert keys mapping to the same sink PK (e.g. a join produces two rows with `name='Football'` but different `product_id`s into a sink keyed by `name`). SUM keeps a per-PK history of "which source-key-versions are still alive" so it knows the right value to emit at each step.

[`SinkUpsertMaterializer`](src/main/java/org/apache/flink/table/runtime/operators/sink/SinkUpsertMaterializer.java) (V1) uses `ValueState<List<RowData>>` keyed by sink PK. Three responsibilities:

1. **Track all live versions per PK** - on `+I`/`+U`, replace the entry with matching upsert key (or append if new). Emit `+I` if the list was empty, otherwise `+U`.
2. **Apply retractions to the right entry** - on `-U`/`-D`, find and remove the first matching entry by upsert key.
3. **"Rollback" emission rule** - if the removed entry was the *last* in the list, emit `+U` for the new last row (rolling back to the prior live value); if the list becomes empty, emit `-D`; if a middle entry was removed, emit nothing.

This per-PK history is the source of the well-known SUM state-size problem (FLIP-558 motivation). TTL via `StateTtlConfig` is the only mitigation in V1.

Example for two source upsert keys `(p=1, name=Football, q=5)` and `(p=2, name=Football, q=6)` both targeting sink PK `name`:

```
input:  +U(p=1,name=F,q=5)  +U(p=2,name=F,q=6)  -U(p=2,name=F,q=6)
state:  [(p=1,q=5)]         [(p=1,q=5),(p=2,q=6)] [(p=1,q=5)]
emit:   +I(name=F,q=5)      +U(name=F,q=6)        +U(name=F,q=5)  // rollback
```

### `SinkUpsertMaterializerV2` (FLIP-558, FLINK-38461)

[`SinkUpsertMaterializerV2`](src/main/java/org/apache/flink/table/runtime/operators/sink/SinkUpsertMaterializerV2.java) keeps the same DEDUPLICATE/rollback contract as V1 but replaces the per-key `List<RowData>` with a `SequencedMultiSetState<RowData>` that tracks insertion order via the `StreamRecord` timestamp. Watermark-driven compaction inside the multiset addresses changelog disorder (out-of-order `-U`/`+U` arriving via different upstream paths). Removal returns a `StateChangeInfo` enum:

- `REMOVAL_ALL` -> emit `-D`
- `REMOVAL_LAST_ADDED` -> emit `+U` for the new last row (rollback)
- `REMOVAL_OTHER` -> no-op
- `REMOVAL_NOT_FOUND` -> warn, no-op

Sibling operator `WatermarkCompactingSinkMaterializer` is a different code path used for the new `ON CONFLICT DO ERROR` / `DO NOTHING` strategies (FLIP-558) - it compacts on combined-watermark progression and either fails or drops on duplicate PKs instead of running the rollback algorithm. Selection between SUM V1, V2, and the watermark-compacting variant happens in [`StreamExecSink`](../flink-table-planner/src/main/java/org/apache/flink/table/planner/plan/nodes/exec/stream/StreamExecSink.java) based on `TABLE_EXEC_SINK_UPSERT_MATERIALIZE_STRATEGY` and the `ON CONFLICT` clause.

### `FROM_CHANGELOG` / `TO_CHANGELOG` PTFs (FLIP-564)

Two built-in process table functions that expose the stream/table duality in SQL:

- **`FROM_CHANGELOG`** (FLINK-39261) - parses an append-only CDC stream into a Flink changelog. Current parameters in [`BuiltInFunctionDefinitions.java:815-837`](../flink-table-common/src/main/java/org/apache/flink/table/functions/BuiltInFunctionDefinitions.java#L815-L837): `input` (table, row semantics), `op` (descriptor pointing at the operation-code column), `op_mapping` (`MAP<STRING, STRING>` mapping source codes to `RowKind` names, e.g. `'u' -> 'UPDATE_BEFORE, UPDATE_AFTER'`). Output changelog mode is `ChangelogMode.all()`. Unmapped or NULL op codes throw `TableRuntimeException` (FLINK-39495 - prior to that fix they were silently dropped). Note: the FLIP-564 design proposes additional parameters (`before`/`after` image descriptors, `state_ttl`, partition/order syntax for reordering, `invalid_op_handling`) that are not in the current implementation.
- **`TO_CHANGELOG`** (FLINK-39419, FLINK-39392) - emits a CDC append stream from a dynamic table. Current parameters in [`BuiltInFunctionDefinitions.java:783-813`](../flink-table-common/src/main/java/org/apache/flink/table/functions/BuiltInFunctionDefinitions.java#L783-L813): `input` (table), `op`, `op_mapping`. Declared with `ROW_SEMANTIC_TABLE`, `SUPPORT_UPDATES`, `REQUIRE_UPDATE_BEFORE`, and `REQUIRE_FULL_DELETE` traits, so the planner ensures the input arrives as a full retract changelog with materialized `-U` and full-row `-D`. Useful for archival/audit pipelines or sending to non-CDC-aware sinks.

Type strategies in flink-table-common: `FromChangelogTypeStrategy.java`, `ToChangelogTypeStrategy.java`.

### Common pitfalls

- New sink/aggregator silently ignoring `-U`/`-D` because it was written assuming insert-only input.
- Tests asserting V1 state layout against an operator now running V2.
- New PTFs that don't honor `state_ttl` and grow state forever.
- Skipping `op_mapping` edge cases in PTF tests (multi-code mappings, NULL op codes, unmapped op codes which now throw).

## Testing Patterns

- **Harness tests:** Use `OneInputStreamOperatorTestHarness<RowData, RowData>` with `RowDataHarnessAssertor` for output validation. See `operators/join/LookupJoinHarnessTest.java` as a reference.
- **Test utilities:** `StreamRecordUtils.insertRecord()` for test records, `RowDataHarnessAssertor` for assertions
- **Operator test base classes:** Module has dedicated base classes per operator type (e.g., `TemporalTimeJoinOperatorTestBase`, `Int2HashJoinOperatorTestBase`, `WindowAggOperatorTestBase`)
- **State migration tests:** Use snapshot files per Flink version and state backend type to verify forward/backward compatibility
