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

# flink-table-planner

Translates and optimizes SQL/Table API programs into executable plans using Apache Calcite. Bridges the Table/SQL API and the runtime by generating code and execution plans. The planner is loaded in a separate classloader (`flink-table-planner-loader`) to isolate Calcite dependencies.

See also [README.md](README.md) for Immutables rule config conventions and JSON plan test regeneration.

## Build Commands

Full table modules rebuild:

```
./mvnw clean install -T1C -DskipTests -Pskip-webui-build -pl flink-table/flink-table-common,flink-table/flink-sql-parser,flink-table/flink-table-planner-loader,flink-table/flink-table-planner,flink-table/flink-table-api-java -am
```

After the first full build, drop `-am` for faster rebuilds when you're only changing code within these modules.

## Key Directory Structure

- `plan/rules/physical/stream/` and `plan/rules/physical/batch/` — Physical planner rules
- `plan/rules/logical/` — Logical optimization rules
- `plan/nodes/exec/stream/` and `plan/nodes/exec/batch/` — ExecNodes (bridge between planner and runtime)
- `plan/nodes/exec/spec/` — Serializable operator specifications (JoinSpec, WindowSpec, etc.)
- `plan/nodes/physical/stream/` and `plan/nodes/physical/batch/` — Intermediate physical nodes (Calcite-based)
- `plan/nodes/logical/` — Logical nodes (Calcite-based)
- `codegen/` — Code generation
- `codegen/calls/` — Custom code generators for specific functions (e.g., `JsonCallGen.scala`)
- `functions/casting/` — Cast rules for code generation (e.g., `BinaryToBinaryCastRule`, `StringToTimeCastRule`)
- `functions/` — Function management and inference
- `catalog/` — Catalog integration

## Key Abstractions

- **ExecNode**: Bridge between planner and runtime. Annotated with `@ExecNodeMetadata(name, version, minPlanVersion, minStateVersion)` for versioning and backwards compatibility. Extends `ExecNodeBase<T>` and implements either `StreamExecNode<T>` (streaming) or `BatchExecNode<T>` (batch); `T` is typically `RowData`.
- **Physical rules**: Extend `RelRule`, use Immutables `@Value.Immutable` for config. Transform logical nodes to physical nodes. Registered in `FlinkStreamRuleSets` and/or `FlinkBatchRuleSets`.
- **Logical optimization rules**: Also extend `RelRule`, often use `RexShuttle` for expression rewriting. Registered in rule sets.
- **Specs**: Serializable specifications in `plan/nodes/exec/spec/` (JoinSpec, WindowSpec, etc.) that carry operator configuration.

## Common Change Patterns

### Adding a new table operator

Components involved (can be developed top-down or bottom-up):

1. **Runtime operator** in `flink-table-runtime` under `operators/` (extend `TableStreamOperator`, implement `OneInputStreamOperator` or `TwoInputStreamOperator`). Test with harness tests. See [flink-table-runtime AGENTS.md](../flink-table-runtime/AGENTS.md).
2. **ExecNode** in `plan/nodes/exec/stream/` and/or `plan/nodes/exec/batch/` (extend `ExecNodeBase<T>`; implement `StreamExecNode<T>` for streaming or `BatchExecNode<T>` for batch; annotate with `@ExecNodeMetadata`; `T` is typically `RowData`)
3. **Physical Node + Physical Rules** in `plan/rules/physical/stream/` and/or `plan/rules/physical/batch/` (physical rules usually extend `ConverterRule` via `Config.INSTANCE.withConversion(...)`; same-convention rewrites extend `RelRule` with an `@Value.Immutable` config)
4. **Logical Node + Planner rule**
5. Tests: semantic tests, plan tests, restore tests (if stateful)

Both `stream/` and `batch/` directories exist for rules and ExecNodes. Consider whether your change applies to one or both.

### Adding a planner optimization rule

Pick the base class by what the rule does:
- Converts a node from one calling convention to another (for example, logical → stream physical): extend `ConverterRule`. 
Call `ConverterRule.Config.INSTANCE.withConversion(...)` in the constructor, do not define your own config.
- Rewrites nodes within the same convention (logical → logical, physical → physical): extend `RelRule` with an `@Value.Immutable` config. 
Some existing rules still use Calcite's older `RelOptRule`; prefer `RelRule` for new code.

Then:
1. Register in `FlinkStreamRuleSets.scala` and/or `FlinkBatchRuleSets.scala`
2. Plan tests with XML golden files — when the test fails, copy the framework's generated log file over the reference `.xml` (cases are ordered alphabetically by method name)
3. A same-convention rewrite needs no runtime changes. A `ConverterRule` that produces a new physical node also needs the physical node, ExecNode, and runtime operator — see "Adding a new table operator" above.

### Extending SQL syntax

1. Modify parser grammar in `flink-sql-parser` (`parserImpls.ftl`)
2. Add operation conversion logic in `SqlNodeToOperationConversion.java`
3. Test with parser tests and SQL gateway integration tests (`.q` files)

### Code generation changes

- Cast rules live in `functions/casting/`. Each extends `AbstractExpressionCodeGeneratorCastRule` or similar.
- Custom call generators for functions live in `codegen/calls/` (e.g., `JsonCallGen.scala`). Simple scalar functions typically don't need these; the planner handles them uniformly through the function definition.
- Immutables library is used for rule configs (`@Value.Immutable`, `@Value.Enclosing`). See [README.md](README.md).

### Plan serialization changes

- ExecNode specs use Jackson for JSON serialization. Source/sink specs should use `@JsonIgnoreProperties(ignoreUnknown = true)` for forward compatibility.
- When adding new ExecNode features, update `RexNodeJsonDeserializer` or related serde classes if new function kinds or types are introduced.

### ExecNode versioning

When bumping an ExecNode version, update the `@ExecNodeMetadata` annotation's `version` and `minPlanVersion`/`minStateVersion` fields. Add restore test snapshots for the new version.

### Configuration options

New features often introduce `ExecutionConfigOptions` entries (in `flink-table-api-java`) for runtime tunability (e.g., cache sizes, timeouts, batch sizes).

## Changelog Processing

Every stream in Flink SQL carries a `ChangelogMode` - the set of `RowKind`s (`+I`, `-U`, `+U`, `-D`) it can produce or consume. The planner tracks this as two traits and propagates them across the plan: most subtle table-layer bugs trace back to mishandling these.

### Core types

- `ChangelogMode` ([flink-table-common](../flink-table-common/src/main/java/org/apache/flink/table/connector/ChangelogMode.java)) - the public contract for source/sink connectors. The three canonical modes: append `{+I}`, upsert `{+I, +U, -D}`, retract `{+I, -U, +U, -D}`.
- `RowKind` ([flink-core](../../flink-core/src/main/java/org/apache/flink/types/RowKind.java)) - the per-record tag.
- `ModifyKindSet` / `ModifyKindSetTrait` and `UpdateKind` / `UpdateKindTrait` (`plan/trait/`) - the planner-internal split. `ModifyKindSet` says *which kinds flow*; `UpdateKind` says whether updates are `BEFORE_AND_AFTER`, `ONLY_UPDATE_AFTER`, or `NONE`. Splitting them lets the planner request the cheaper `ONLY_UPDATE_AFTER` form when downstream tolerates it.

### Two-pass inference

[`FlinkChangelogModeInferenceProgram.scala`](src/main/scala/org/apache/flink/table/planner/plan/optimize/program/FlinkChangelogModeInferenceProgram.scala) runs three nested visitors:

1. **`SatisfyModifyKindSetTraitVisitor`** (bottom-up) - asks each node which `RowKind`s it *can* produce given its inputs.
2. **`SatisfyUpdateKindTraitVisitor`** (top-down) - propagates each consumer's `UpdateKind` requirement to its inputs, so producers don't emit `-U` when no consumer needs it.
3. **`SatisfyDeleteKindTraitVisitor`** (top-down) - decides between delete-by-key and full-delete on inputs that produce `-D`.

Each visitor is a big `case` match over `StreamPhysicalRel` subclasses. A new physical node that isn't covered by those matches falls through to default handling and may silently get wrong changelog metadata - add it to all three visitors when introducing one.

### Primary keys, unique keys, upsert keys

Three related but distinct concepts:

- **Primary key (PK)** - user contract, declared in `CREATE TABLE ... PRIMARY KEY NOT ENFORCED`. Always NOT NULL (Flink enforces this at table creation). At most one per table. Used by sinks to know what to upsert against, and by sources to mark a row identifier. The PK is what shows up in `ResolvedSchema.getPrimaryKey()`.
- **Unique key** - planner-derived set(s) of columns guaranteed to identify a row uniquely in a given relational node's output. Multiple unique keys may coexist (e.g. PK plus a `GROUP BY` introducing another). Tracked by Calcite metadata in [`FlinkRelMdUniqueKeys`](src/main/scala/org/apache/flink/table/planner/plan/metadata/FlinkRelMdUniqueKeys.scala). Carries an `ignoreNulls` flag passed through Calcite's API - it controls whether SQL-level uniqueness analysis treats nullable columns as unique. The runtime engine itself handles NULL key columns fine (Java equality, deterministic hashing), so this flag is a planner-side semantic concern, not a runtime invariant.
- **Upsert key** - the subset of unique keys that the *streaming pipeline* can use to route and apply updates. Derived in [`FlinkRelMdUpsertKeys.scala`](src/main/scala/org/apache/flink/table/planner/plan/metadata/FlinkRelMdUpsertKeys.scala) starting from unique keys (with `ignoreNulls=false`). Two helpers shape the result: `enrichWithImmutableColumns` adds extra key variants by unioning each base key with the input's immutable columns (e.g. `{k1}` plus `{k1, immutable_col}`), giving downstream more candidates to match against; `filterKeys` then keeps only variants whose columns subsume the hash distribution key (so all updates for that key land at the same task). The "smallest" upsert key chosen for an operator comes from [`UpsertKeyUtil.getSmallestKey`](src/main/java/org/apache/flink/table/planner/plan/utils/UpsertKeyUtil.java).

**PK vs upsert key at the sink.** When the upstream upsert key equals (or is contained in) the sink's PK, updates can be routed straight through. When they differ, the planner inserts `SinkUpsertMaterializer` to reconcile - see the operators section below. This mismatch is the single most common source of expensive plans.

**Preservation through expressions.** Casts and projections preserve upsert keys *only when injective* - non-injective casts (e.g. `BIGINT -> INT` on a key column) collapse distinct keys and must be treated as key-destroying. See FLINK-39088 for the recent injective-cast handling. When extending key derivation through a new `RexCall`, add an explicit injectivity check; do not assume preservation.

### Operators the planner inserts

The planner injects these to bridge changelog-mode mismatches. See [flink-table-runtime AGENTS.md](../flink-table-runtime/AGENTS.md#changelog-processing) for runtime internals and worked examples.

For what each operator actually does at runtime (the per-key dedupe/expand/normalize logic of ChangelogNormalize, the rollback algorithm of SUM, V1 vs V2 vs `WatermarkCompactingSinkMaterializer`), see [flink-table-runtime AGENTS.md](../flink-table-runtime/AGENTS.md#changelog-processing). The planner-side concern is *when* and *why* each gets inserted:

**`StreamPhysicalChangelogNormalize`** ([file](src/main/scala/org/apache/flink/table/planner/plan/nodes/physical/stream/StreamPhysicalChangelogNormalize.scala)) - inserted as a speculative placeholder during physical conversion, then often pruned once trait inference knows what downstream actually consumes:

1. **Speculative insertion during physical conversion.** [`StreamPhysicalTableSourceScanRule`](src/main/scala/org/apache/flink/table/planner/plan/rules/physical/stream/StreamPhysicalTableSourceScanRule.scala) calls [`DynamicSourceUtils.changelogNormalizeEnabled`](src/main/java/org/apache/flink/table/planner/connectors/DynamicSourceUtils.java); if true, a `StreamPhysicalChangelogNormalize` is placed on top of the scan as a placeholder. Conditions: the source is an **upsert source** (`+U` present, `-U` absent, PK declared) or a **CDC source with possible duplicates** (non-insert-only + PK + `TABLE_EXEC_SOURCE_CDC_EVENTS_DUPLICATE=true`); and `eventTimeSnapshotRequired` is false. At this point the planner has no view of what downstream actually consumes, so it inserts the node defensively.
2. **Trait inference decides what survives.** Once `FlinkChangelogModeInferenceProgram` runs, it knows the full downstream demand:
   - If downstream is happy with `ONLY_UPDATE_AFTER` and no other consumer needs the normalize's work (no filter pushed in, no CDC dedup requested, no metadata columns accessed), [`ChangelogNormalizeRequirementResolver.isRequired`](src/main/java/org/apache/flink/table/planner/plan/optimize/ChangelogNormalizeRequirementResolver.java) returns false and the node is removed entirely - the upsert stream flows through untouched.
   - Otherwise the node is kept, with `generateUpdateBefore` toggled based on demand (`BEFORE_AND_AFTER` vs `ONLY_UPDATE_AFTER`) by `SatisfyUpdateKindTraitVisitor`, and the input delete trait picked by `SatisfyDeleteKindTraitVisitor` (prefers delete-by-key, falls back to full).
3. **Filter pushdown** is prepared by [`FlinkMarkChangelogNormalizeProgram`](src/main/java/org/apache/flink/table/planner/plan/rules/physical/stream/FlinkMarkChangelogNormalizeProgram.java) + `PushCalcPastChangelogNormalizeRule`, which can push a downstream filter into a surviving normalize to shrink its state.

The takeaway: an upsert source does not force a `ChangelogNormalize` in the final plan - it just opens the door for one, and the inference pass is the real arbiter.

**`StreamPhysicalDropUpdateBefore`** ([file](src/main/scala/org/apache/flink/table/planner/plan/nodes/physical/stream/StreamPhysicalDropUpdateBefore.scala)) - inserted by trait inference in `FlinkChangelogModeInferenceProgram` when an upstream produces `BEFORE_AND_AFTER` but the requirement above is `ONLY_UPDATE_AFTER` - drops `-U` records to save shuffle cost.

**`StreamPhysicalSinkUpsertMaterializer` (SUM)** - decision lives in [`FlinkChangelogModeInferenceProgram.analyzeUpsertMaterializeStrategy`](src/main/scala/org/apache/flink/table/planner/plan/optimize/program/FlinkChangelogModeInferenceProgram.scala), gated by `TABLE_EXEC_SINK_UPSERT_MATERIALIZE`:

- `FORCE` - inserted whenever the sink has a non-empty PK and is not a retract sink.
- `NONE` - never inserted.
- `AUTO` (default) - inserted only if **all** of: sink has PK, sink is upsert (not append, not retract), input has updates, and **sink PK does not contain the input upsert key**. Skipped (FLINK-38201) for retract sinks; also skipped for an append input combined with `ON CONFLICT DO DEDUPLICATE` (the dedupe is implicit).

The runtime variant (V1 / V2 / `WatermarkCompactingSinkMaterializer`) is chosen in [`StreamExecSink`](src/main/java/org/apache/flink/table/planner/plan/nodes/exec/stream/StreamExecSink.java) based on `SinkUpsertMaterializeStrategy` and the `ON CONFLICT` clause. `ON CONFLICT DO ERROR` / `DO NOTHING` additionally requires source watermarks (`validateSourcesHaveWatermarks`).

### Sink contract

[`DynamicTableSink#getChangelogMode(ChangelogMode requestedMode)`](../flink-table-common/src/main/java/org/apache/flink/table/connector/sink/DynamicTableSink.java) is a *negotiation*, not a getter. Return only modes the sink can actually handle: the planner uses the answer to decide which of the operators above to insert. Silently accepting `RETRACT` when you only handle `INSERT` produces wrong results at runtime.

### Common pitfalls

- New `StreamPhysicalRel` declares `INSERT_ONLY` but downstream of it produces updates - inference picks the wrong mode and codegen emits without `-U`.
- Adding a `RexCall` to upsert-key derivation without an injectivity check (FLINK-39088 territory).
- Forcing `BEFORE_AND_AFTER` when `ONLY_UPDATE_AFTER` would suffice - inserts an unnecessary `ChangelogNormalize` and bloats state.
- Changing trait inference and not regenerating plan-test XML golden files.
- Assuming SUM V1 layout in tests when V2 is now active by default.

## Testing Patterns

Choose test types based on what you're changing:

- **Semantic tests** (for ExecNode/operator changes): Use `SemanticTestBase` (streaming) or `BatchSemanticTestBase` (batch) in `plan/nodes/exec/testutils/`. Extends `CommonSemanticTestBase` which implements `TableTestProgramRunner`. Prefer these over ITCase for operators and ExecNodes.
- **Restore tests** (for stateful operators): Use `RestoreTestBase` or `BatchRestoreTestBase` in `plan/nodes/exec/testutils/`. Implements `TableTestProgramRunner`, uses `@ExtendWith(MiniClusterExtension.class)`. Required when your operator uses state. Tests savepoint creation and job restart in two phases: (1) generate compiled plans + savepoints, (2) verify recovery.
- **Plan tests** (for optimization rules): Verify the generated execution plan using XML golden files. Used for logical and physical optimization rules.
- **ITCase** (for built-in functions): Function tests typically use ITCase with `TestSetSpec` for end-to-end verification (e.g., `JsonFunctionsITCase`, `TimeFunctionsITCase`).
- **JSON plan test regeneration:** Set `PLAN_TEST_FORCE_OVERWRITE=true` environment variable (documented in [README.md](README.md)).
