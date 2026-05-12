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

## Testing Patterns

- **Harness tests:** Use `OneInputStreamOperatorTestHarness<RowData, RowData>` with `RowDataHarnessAssertor` for output validation. See `operators/join/LookupJoinHarnessTest.java` as a reference.
- **Test utilities:** `StreamRecordUtils.insertRecord()` for test records, `RowDataHarnessAssertor` for assertions
- **Operator test base classes:** Module has dedicated base classes per operator type (e.g., `TemporalTimeJoinOperatorTestBase`, `Int2HashJoinOperatorTestBase`, `WindowAggOperatorTestBase`)
- **State migration tests:** Use snapshot files per Flink version and state backend type to verify forward/backward compatibility
