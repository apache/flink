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

- **ExecNode**: Bridge between planner and runtime. Annotated with `@ExecNodeMetadata(name, version, minPlanVersion, minStateVersion)` for versioning and backwards compatibility. Extends `ExecNodeBase<RowData>`, implements `StreamExecNode<RowData>`.
- **Physical rules**: Extend `RelRule`, use Immutables `@Value.Immutable` for config. Transform logical nodes to physical nodes. Registered in `FlinkStreamRuleSets` and/or `FlinkBatchRuleSets`.
- **Logical optimization rules**: Also extend `RelRule`, often use `RexShuttle` for expression rewriting. Registered in rule sets.
- **Specs**: Serializable specifications in `plan/nodes/exec/spec/` (JoinSpec, WindowSpec, etc.) that carry operator configuration.

## Common Change Patterns

### Adding a new table operator

Components involved (can be developed top-down or bottom-up):

1. **Runtime operator** in `flink-table-runtime` under `operators/` (extend `TableStreamOperator`, implement `OneInputStreamOperator` or `TwoInputStreamOperator`). Test with harness tests. See [flink-table-runtime AGENTS.md](../flink-table-runtime/AGENTS.md).
2. **ExecNode** in `plan/nodes/exec/stream/` and/or `plan/nodes/exec/batch/` (extend `ExecNodeBase<RowData>`, implement `StreamExecNode<RowData>`, annotate with `@ExecNodeMetadata`)
3. **Physical Node + Physical Rules** in `plan/rules/physical/stream/` and/or `plan/rules/physical/batch/` (extend `RelRule`, use `@Value.Immutable` config pattern)
4. **Logical Node + Planner rule**
5. Tests: semantic tests, plan tests, restore tests (if stateful)

Both `stream/` and `batch/` directories exist for rules and ExecNodes. Consider whether your change applies to one or both.

### Adding a planner optimization rule

1. Create rule class extending `RelRule` with `@Value.Immutable` config
2. Register in `FlinkStreamRuleSets.scala` and/or `FlinkBatchRuleSets.scala`
3. Test with plan tests using XML golden files (before/after comparison)
4. No runtime changes needed (optimization is compile-time only)

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

## Testing Patterns

Choose test types based on what you're changing:

- **Semantic tests** (for ExecNode/operator changes): Use `SemanticTestBase` (streaming) or `BatchSemanticTestBase` (batch) in `plan/nodes/exec/testutils/`. Extends `CommonSemanticTestBase` which implements `TableTestProgramRunner`. Prefer these over ITCase for operators and ExecNodes.
- **Restore tests** (for stateful operators): Use `RestoreTestBase` or `BatchRestoreTestBase` in `plan/nodes/exec/testutils/`. Implements `TableTestProgramRunner`, uses `@ExtendWith(MiniClusterExtension.class)`. Required when your operator uses state. Tests savepoint creation and job restart in two phases: (1) generate compiled plans + savepoints, (2) verify recovery.
- **Plan tests** (for optimization rules): Verify the generated execution plan using XML golden files. Used for logical and physical optimization rules.
- **ITCase** (for built-in functions): Function tests typically use ITCase with `TestSetSpec` for end-to-end verification (e.g., `JsonFunctionsITCase`, `TimeFunctionsITCase`).
- **JSON plan test regeneration:** Set `PLAN_TEST_FORCE_OVERWRITE=true` environment variable (documented in [README.md](README.md)).
