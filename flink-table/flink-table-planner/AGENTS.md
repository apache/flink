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

## Keep it short

Short, easy to read code always. Match the surrounding planner/rule style: no speculative abstractions, no comments restating the code. Cut detailed information, specific information not relevant for future readers and anything visible in the diff.

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
- `plan/nodes/exec/spec/` — Serializable operator specifications (JoinSpec, OverSpec, etc.)
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
- **Specs**: Serializable specifications in `plan/nodes/exec/spec/` (JoinSpec, OverSpec, etc.) that carry operator configuration.

## SQL to Operator lifecycle

Every table operator traverses the same seven stages. Read the diagram top-to-bottom for the shape, then jump into the stage you need.

```
SQL string
   │  parse
   ▼
SqlNode (AST)
   │  validate            ← names resolve, types infer
   ▼
typed SqlNode
   │  SqlToRel
   ▼
RelNode + RexNode         ← logical algebra
   │  logical phases
   ▼
FlinkLogical*Node
   │  physical phase
   ▼
FlinkPhysical*Node
   │  translateToExecNode
   ▼
ExecNode (checkpoint-stable serializable plan)
   │  translateToPlanInternal + codegen
   ▼
StreamOperator (runs on TaskManager)
```

**Five types to know:**

> - **`SqlNode`** - a node in the SQL syntax tree (what you typed).
> - **`RelNode`** - a node in the relational algebra tree (scan, project, join, ...).
> - **`RexNode`** - an expression inside a `RelNode` (column ref, literal, function call).
> - **`ExecNode`** - the planner-runtime bridge, JSON-serializable so a compiled plan can be saved and restored.
> - **`StreamOperator`** - the runtime class that processes records on a TaskManager.

**Two planners to know** (both used in stages 4-5, see `FlinkStreamProgram` for the chained phases):

> - **HepPlanner** - deterministic, fires rules in a fixed order until no more match. Used for rewrites that don't need cost estimation: subquery elimination, decorrelation, predicate pushdown, projection cleanup, etc. Rules typically extend `RelRule` (or the older `RelOptRule`) and rewrite within the same calling convention (logical → logical, physical → physical).
> - **VolcanoPlanner** - cost-based, explores alternative plans and picks the cheapest using a cost model. Used for convention conversion (Calcite logical → Flink logical in the `LOGICAL` phase, Flink logical → Flink physical in the `PHYSICAL` phase) and for picking among physical strategies (e.g., interval join vs regular join). Rules typically extend `ConverterRule`, change calling convention via `Config.INSTANCE.withConversion(...)`, and participate in the cost-based search.

Each stage below names the framework piece, then shows where binary join and PTF land in it.

### 1. Parsing (SQL text → SqlNode tree)
SQL string is tokenized and parsed into a `SqlNode` AST by the generated parser (grammar in `flink-sql-parser/parserImpls.ftl`, driver in `planner/parse/CalciteParser`).

  Examples:
  - Binary join: `a JOIN b ON a.x = b.y` becomes a `SqlJoin` node.
  - PTF: `TABLE(my_ptf(TABLE t1 PARTITION BY a))` becomes a `SqlBasicCall` holding the function call with table args.

### 2. Validation (resolve names, infer types)
`FlinkPlannerImpl.validate` runs `FlinkCalciteSqlValidator` over the AST: identifiers (tables, functions, columns) get resolved, types are inferred, arg counts/types are checked. Function calls are matched to a `SqlOperator` via the `SqlOperatorTable`. Extension points: register a function in the operator table; provide a `TypeInference` for typing.

  Examples:
  - Binary join: the `SqlJoin` operator is validated; equality predicates resolve to standard `SqlBinaryOperator`s.
  - PTF: `FunctionCatalogOperatorTable.lookupOperatorOverloads` resolves the function name; `BridgingSqlFunction` wraps the user's `TypeInference` via `SystemTypeInference` (system args, output column derivation).

### 3. SqlToRel (SqlNode → RelNode + RexNode)
`FlinkPlannerImpl.rel` invokes Calcite's `SqlToRelConverter` to produce the initial `RelNode` tree with `RexNode` expressions. Customizable via convertlets registered in `FlinkConvertletTable`.

  Examples:
  - Binary join: produces a `LogicalJoin` with the ON condition as a `RexNode`.
  - PTF: `FlinkConvertletTable.convertTableArgs` rewrites table arguments into `RexTableArgCall` operands.

### 4. Logical plan (rewrite to a cheaper equivalent tree)
`FlinkStreamProgram` chains the phases that run during this stage. HepPlanner drives the rewrite phases (`SUBQUERY_REWRITE`, `TEMPORAL_JOIN_REWRITE`, `DECORRELATE`, `DEFAULT_REWRITE`, `PREDICATE_PUSHDOWN`, `JOIN_REORDER`, `MULTI_JOIN`, `PROJECT_REWRITE`, `LOGICAL_REWRITE`); VolcanoPlanner drives the `LOGICAL` phase that converts Calcite logical nodes to Flink logical nodes (required output trait `FlinkConventions.LOGICAL`); `TIME_INDICATOR` runs the dedicated `FlinkRelTimeIndicatorProgram`. All rule sets live in `FlinkStreamRuleSets`.

  Examples:
  - Binary join: `FlinkLogicalJoinConverter` produces `FlinkLogicalJoin`.
  - PTF: `FlinkLogicalTableFunctionScanConverter` produces `FlinkLogicalTableFunctionScan`, then calls `BridgingSqlFunction.resolveCallTraits(call)` to bake conditional traits into the operator's static args.

### 5. Physical plan (pick a concrete execution strategy)
VolcanoPlanner runs the `PHYSICAL` phase, cost-picking a physical strategy via `ConverterRule`s. The `PHYSICAL_REWRITE` phase runs post-passes such as `FlinkChangelogModeInferenceProgram`.

  Examples:
  - Binary join: depending on predicate shape, `StreamPhysicalJoinRule` / `StreamPhysicalIntervalJoinRule` / `StreamPhysicalTemporalJoinRule` produces `StreamPhysicalJoin`, `StreamPhysicalIntervalJoin`, or `StreamPhysicalTemporalJoin`.
  - PTF: `StreamPhysicalProcessTableFunctionRule` produces `StreamPhysicalProcessTableFunction`.

### 6. Exec node & compiled plan (serializable handoff to the runtime)
`physicalNode.translateToExecNode()` produces an `@ExecNodeMetadata`-annotated `ExecNode` (the bridge to runtime). JSON serde via `ExecNodeGraphJsonSerializer/Deserializer` (and `RexNodeJsonSerializer/Deserializer` for expressions) supports compiled-plan write and restore. Each `ExecNode` is rebuilt via its `@JsonCreator` constructor on restore.

  Examples:
  - Binary join: `StreamExecJoin`.
  - PTF: `StreamExecProcessTableFunction`. Compiled-plan restore re-runs `BridgingSqlFunction.resolveCallTraits` from the `StreamExecProcessTableFunction` `@JsonCreator` constructor so the runtime still sees the resolved signature.

### 7. Runtime (codegen and build the operator)
`ExecNode.translateToPlanInternal` drives codegen, wraps the generated runner in an operator factory, and emits a `Transformation`. At task startup the factory instantiates the actual operator and `open()` wires state, timers, collectors, then opens the runner; `processElement` feeds records into the runner. Runtime classes live in `flink-table-runtime` (see [its AGENTS.md](../flink-table-runtime/AGENTS.md)).

  Examples:
  - Binary join (streaming): `StreamExecJoin` wires a `StreamingJoinOperator` (or `MiniBatchStreamingJoinOperator` / `AsyncStateStreamingJoinOperator` depending on config). Batch counterparts (`BatchExecHashJoin`, `BatchExecSortMergeJoin`) wire `HashJoinOperator` (codegen via `LongHashJoinGenerator`) or `SortMergeJoinOperator` instead.
  - PTF: `ProcessTableRunnerGenerator` produces a `GeneratedProcessTableRunner` wrapped in `ProcessTableOperatorFactory`; the factory creates either `ProcessSetTableOperator` (set semantics, keyed) or `ProcessRowTableOperator` (row semantics or no table args), both extending `AbstractProcessTableOperator`.

### Worked examples (side-by-side)

| Stage         | Binary join: `SELECT * FROM a JOIN b ON a.x = b.y` | PTF: `SELECT * FROM TABLE(my_ptf(TABLE t1 PARTITION BY a))` |
| ------------- | -------------------------------------------------- | ----------------------------------------------------------- |
| Parsing       | `SqlJoin`                                          | `SqlBasicCall`                                              |
| Validation    | `SqlJoin` operator                                 | `BridgingSqlFunction`                                       |
| SqlToRel      | `LogicalJoin` with ON `RexNode`                    | `RexCall` with `RexTableArgCall` operand                    |
| Logical       | `FlinkLogicalJoin`                                 | `FlinkLogicalTableFunctionScan`                             |
| Physical      | `StreamPhysicalJoin`                               | `StreamPhysicalProcessTableFunction`                        |
| Exec / plan   | `StreamExecJoin`                                   | `StreamExecProcessTableFunction`                            |
| Runtime       | `StreamingJoinOperator`                            | `ProcessTableOperatorFactory` -> `ProcessSetTableOperator`  |

## Common workflows

Recipes organized by the lifecycle stage they target. Stage numbering matches the lifecycle above; stages without recipes today are omitted. The final "Cross-stage" subsection groups workflows that touch more than one stage.

### 1. Parsing

#### 1.1 Extending SQL syntax

1. Modify parser grammar in `flink-sql-parser` (`parserImpls.ftl`)
2. Add operation conversion logic in `SqlNodeToOperationConversion.java`
3. Test with parser tests and SQL gateway integration tests (`.q` files)

### 6. Exec node & compiled plan

#### 6.1 Plan serialization changes

- ExecNode specs use Jackson for JSON serialization. Source/sink specs should use `@JsonIgnoreProperties(ignoreUnknown = true)` for forward compatibility.
- When adding new ExecNode features, update `RexNodeJsonDeserializer` or related serde classes if new function kinds or types are introduced.

#### 6.2 ExecNode versioning

When bumping an ExecNode version, update the `@ExecNodeMetadata` annotation's `version` and `minPlanVersion`/`minStateVersion` fields. Add restore test snapshots for the new version.

### 7. Runtime

#### 7.1 Code generation changes

- Cast rules live in `functions/casting/`. Each extends `AbstractExpressionCodeGeneratorCastRule` or similar.
- Custom call generators for functions live in `codegen/calls/` (e.g., `JsonCallGen.scala`). Simple scalar functions typically don't need these; the planner handles them uniformly through the function definition.
- Immutables library is used for rule configs (`@Value.Immutable`, `@Value.Enclosing`). See [README.md](README.md).

### Cross-stage

#### Adding a new table operator

Components involved (can be developed top-down or bottom-up):

1. **(Runtime)** Runtime operator in `flink-table-runtime` under `operators/` ((extend TableStreamOperator, implement OneInputStreamOperator, TwoInputStreamOperator, or MultipleInputStreamOperator for N-ary inputs). Test with harness tests. See [flink-table-runtime AGENTS.md](../flink-table-runtime/AGENTS.md).
2. **(Exec node & compiled plan)** ExecNode in `plan/nodes/exec/stream/` and/or `plan/nodes/exec/batch/` (extend `ExecNodeBase<T>`; implement `StreamExecNode<T>` for streaming or `BatchExecNode<T>` for batch; annotate with `@ExecNodeMetadata`; `T` is typically `RowData`)
3. **(Physical plan)** Physical Node + Physical Rules in `plan/rules/physical/stream/` and/or `plan/rules/physical/batch/` (physical rules usually extend `ConverterRule` via `Config.INSTANCE.withConversion(...)`; same-convention rewrites extend `RelRule` with an `@Value.Immutable` config)
4. **(Logical plan)** Logical Node + Planner rule
5. Tests: semantic tests, plan tests, restore tests (if stateful)

Both `stream/` and `batch/` directories exist for rules and ExecNodes. Consider whether your change applies to one or both.

#### Adding a planner optimization rule

Pick the base class by what the rule does:
- Converts a node from one calling convention to another (for example, logical → stream physical, **Physical plan**): extend `ConverterRule`. Call `ConverterRule.Config.INSTANCE.withConversion(...)` in the constructor, do not define your own config.
- Rewrites nodes within the same convention (logical → logical at **Logical plan**, physical → physical at **Physical plan**): extend `RelRule` with an `@Value.Immutable` config. Some existing rules still use Calcite's older `RelOptRule`; prefer `RelRule` for new code.

Then:
1. Register in `FlinkStreamRuleSets.scala` and/or `FlinkBatchRuleSets.scala`
2. Plan tests with XML golden files — when the test fails, copy the framework's generated log file over the reference `.xml` (cases are ordered alphabetically by method name)
3. A same-convention rewrite needs no runtime changes. A `ConverterRule` that produces a new physical node also needs the physical node, ExecNode, and runtime operator — see "Adding a new table operator" above.

## Configuration options

New features often introduce `ExecutionConfigOptions` entries (in `flink-table-api-java`) for runtime tunability (e.g., cache sizes, timeouts, batch sizes).

### PTF conditional traits

A *conditional trait* lets a PTF's table-argument traits depend on the call site instead of being fixed at declaration. Example for `TO_CHANGELOG`: the `input` argument is row-semantic by default (single stream, no PARTITION BY), but switches to set-semantic when the user writes `PARTITION BY` so the runtime can co-locate state per key. One declaration, two effective signatures depending on the call.

**Declaration.** Built-in functions add conditional rules in `BuiltInFunctionDefinitions` via `StaticArgument.withConditionalTrait(trait, condition)`. The condition (a `TraitCondition`) is a small value-comparable predicate evaluated against a `TraitContext`. Built-in factories live on `TraitCondition` (`hasPartitionBy()`, `argIsEqualTo(name, value)`, `not(c)`); under the hood they wrap into the package-private `BuiltInCondition` so equality cascades correctly through `StaticArgument.equals`.

**Evaluation.** A `TraitCondition` reads two things: whether `PARTITION BY` is present on this table arg, and the literal value of named scalar args. Both come through `TraitContext`. There are two factories: `TraitContext.of(TableSemantics, CallContext, declared)` for the validation side (called from `SystemTypeInference.resolveStaticArgs`) and a planner-side adapter inside `BridgingSqlFunction.buildTraitContext` that sources the same data from a `RexCall` + `RexTableArgCall`. Same logical context, different inputs because the two layers don't share types.

**Resolution.** Three call sites bake conditional traits into the operator's effective signature:

1. **Validation** — `SystemTypeInference.resolveStaticArgs` runs once each from `inferInputTypes` and `inferType`. Twice per validation pass; can't dedupe across Calcite hooks because each gets a different `CallContext` instance.
2. **Planning** — `BridgingSqlFunction.resolveCallTraits` is called from `FlinkLogicalTableFunctionScan.Converter.convert`. It rewrites the operator on the `RexCall` so all downstream readers see the resolved view via plain `function.getTypeInference().getStaticArguments()`.
3. **Compiled-plan restore** — `BridgingSqlFunction.resolveCallTraits` is called again from `StreamExecProcessTableFunction.@JsonCreator`, because the JSON path skips the logical converter. Without this hook, restore would silently produce wrong results for any conditional-trait PTF.

The payoff: downstream rules, exec nodes, codegen, and changelog inference all use ordinary `staticArg.is(SET_SEMANTIC_TABLE)` checks. No consumer needs to know that conditional traits exist. Why three sites and not one. The three resolution points exist because they sit in different lifecycles that can't share state.

## Testing Patterns

Choose test types based on what you're changing:

- **Semantic tests** (for ExecNode/operator changes): Use `SemanticTestBase` (streaming) or `BatchSemanticTestBase` (batch) in `plan/nodes/exec/testutils/`. Extends `CommonSemanticTestBase` which implements `TableTestProgramRunner`. Prefer these over ITCase for operators and ExecNodes.
- **Restore tests** (for stateful operators): Use `RestoreTestBase` or `BatchRestoreTestBase` in `plan/nodes/exec/testutils/`. Implements `TableTestProgramRunner`, uses `@ExtendWith(MiniClusterExtension.class)`. Required when your operator uses state. Tests savepoint creation and job restart in two phases: (1) generate compiled plans + savepoints, (2) verify recovery.
- **Plan tests** (for optimization rules): Verify the generated execution plan using XML golden files. Used for logical and physical optimization rules.
- **ITCase** (for built-in functions): Function tests typically use ITCase with `TestSetSpec` for end-to-end verification (e.g., `JsonFunctionsITCase`, `TimeFunctionsITCase`).
- **JSON plan test regeneration:** Set `PLAN_TEST_FORCE_OVERWRITE=true` environment variable (documented in [README.md](README.md)).
