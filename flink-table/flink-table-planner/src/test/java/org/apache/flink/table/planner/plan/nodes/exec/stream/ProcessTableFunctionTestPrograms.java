/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.flink.table.planner.plan.nodes.exec.stream;

import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.TableRuntimeException;
import org.apache.flink.table.api.config.ExecutionConfigOptions;
import org.apache.flink.table.planner.plan.nodes.exec.stream.ProcessTableFunctionTestUtils.AtomicTypeWrappingFunction;
import org.apache.flink.table.planner.plan.nodes.exec.stream.ProcessTableFunctionTestUtils.ChainedReceivingFunction;
import org.apache.flink.table.planner.plan.nodes.exec.stream.ProcessTableFunctionTestUtils.ChainedSendingFunction;
import org.apache.flink.table.planner.plan.nodes.exec.stream.ProcessTableFunctionTestUtils.ClearStateFunction;
import org.apache.flink.table.planner.plan.nodes.exec.stream.ProcessTableFunctionTestUtils.ContextFunction;
import org.apache.flink.table.planner.plan.nodes.exec.stream.ProcessTableFunctionTestUtils.DescriptorFunction;
import org.apache.flink.table.planner.plan.nodes.exec.stream.ProcessTableFunctionTestUtils.EmptyArgFunction;
import org.apache.flink.table.planner.plan.nodes.exec.stream.ProcessTableFunctionTestUtils.InvalidPassThroughTimersFunction;
import org.apache.flink.table.planner.plan.nodes.exec.stream.ProcessTableFunctionTestUtils.InvalidTableAsRowTimersFunction;
import org.apache.flink.table.planner.plan.nodes.exec.stream.ProcessTableFunctionTestUtils.InvalidUpdatingTimersFunction;
import org.apache.flink.table.planner.plan.nodes.exec.stream.ProcessTableFunctionTestUtils.LateTimersFunction;
import org.apache.flink.table.planner.plan.nodes.exec.stream.ProcessTableFunctionTestUtils.ListStateFunction;
import org.apache.flink.table.planner.plan.nodes.exec.stream.ProcessTableFunctionTestUtils.MapStateFunction;
import org.apache.flink.table.planner.plan.nodes.exec.stream.ProcessTableFunctionTestUtils.MultiStateFunction;
import org.apache.flink.table.planner.plan.nodes.exec.stream.ProcessTableFunctionTestUtils.NamedTimersFunction;
import org.apache.flink.table.planner.plan.nodes.exec.stream.ProcessTableFunctionTestUtils.OptionalOnTimeFunction;
import org.apache.flink.table.planner.plan.nodes.exec.stream.ProcessTableFunctionTestUtils.OptionalPartitionOnTimeFunction;
import org.apache.flink.table.planner.plan.nodes.exec.stream.ProcessTableFunctionTestUtils.PojoArgsFunction;
import org.apache.flink.table.planner.plan.nodes.exec.stream.ProcessTableFunctionTestUtils.PojoCreatingFunction;
import org.apache.flink.table.planner.plan.nodes.exec.stream.ProcessTableFunctionTestUtils.PojoStateFunction;
import org.apache.flink.table.planner.plan.nodes.exec.stream.ProcessTableFunctionTestUtils.PojoStateTimeFunction;
import org.apache.flink.table.planner.plan.nodes.exec.stream.ProcessTableFunctionTestUtils.PojoWithDefaultStateFunction;
import org.apache.flink.table.planner.plan.nodes.exec.stream.ProcessTableFunctionTestUtils.ScalarArgsFunction;
import org.apache.flink.table.planner.plan.nodes.exec.stream.ProcessTableFunctionTestUtils.ScalarArgsTimeFunction;
import org.apache.flink.table.planner.plan.nodes.exec.stream.ProcessTableFunctionTestUtils.TableAsRowFunction;
import org.apache.flink.table.planner.plan.nodes.exec.stream.ProcessTableFunctionTestUtils.TableAsRowPassThroughFunction;
import org.apache.flink.table.planner.plan.nodes.exec.stream.ProcessTableFunctionTestUtils.TableAsSetFunction;
import org.apache.flink.table.planner.plan.nodes.exec.stream.ProcessTableFunctionTestUtils.TableAsSetOptionalPartitionFunction;
import org.apache.flink.table.planner.plan.nodes.exec.stream.ProcessTableFunctionTestUtils.TableAsSetPassThroughFunction;
import org.apache.flink.table.planner.plan.nodes.exec.stream.ProcessTableFunctionTestUtils.TableAsSetUpdatingArgFunction;
import org.apache.flink.table.planner.plan.nodes.exec.stream.ProcessTableFunctionTestUtils.TimeConversionsFunction;
import org.apache.flink.table.planner.plan.nodes.exec.stream.ProcessTableFunctionTestUtils.TimeToLiveStateFunction;
import org.apache.flink.table.planner.plan.nodes.exec.stream.ProcessTableFunctionTestUtils.TypedTableAsRowFunction;
import org.apache.flink.table.planner.plan.nodes.exec.stream.ProcessTableFunctionTestUtils.TypedTableAsSetFunction;
import org.apache.flink.table.planner.plan.nodes.exec.stream.ProcessTableFunctionTestUtils.UnnamedTimersFunction;
import org.apache.flink.table.test.program.SinkTestStep;
import org.apache.flink.table.test.program.SourceTestStep;
import org.apache.flink.table.test.program.TableTestProgram;
import org.apache.flink.types.Row;

import java.time.Duration;
import java.time.LocalDateTime;

import static org.apache.flink.table.api.Expressions.$;
import static org.apache.flink.table.api.Expressions.descriptor;
import static org.apache.flink.table.api.Expressions.lit;
import static org.apache.flink.table.planner.plan.nodes.exec.stream.ProcessTableFunctionTestUtils.BASE_SINK_SCHEMA;
import static org.apache.flink.table.planner.plan.nodes.exec.stream.ProcessTableFunctionTestUtils.BASIC_VALUES;
import static org.apache.flink.table.planner.plan.nodes.exec.stream.ProcessTableFunctionTestUtils.KEYED_BASE_SINK_SCHEMA;
import static org.apache.flink.table.planner.plan.nodes.exec.stream.ProcessTableFunctionTestUtils.KEYED_TIMED_BASE_SINK_SCHEMA;
import static org.apache.flink.table.planner.plan.nodes.exec.stream.ProcessTableFunctionTestUtils.MULTI_VALUES;
import static org.apache.flink.table.planner.plan.nodes.exec.stream.ProcessTableFunctionTestUtils.PASS_THROUGH_BASE_SINK_SCHEMA;
import static org.apache.flink.table.planner.plan.nodes.exec.stream.ProcessTableFunctionTestUtils.TIMED_BASE_SINK_SCHEMA;
import static org.apache.flink.table.planner.plan.nodes.exec.stream.ProcessTableFunctionTestUtils.TIMED_SOURCE;
import static org.apache.flink.table.planner.plan.nodes.exec.stream.ProcessTableFunctionTestUtils.TIMED_SOURCE_LATE_EVENTS;
import static org.apache.flink.table.planner.plan.nodes.exec.stream.ProcessTableFunctionTestUtils.UPDATING_VALUES;

/** {@link TableTestProgram} definitions for testing {@link StreamExecProcessTableFunction}. */
public class ProcessTableFunctionTestPrograms {

    public static final TableTestProgram PROCESS_SCALAR_ARGS =
            TableTestProgram.of("process-scalar-args", "no table as input")
                    .setupTemporarySystemFunction("f", ScalarArgsFunction.class)
                    .setupTableSink(
                            SinkTestStep.newBuilder("sink")
                                    .addSchema(BASE_SINK_SCHEMA)
                                    .consumedValues("+I[{42, true}]")
                                    .build())
                    .runSql(
                            "INSERT INTO sink SELECT * FROM f(i => 42, b => CAST('TRUE' AS BOOLEAN))")
                    .build();

    public static final TableTestProgram PROCESS_SCALAR_ARGS_TABLE_API =
            TableTestProgram.of("process-scalar-args-table-api", "no table as input")
                    .setupTableSink(
                            SinkTestStep.newBuilder("sink")
                                    .addSchema(BASE_SINK_SCHEMA)
                                    .consumedValues("+I[{42, true}]")
                                    .build())
                    .runTableApi(
                            env ->
                                    env.fromCall(
                                            ScalarArgsFunction.class,
                                            lit(42).asArgument("i"),
                                            lit(true).asArgument("b")),
                            "sink")
                    .build();

    public static final TableTestProgram PROCESS_TABLE_AS_ROW =
            TableTestProgram.of("process-row", "table with row semantics")
                    .setupTemporarySystemFunction("f", TableAsRowFunction.class)
                    .setupSql(BASIC_VALUES)
                    .setupTableSink(
                            SinkTestStep.newBuilder("sink")
                                    .addSchema(BASE_SINK_SCHEMA)
                                    .consumedValues(
                                            "+I[{+I[Bob, 12], 1}]", "+I[{+I[Alice, 42], 1}]")
                                    .build())
                    .runSql("INSERT INTO sink SELECT * FROM f(r => TABLE t, i => 1)")
                    .build();

    public static final TableTestProgram PROCESS_TABLE_AS_ROW_TABLE_API =
            TableTestProgram.of("process-row-table-api", "table with row semantics")
                    .setupSql(BASIC_VALUES)
                    .setupTableSink(
                            SinkTestStep.newBuilder("sink")
                                    .addSchema(BASE_SINK_SCHEMA)
                                    .consumedValues(
                                            "+I[{+I[Bob, 12], 1}]", "+I[{+I[Alice, 42], 1}]")
                                    .build())
                    .runTableApi(
                            env ->
                                    env.fromCall(
                                            TableAsRowFunction.class,
                                            env.from("t").asArgument("r"),
                                            lit(1).asArgument("i")),
                            "sink")
                    .build();

    public static final TableTestProgram PROCESS_TABLE_AS_ROW_TABLE_API_INLINE =
            TableTestProgram.of(
                            "process-row-table-api-inline",
                            "tests the inline Table.process() position-based")
                    .setupSql(BASIC_VALUES)
                    .setupTableSink(
                            SinkTestStep.newBuilder("sink")
                                    .addSchema(BASE_SINK_SCHEMA)
                                    .consumedValues(
                                            "+I[{+I[Bob, 12], 1}]", "+I[{+I[Alice, 42], 1}]")
                                    .build())
                    .runTableApi(env -> env.from("t").process(TableAsRowFunction.class, 1), "sink")
                    .build();

    public static final TableTestProgram PROCESS_TABLE_AS_ROW_TABLE_API_INLINE_NAMED =
            TableTestProgram.of(
                            "process-row-table-api-inline-named",
                            "tests the inline Table.process() name-based")
                    .setupSql(BASIC_VALUES)
                    .setupTableSink(
                            SinkTestStep.newBuilder("sink")
                                    .addSchema(BASE_SINK_SCHEMA)
                                    .consumedValues(
                                            "+I[{+I[Bob, 12], 1}]", "+I[{+I[Alice, 42], 1}]")
                                    .build())
                    .runTableApi(
                            env ->
                                    env.from("t")
                                            .process(
                                                    TableAsRowFunction.class,
                                                    lit(1).asArgument("i")),
                            "sink")
                    .build();

    public static final TableTestProgram PROCESS_TYPED_TABLE_AS_ROW =
            TableTestProgram.of("process-typed-row", "typed table with row semantics")
                    .setupTemporarySystemFunction("f", TypedTableAsRowFunction.class)
                    .setupSql(BASIC_VALUES)
                    .setupTableSink(
                            SinkTestStep.newBuilder("sink")
                                    .addSchema(BASE_SINK_SCHEMA)
                                    .consumedValues(
                                            "+I[{User(s='Bob', i=12), 1}]",
                                            "+I[{User(s='Alice', i=42), 1}]")
                                    .build())
                    .runSql("INSERT INTO sink SELECT * FROM f(u => TABLE t, i => 1)")
                    .build();

    public static final TableTestProgram PROCESS_TYPED_TABLE_AS_ROW_TABLE_API =
            TableTestProgram.of("process-typed-row-table-api", "typed table with row semantics")
                    .setupSql(BASIC_VALUES)
                    .setupTableSink(
                            SinkTestStep.newBuilder("sink")
                                    .addSchema(BASE_SINK_SCHEMA)
                                    .consumedValues(
                                            "+I[{User(s='Bob', i=12), 1}]",
                                            "+I[{User(s='Alice', i=42), 1}]")
                                    .build())
                    .runTableApi(
                            env ->
                                    env.fromCall(
                                            TypedTableAsRowFunction.class,
                                            env.from("t").asArgument("u"),
                                            lit(1).asArgument("i")),
                            "sink")
                    .build();

    public static final TableTestProgram PROCESS_TABLE_AS_SET =
            TableTestProgram.of("process-set", "table with set semantics")
                    .setupTemporarySystemFunction("f", TableAsSetFunction.class)
                    .setupSql(BASIC_VALUES)
                    .setupTableSink(
                            SinkTestStep.newBuilder("sink")
                                    .addSchema(KEYED_BASE_SINK_SCHEMA)
                                    .consumedValues(
                                            "+I[Bob, {+I[Bob, 12], 1}]",
                                            "+I[Alice, {+I[Alice, 42], 1}]")
                                    .build())
                    .runSql(
                            "INSERT INTO sink SELECT * FROM f(r => TABLE t PARTITION BY name, i => 1)")
                    .build();

    public static final TableTestProgram PROCESS_TABLE_AS_SET_TABLE_API =
            TableTestProgram.of("process-set-table-api", "table with set semantics")
                    .setupSql(BASIC_VALUES)
                    .setupTableSink(
                            SinkTestStep.newBuilder("sink")
                                    .addSchema(KEYED_BASE_SINK_SCHEMA)
                                    .consumedValues(
                                            "+I[Bob, {+I[Bob, 12], 1}]",
                                            "+I[Alice, {+I[Alice, 42], 1}]")
                                    .build())
                    .runTableApi(
                            env ->
                                    env.fromCall(
                                            TableAsSetFunction.class,
                                            env.from("t").partitionBy($("name")).asArgument("r"),
                                            lit(1).asArgument("i")),
                            "sink")
                    .build();

    public static final TableTestProgram PROCESS_TABLE_AS_SET_TABLE_API_INLINE =
            TableTestProgram.of(
                            "process-set-table-api-inline",
                            "tests the inline Table.process() position-based")
                    .setupSql(BASIC_VALUES)
                    .setupTableSink(
                            SinkTestStep.newBuilder("sink")
                                    .addSchema(KEYED_BASE_SINK_SCHEMA)
                                    .consumedValues(
                                            "+I[Bob, {+I[Bob, 12], 1}]",
                                            "+I[Alice, {+I[Alice, 42], 1}]")
                                    .build())
                    .runTableApi(
                            env ->
                                    env.from("t")
                                            .partitionBy($("name"))
                                            .process(TableAsSetFunction.class, 1),
                            "sink")
                    .build();

    public static final TableTestProgram PROCESS_TABLE_AS_SET_TABLE_API_INLINE_NAMED =
            TableTestProgram.of(
                            "process-set-table-api-inline-named",
                            "tests the inline Table.process() name-based")
                    .setupSql(BASIC_VALUES)
                    .setupTableSink(
                            SinkTestStep.newBuilder("sink")
                                    .addSchema(KEYED_BASE_SINK_SCHEMA)
                                    .consumedValues(
                                            "+I[Bob, {+I[Bob, 12], 1}]",
                                            "+I[Alice, {+I[Alice, 42], 1}]")
                                    .build())
                    .runTableApi(
                            env ->
                                    env.from("t")
                                            .partitionBy($("name"))
                                            .process(
                                                    TableAsSetFunction.class,
                                                    lit(1).asArgument("i")),
                            "sink")
                    .build();

    public static final TableTestProgram PROCESS_TYPED_TABLE_AS_SET =
            TableTestProgram.of("process-typed-set", "typed table with set semantics")
                    .setupTemporarySystemFunction("f", TypedTableAsSetFunction.class)
                    .setupSql(BASIC_VALUES)
                    .setupTableSink(
                            SinkTestStep.newBuilder("sink")
                                    .addSchema(KEYED_BASE_SINK_SCHEMA)
                                    .consumedValues(
                                            "+I[Bob, {User(s='Bob', i=12), 1}]",
                                            "+I[Alice, {User(s='Alice', i=42), 1}]")
                                    .build())
                    .runSql(
                            "INSERT INTO sink SELECT * FROM f(u => TABLE t PARTITION BY name, i => 1)")
                    .build();

    public static final TableTestProgram PROCESS_TYPED_TABLE_AS_SET_TABLE_API =
            TableTestProgram.of("process-typed-set-table-api", "typed table with set semantics")
                    .setupSql(BASIC_VALUES)
                    .setupTableSink(
                            SinkTestStep.newBuilder("sink")
                                    .addSchema(KEYED_BASE_SINK_SCHEMA)
                                    .consumedValues(
                                            "+I[Bob, {User(s='Bob', i=12), 1}]",
                                            "+I[Alice, {User(s='Alice', i=42), 1}]")
                                    .build())
                    .runTableApi(
                            env ->
                                    env.fromCall(
                                            TypedTableAsSetFunction.class,
                                            env.from("t").partitionBy($("name")).asArgument("u"),
                                            lit(1).asArgument("i")),
                            "sink")
                    .build();

    public static final TableTestProgram PROCESS_POJO_ARGS =
            TableTestProgram.of("process-pojo-args", "POJOs for both table and scalar argument")
                    .setupTemporarySystemFunction("f", PojoArgsFunction.class)
                    .setupTemporarySystemFunction("pojoCreator", PojoCreatingFunction.class)
                    .setupSql(BASIC_VALUES)
                    .setupTableSink(
                            SinkTestStep.newBuilder("sink")
                                    .addSchema(BASE_SINK_SCHEMA)
                                    .consumedValues(
                                            "+I[{User(s='Bob', i=12), User(s='Bob', i=12)}]",
                                            "+I[{User(s='Alice', i=42), User(s='Bob', i=12)}]")
                                    .build())
                    .runSql(
                            "INSERT INTO sink SELECT * FROM "
                                    + "f(input => TABLE t, scalar => pojoCreator('Bob', 12))")
                    .build();

    public static final TableTestProgram PROCESS_EMPTY_ARGS =
            TableTestProgram.of("process-empty-args", "no arguments")
                    .setupTemporarySystemFunction("f", EmptyArgFunction.class)
                    .setupSql(BASIC_VALUES)
                    .setupTableSink(
                            SinkTestStep.newBuilder("sink")
                                    .addSchema(BASE_SINK_SCHEMA)
                                    .consumedValues("+I[{empty}]")
                                    .build())
                    .runSql("INSERT INTO sink SELECT * FROM f()")
                    .build();

    public static final TableTestProgram PROCESS_TABLE_AS_ROW_PASS_THROUGH =
            TableTestProgram.of("process-row-pass-through", "pass columns through enabled")
                    .setupTemporarySystemFunction("f", TableAsRowPassThroughFunction.class)
                    .setupSql(BASIC_VALUES)
                    .setupTableSink(
                            SinkTestStep.newBuilder("sink")
                                    .addSchema(PASS_THROUGH_BASE_SINK_SCHEMA)
                                    .consumedValues(
                                            "+I[Bob, 12, {+I[Bob, 12], 1}]",
                                            "+I[Alice, 42, {+I[Alice, 42], 1}]")
                                    .build())
                    .runSql("INSERT INTO sink SELECT * FROM f(r => TABLE t, i => 1)")
                    .build();

    public static final TableTestProgram PROCESS_TABLE_AS_SET_PASS_THROUGH =
            TableTestProgram.of("process-set-pass-through", "pass columns through enabled")
                    .setupTemporarySystemFunction("f", TableAsSetPassThroughFunction.class)
                    .setupSql(BASIC_VALUES)
                    .setupTableSink(
                            SinkTestStep.newBuilder("sink")
                                    .addSchema(PASS_THROUGH_BASE_SINK_SCHEMA)
                                    .consumedValues(
                                            "+I[Bob, 12, {+I[Bob, 12], 1}]",
                                            "+I[Alice, 42, {+I[Alice, 42], 1}]")
                                    .build())
                    .runSql(
                            "INSERT INTO sink SELECT * FROM f(r => TABLE t PARTITION BY name, i => 1)")
                    .build();

    public static final TableTestProgram PROCESS_UPDATING_INPUT =
            TableTestProgram.of("process-updating-input", "table argument accepts retractions")
                    .setupTemporarySystemFunction("f", TableAsSetUpdatingArgFunction.class)
                    .setupSql(UPDATING_VALUES)
                    .setupTableSink(
                            SinkTestStep.newBuilder("sink")
                                    .addSchema(BASE_SINK_SCHEMA)
                                    .consumedValues(
                                            "+I[{+I[Bob, 1]}]",
                                            "+I[{+I[Alice, 1]}]",
                                            "+I[{-U[Bob, 1]}]",
                                            "+I[{+U[Bob, 2]}]")
                                    .build())
                    .runSql("INSERT INTO sink SELECT * FROM f(r => TABLE t)")
                    .build();

    public static final TableTestProgram PROCESS_OPTIONAL_PARTITION_BY =
            TableTestProgram.of("process-optional-partition-by", "no partition by")
                    .setupTemporarySystemFunction("f", TableAsSetOptionalPartitionFunction.class)
                    .setupSql(BASIC_VALUES)
                    .setupTableSink(
                            SinkTestStep.newBuilder("sink")
                                    .addSchema(BASE_SINK_SCHEMA)
                                    .consumedValues(
                                            "+I[{+I[Bob, 12], 1}]", "+I[{+I[Alice, 42], 1}]")
                                    .build())
                    .runSql("INSERT INTO sink SELECT * FROM f(r => TABLE t, i => 1)")
                    .build();

    public static final TableTestProgram PROCESS_OPTIONAL_PARTITION_BY_TABLE_API =
            TableTestProgram.of("process-optional-partition-by-table-api", "no partition by")
                    .setupSql(BASIC_VALUES)
                    .setupTableSink(
                            SinkTestStep.newBuilder("sink")
                                    .addSchema(BASE_SINK_SCHEMA)
                                    .consumedValues(
                                            "+I[{+I[Bob, 12], 1}]", "+I[{+I[Alice, 42], 1}]")
                                    .build())
                    .runTableApi(
                            env ->
                                    env.fromCall(
                                            TableAsSetOptionalPartitionFunction.class,
                                            env.from("t").asArgument("r"),
                                            lit(1).asArgument("i")),
                            "sink")
                    .build();

    public static final TableTestProgram PROCESS_ATOMIC_WRAPPING =
            TableTestProgram.of("process-set-atomic-wrapping", "wrap atomic type into row")
                    .setupTemporarySystemFunction("f", AtomicTypeWrappingFunction.class)
                    .setupSql(BASIC_VALUES)
                    .setupTableSink(
                            SinkTestStep.newBuilder("sink")
                                    .addSchema("`name` STRING, `atomic` INT")
                                    .consumedValues("+I[Bob, 12]", "+I[Alice, 42]")
                                    .build())
                    .runSql("INSERT INTO sink SELECT * FROM f(r => TABLE t PARTITION BY name)")
                    .build();

    public static final TableTestProgram PROCESS_CONTEXT =
            TableTestProgram.of("process-context", "outputs values from function context")
                    .setupTemporarySystemFunction("f", ContextFunction.class)
                    .setupSql(BASIC_VALUES)
                    .setupTableSink(
                            SinkTestStep.newBuilder("sink")
                                    .addSchema(KEYED_BASE_SINK_SCHEMA)
                                    .consumedValues(
                                            "+I[Bob, {+I[Bob, 12], param, [0], [INSERT], ROW<`name` VARCHAR(5) NOT NULL, `score` INT NOT NULL>}]",
                                            "+I[Alice, {+I[Alice, 42], param, [0], [INSERT], ROW<`name` VARCHAR(5) NOT NULL, `score` INT NOT NULL>}]")
                                    .build())
                    .runSql(
                            "INSERT INTO sink SELECT * FROM f(r => TABLE t PARTITION BY name, s => 'param')")
                    .build();

    public static final TableTestProgram PROCESS_POJO_STATE =
            TableTestProgram.of("process-pojo-state", "single POJO state entry")
                    .setupTemporarySystemFunction("f", PojoStateFunction.class)
                    .setupSql(MULTI_VALUES)
                    .setupTableSink(
                            SinkTestStep.newBuilder("sink")
                                    .addSchema(KEYED_BASE_SINK_SCHEMA)
                                    .consumedValues(
                                            "+I[Bob, {Score(s='null', i=null), +I[Bob, 12]}]",
                                            "+I[Alice, {Score(s='null', i=null), +I[Alice, 42]}]",
                                            "+I[Bob, {Score(s='Bob', i=0), +I[Bob, 99]}]",
                                            "+I[Bob, {Score(s='Bob', i=1), +I[Bob, 100]}]",
                                            "+I[Alice, {Score(s='null', i=0), +I[Alice, 400]}]")
                                    .build())
                    .runSql("INSERT INTO sink SELECT * FROM f(r => TABLE t PARTITION BY name)")
                    .build();

    public static final TableTestProgram PROCESS_DEFAULT_POJO_STATE =
            TableTestProgram.of(
                            "process-default-pojo-state",
                            "single POJO state entry that is initialized with values")
                    .setupTemporarySystemFunction("f", PojoWithDefaultStateFunction.class)
                    .setupSql(MULTI_VALUES)
                    .setupTableSink(
                            SinkTestStep.newBuilder("sink")
                                    .addSchema(KEYED_BASE_SINK_SCHEMA)
                                    .consumedValues(
                                            "+I[Bob, {ScoreWithDefaults(s='null', i=99), +I[Bob, 12]}]",
                                            "+I[Alice, {ScoreWithDefaults(s='null', i=99), +I[Alice, 42]}]",
                                            "+I[Bob, {ScoreWithDefaults(s='Bob', i=0), +I[Bob, 99]}]",
                                            "+I[Bob, {ScoreWithDefaults(s='Bob', i=1), +I[Bob, 100]}]",
                                            "+I[Alice, {ScoreWithDefaults(s='null', i=0), +I[Alice, 400]}]")
                                    .build())
                    .runSql("INSERT INTO sink SELECT * FROM f(r => TABLE t PARTITION BY name)")
                    .build();

    public static final TableTestProgram PROCESS_MULTI_STATE =
            TableTestProgram.of("process-multi-state", "multiple state entries")
                    .setupTemporarySystemFunction("f", MultiStateFunction.class)
                    .setupSql(MULTI_VALUES)
                    .setupTableSink(
                            SinkTestStep.newBuilder("sink")
                                    .addSchema(KEYED_BASE_SINK_SCHEMA)
                                    .consumedValues(
                                            "+I[Bob, {+I[null], +I[null], +I[Bob, 12]}]",
                                            "+I[Alice, {+I[null], +I[null], +I[Alice, 42]}]",
                                            "+I[Bob, {+I[1], +I[0], +I[Bob, 99]}]",
                                            "+I[Bob, {+I[2], +I[1], +I[Bob, 100]}]",
                                            "+I[Alice, {+I[1], +I[0], +I[Alice, 400]}]")
                                    .build())
                    .runSql("INSERT INTO sink SELECT * FROM f(r => TABLE t PARTITION BY name)")
                    .build();

    public static final TableTestProgram PROCESS_CLEARING_STATE =
            TableTestProgram.of(
                            "process-clearing-state", "state for Bob is cleared after second row")
                    .setupTemporarySystemFunction("f", ClearStateFunction.class)
                    .setupSql(MULTI_VALUES)
                    .setupTableSink(
                            SinkTestStep.newBuilder("sink")
                                    .addSchema(KEYED_BASE_SINK_SCHEMA)
                                    .consumedValues(
                                            "+I[Bob, {ScoreWithDefaults(s='null', i=99), +I[Bob, 12]}]",
                                            "+I[Alice, {ScoreWithDefaults(s='null', i=99), +I[Alice, 42]}]",
                                            "+I[Bob, {ScoreWithDefaults(s='null', i=100), +I[Bob, 99]}]",
                                            "+I[Bob, {ScoreWithDefaults(s='null', i=99), +I[Bob, 100]}]",
                                            "+I[Alice, {ScoreWithDefaults(s='null', i=100), +I[Alice, 400]}]")
                                    .build())
                    .runSql("INSERT INTO sink SELECT * FROM f(r => TABLE t PARTITION BY name)")
                    .build();

    public static final TableTestProgram PROCESS_STATE_TTL =
            TableTestProgram.of(
                            "process-state-ttl",
                            "state TTL with custom, disabled, and global retention")
                    .setupTemporarySystemFunction("f", TimeToLiveStateFunction.class)
                    .setupSql(MULTI_VALUES)
                    .setupConfig(ExecutionConfigOptions.IDLE_STATE_RETENTION, Duration.ofHours(5))
                    .setupTableSink(
                            SinkTestStep.newBuilder("sink")
                                    .addSchema("ttl STRING")
                                    .consumedValues(
                                            "+I["
                                                    + "s1=StateTtlConfig{updateType=OnCreateAndWrite, stateVisibility=NeverReturnExpired, ttlTimeCharacteristic=ProcessingTime, ttl=PT120H}, "
                                                    + "s2=StateTtlConfig{updateType=Disabled, stateVisibility=NeverReturnExpired, ttlTimeCharacteristic=ProcessingTime, ttl=PT2562047788015H12M55.807S}, "
                                                    + "s3=StateTtlConfig{updateType=OnCreateAndWrite, stateVisibility=NeverReturnExpired, ttlTimeCharacteristic=ProcessingTime, ttl=PT5H}"
                                                    + "]")
                                    .build())
                    .runSql("INSERT INTO sink SELECT * FROM f(r => TABLE t)")
                    .build();

    public static final TableTestProgram PROCESS_DESCRIPTOR =
            TableTestProgram.of(
                            "process-descriptor",
                            "takes nullable, optional, and not nullable DESCRIPTOR() arguments")
                    .setupTemporarySystemFunction("f", DescriptorFunction.class)
                    .setupSql(BASIC_VALUES)
                    .setupTableSink(
                            SinkTestStep.newBuilder("sink")
                                    .addSchema(BASE_SINK_SCHEMA)
                                    .consumedValues("+I[{null, null, (`a`, `b`, `c`)}]")
                                    .build())
                    .runSql(
                            "INSERT INTO sink SELECT * FROM f(columnList1 => NULL, columnList3 => DESCRIPTOR(a, b, c))")
                    .build();

    public static final TableTestProgram PROCESS_TIME_CONVERSIONS =
            TableTestProgram.of(
                            "process-time-conversions",
                            "test all support conversion classes for both time and watermarks")
                    .setupTemporarySystemFunction("f", TimeConversionsFunction.class)
                    .setupTableSource(TIMED_SOURCE)
                    .setupTableSink(
                            SinkTestStep.newBuilder("sink")
                                    .addSchema(TIMED_BASE_SINK_SCHEMA)
                                    .consumedValues(
                                            "+I[{Time (Long: 0, Instant: 1970-01-01T00:00:00Z, LocalDateTime: 1970-01-01T00:00), "
                                                    + "Watermark (Long: null, Instant: null, LocalDateTime: null)}, 1970-01-01T00:00:00Z]",
                                            "+I[{Time (Long: 1, Instant: 1970-01-01T00:00:00.001Z, LocalDateTime: 1970-01-01T00:00:00.001), "
                                                    + "Watermark (Long: -1, Instant: 1969-12-31T23:59:59.999Z, LocalDateTime: 1969-12-31T23:59:59.999)}, 1970-01-01T00:00:00.001Z]",
                                            "+I[{Time (Long: 2, Instant: 1970-01-01T00:00:00.002Z, LocalDateTime: 1970-01-01T00:00:00.002), "
                                                    + "Watermark (Long: 0, Instant: 1970-01-01T00:00:00Z, LocalDateTime: 1970-01-01T00:00)}, 1970-01-01T00:00:00.002Z]",
                                            "+I[{Time (Long: 3, Instant: 1970-01-01T00:00:00.003Z, LocalDateTime: 1970-01-01T00:00:00.003), "
                                                    + "Watermark (Long: 1, Instant: 1970-01-01T00:00:00.001Z, LocalDateTime: 1970-01-01T00:00:00.001)}, 1970-01-01T00:00:00.003Z]",
                                            "+I[{Time (Long: 4, Instant: 1970-01-01T00:00:00.004Z, LocalDateTime: 1970-01-01T00:00:00.004), "
                                                    + "Watermark (Long: 2, Instant: 1970-01-01T00:00:00.002Z, LocalDateTime: 1970-01-01T00:00:00.002)}, 1970-01-01T00:00:00.004Z]",
                                            "+I[{Time (Long: 5, Instant: 1970-01-01T00:00:00.005Z, LocalDateTime: 1970-01-01T00:00:00.005),"
                                                    + " Watermark (Long: 3, Instant: 1970-01-01T00:00:00.003Z, LocalDateTime: 1970-01-01T00:00:00.003)}, 1970-01-01T00:00:00.005Z]",
                                            "+I[{Time (Long: 6, Instant: 1970-01-01T00:00:00.006Z, LocalDateTime: 1970-01-01T00:00:00.006), "
                                                    + "Watermark (Long: 4, Instant: 1970-01-01T00:00:00.004Z, LocalDateTime: 1970-01-01T00:00:00.004)}, 1970-01-01T00:00:00.006Z]")
                                    .build())
                    .runSql(
                            "INSERT INTO sink SELECT * FROM f(r => TABLE t, on_time => DESCRIPTOR(ts))")
                    .build();

    public static final TableTestProgram PROCESS_TIME_CONVERSIONS_TABLE_API =
            TableTestProgram.of(
                            "process-time-conversions-table-api",
                            "test all support conversion classes for both time and watermarks")
                    .setupTableSource(TIMED_SOURCE)
                    .setupTableSink(
                            SinkTestStep.newBuilder("sink")
                                    .addSchema(TIMED_BASE_SINK_SCHEMA)
                                    .consumedValues(
                                            "+I[{Time (Long: 0, Instant: 1970-01-01T00:00:00Z, LocalDateTime: 1970-01-01T00:00), "
                                                    + "Watermark (Long: null, Instant: null, LocalDateTime: null)}, 1970-01-01T00:00:00Z]",
                                            "+I[{Time (Long: 1, Instant: 1970-01-01T00:00:00.001Z, LocalDateTime: 1970-01-01T00:00:00.001), "
                                                    + "Watermark (Long: -1, Instant: 1969-12-31T23:59:59.999Z, LocalDateTime: 1969-12-31T23:59:59.999)}, 1970-01-01T00:00:00.001Z]",
                                            "+I[{Time (Long: 2, Instant: 1970-01-01T00:00:00.002Z, LocalDateTime: 1970-01-01T00:00:00.002), "
                                                    + "Watermark (Long: 0, Instant: 1970-01-01T00:00:00Z, LocalDateTime: 1970-01-01T00:00)}, 1970-01-01T00:00:00.002Z]",
                                            "+I[{Time (Long: 3, Instant: 1970-01-01T00:00:00.003Z, LocalDateTime: 1970-01-01T00:00:00.003), "
                                                    + "Watermark (Long: 1, Instant: 1970-01-01T00:00:00.001Z, LocalDateTime: 1970-01-01T00:00:00.001)}, 1970-01-01T00:00:00.003Z]",
                                            "+I[{Time (Long: 4, Instant: 1970-01-01T00:00:00.004Z, LocalDateTime: 1970-01-01T00:00:00.004), "
                                                    + "Watermark (Long: 2, Instant: 1970-01-01T00:00:00.002Z, LocalDateTime: 1970-01-01T00:00:00.002)}, 1970-01-01T00:00:00.004Z]",
                                            "+I[{Time (Long: 5, Instant: 1970-01-01T00:00:00.005Z, LocalDateTime: 1970-01-01T00:00:00.005),"
                                                    + " Watermark (Long: 3, Instant: 1970-01-01T00:00:00.003Z, LocalDateTime: 1970-01-01T00:00:00.003)}, 1970-01-01T00:00:00.005Z]",
                                            "+I[{Time (Long: 6, Instant: 1970-01-01T00:00:00.006Z, LocalDateTime: 1970-01-01T00:00:00.006), "
                                                    + "Watermark (Long: 4, Instant: 1970-01-01T00:00:00.004Z, LocalDateTime: 1970-01-01T00:00:00.004)}, 1970-01-01T00:00:00.006Z]")
                                    .build())
                    .runTableApi(
                            env ->
                                    env.fromCall(
                                            TimeConversionsFunction.class,
                                            env.from("t").asArgument("r"),
                                            descriptor("ts").asArgument("on_time")),
                            "sink")
                    .build();

    public static final TableTestProgram PROCESS_REGULAR_TIMESTAMP =
            TableTestProgram.of(
                            "process-regular-timestamp",
                            "tests regular TIMESTAMP type instead of TIMESTAMP_LTZ")
                    .setupTemporarySystemFunction("f", TimeConversionsFunction.class)
                    .setupTableSource(
                            SourceTestStep.newBuilder("t")
                                    .addSchema(
                                            "name STRING",
                                            "score INT",
                                            "ts TIMESTAMP(3)",
                                            "WATERMARK FOR ts AS ts - INTERVAL '1' HOUR")
                                    .producedValues(
                                            Row.of("Bob", 1, LocalDateTime.of(2025, 1, 1, 1, 0)),
                                            Row.of("Alice", 1, LocalDateTime.of(2025, 1, 1, 2, 0)),
                                            Row.of("Bob", 2, LocalDateTime.of(2025, 1, 1, 3, 0)),
                                            Row.of("Bob", 3, LocalDateTime.of(2025, 1, 1, 4, 0)),
                                            Row.of("Bob", 4, LocalDateTime.of(2025, 1, 1, 5, 0)),
                                            Row.of("Bob", 5, LocalDateTime.of(2025, 1, 1, 6, 0)),
                                            Row.of("Bob", 6, LocalDateTime.of(2025, 1, 1, 7, 0)))
                                    .build())
                    .setupTableSink(
                            SinkTestStep.newBuilder("sink")
                                    .addSchema("`out` STRING, `rowtime` TIMESTAMP(3)")
                                    .consumedValues(
                                            "+I[{Time (Long: 1735693200000, Instant: 2025-01-01T01:00:00Z, LocalDateTime: 2025-01-01T01:00), Watermark (Long: null, Instant: null, LocalDateTime: null)}, 2025-01-01T01:00]",
                                            "+I[{Time (Long: 1735696800000, Instant: 2025-01-01T02:00:00Z, LocalDateTime: 2025-01-01T02:00), Watermark (Long: 1735689600000, Instant: 2025-01-01T00:00:00Z, LocalDateTime: 2025-01-01T00:00)}, 2025-01-01T02:00]",
                                            "+I[{Time (Long: 1735700400000, Instant: 2025-01-01T03:00:00Z, LocalDateTime: 2025-01-01T03:00), Watermark (Long: 1735693200000, Instant: 2025-01-01T01:00:00Z, LocalDateTime: 2025-01-01T01:00)}, 2025-01-01T03:00]",
                                            "+I[{Time (Long: 1735704000000, Instant: 2025-01-01T04:00:00Z, LocalDateTime: 2025-01-01T04:00), Watermark (Long: 1735696800000, Instant: 2025-01-01T02:00:00Z, LocalDateTime: 2025-01-01T02:00)}, 2025-01-01T04:00]",
                                            "+I[{Time (Long: 1735707600000, Instant: 2025-01-01T05:00:00Z, LocalDateTime: 2025-01-01T05:00), Watermark (Long: 1735700400000, Instant: 2025-01-01T03:00:00Z, LocalDateTime: 2025-01-01T03:00)}, 2025-01-01T05:00]",
                                            "+I[{Time (Long: 1735711200000, Instant: 2025-01-01T06:00:00Z, LocalDateTime: 2025-01-01T06:00), Watermark (Long: 1735704000000, Instant: 2025-01-01T04:00:00Z, LocalDateTime: 2025-01-01T04:00)}, 2025-01-01T06:00]",
                                            "+I[{Time (Long: 1735714800000, Instant: 2025-01-01T07:00:00Z, LocalDateTime: 2025-01-01T07:00), Watermark (Long: 1735707600000, Instant: 2025-01-01T05:00:00Z, LocalDateTime: 2025-01-01T05:00)}, 2025-01-01T07:00]")
                                    .build())
                    .runSql(
                            "INSERT INTO sink SELECT * FROM f(r => TABLE t, on_time => DESCRIPTOR(ts))")
                    .build();

    public static final TableTestProgram PROCESS_NAMED_TIMERS =
            TableTestProgram.of(
                            "process-partitioned-named-timers",
                            "test create/fire/replace/clear/clear-all named timers")
                    .setupTemporarySystemFunction("f", NamedTimersFunction.class)
                    .setupTableSource(TIMED_SOURCE)
                    .setupTableSink(
                            SinkTestStep.newBuilder("sink")
                                    .addSchema(KEYED_TIMED_BASE_SINK_SCHEMA)
                                    .consumedValues(
                                            "+I[Bob, {Processing input row +I[Bob, 1, 1970-01-01T00:00:00Z] at time 0 watermark null}, 1970-01-01T00:00:00Z]",
                                            "+I[Bob, {Registering timer timeout1 for 1 at time 0 watermark null}, 1970-01-01T00:00:00Z]",
                                            "+I[Bob, {Registering timer timeout2 for 2 at time 0 watermark null}, 1970-01-01T00:00:00Z]",
                                            "+I[Bob, {Registering timer timeout3 for 3 at time 0 watermark null}, 1970-01-01T00:00:00Z]",
                                            "+I[Bob, {Registering timer timeout4 for 9223372036854775807 at time 0 watermark null}, 1970-01-01T00:00:00Z]",
                                            "+I[Bob, {Registering timer timeout5 for 9223372036854775807 at time 0 watermark null}, 1970-01-01T00:00:00Z]",
                                            "+I[Alice, {Processing input row +I[Alice, 1, 1970-01-01T00:00:00.001Z] at time 1 watermark -1}, 1970-01-01T00:00:00.001Z]",
                                            "+I[Bob, {Processing input row +I[Bob, 2, 1970-01-01T00:00:00.002Z] at time 2 watermark 0}, 1970-01-01T00:00:00.002Z]",
                                            "+I[Bob, {Timer timeout1 fired at time 1 watermark 1}, 1970-01-01T00:00:00.001Z]",
                                            "+I[Bob, {Registering timer timeout2 for 5 at time 1 watermark 1}, 1970-01-01T00:00:00.001Z]",
                                            "+I[Bob, {Clearing timer timeout3 at time 1 watermark 1}, 1970-01-01T00:00:00.001Z]",
                                            "+I[Bob, {Processing input row +I[Bob, 3, 1970-01-01T00:00:00.003Z] at time 3 watermark 1}, 1970-01-01T00:00:00.003Z]",
                                            "+I[Bob, {Processing input row +I[Bob, 4, 1970-01-01T00:00:00.004Z] at time 4 watermark 2}, 1970-01-01T00:00:00.004Z]",
                                            "+I[Bob, {Processing input row +I[Bob, 5, 1970-01-01T00:00:00.005Z] at time 5 watermark 3}, 1970-01-01T00:00:00.005Z]",
                                            "+I[Bob, {Processing input row +I[Bob, 6, 1970-01-01T00:00:00.006Z] at time 6 watermark 4}, 1970-01-01T00:00:00.006Z]",
                                            "+I[Bob, {Timer timeout2 fired at time 5 watermark 5}, 1970-01-01T00:00:00.005Z]",
                                            "+I[Bob, {Clearing all timers at time 5 watermark 5}, 1970-01-01T00:00:00.005Z]")
                                    .build())
                    .runSql(
                            "INSERT INTO sink SELECT * FROM f(r => TABLE t PARTITION BY name, on_time => DESCRIPTOR(ts))")
                    .build();

    public static final TableTestProgram PROCESS_UNNAMED_TIMERS =
            TableTestProgram.of(
                            "process-partitioned-unnamed-timers",
                            "test create/fire/re-create/clear/clear-all unnamed timers")
                    .setupTemporarySystemFunction("f", UnnamedTimersFunction.class)
                    .setupTableSource(TIMED_SOURCE)
                    .setupTableSink(
                            SinkTestStep.newBuilder("sink")
                                    .addSchema(KEYED_TIMED_BASE_SINK_SCHEMA)
                                    .consumedValues(
                                            "+I[Bob, {Processing input row +I[Bob, 1, 1970-01-01T00:00:00Z] at time 0 watermark null}, 1970-01-01T00:00:00Z]",
                                            "+I[Bob, {Registering timer for 1 at time 0 watermark null}, 1970-01-01T00:00:00Z]",
                                            "+I[Bob, {Registering timer for 2 at time 0 watermark null}, 1970-01-01T00:00:00Z]",
                                            "+I[Bob, {Registering timer for 9223372036854775807 at time 0 watermark null}, 1970-01-01T00:00:00Z]",
                                            "+I[Alice, {Processing input row +I[Alice, 1, 1970-01-01T00:00:00.001Z] at time 1 watermark -1}, 1970-01-01T00:00:00.001Z]",
                                            "+I[Bob, {Processing input row +I[Bob, 2, 1970-01-01T00:00:00.002Z] at time 2 watermark 0}, 1970-01-01T00:00:00.002Z]",
                                            "+I[Bob, {Timer null fired at time 1 watermark 1}, 1970-01-01T00:00:00.001Z]",
                                            "+I[Bob, {Registering timer for 5 at time 1 watermark 1}, 1970-01-01T00:00:00.001Z]",
                                            "+I[Bob, {Clearing timer 2 at time 1 watermark 1}, 1970-01-01T00:00:00.001Z]",
                                            "+I[Bob, {Processing input row +I[Bob, 3, 1970-01-01T00:00:00.003Z] at time 3 watermark 1}, 1970-01-01T00:00:00.003Z]",
                                            "+I[Bob, {Processing input row +I[Bob, 4, 1970-01-01T00:00:00.004Z] at time 4 watermark 2}, 1970-01-01T00:00:00.004Z]",
                                            "+I[Bob, {Processing input row +I[Bob, 5, 1970-01-01T00:00:00.005Z] at time 5 watermark 3}, 1970-01-01T00:00:00.005Z]",
                                            "+I[Bob, {Processing input row +I[Bob, 6, 1970-01-01T00:00:00.006Z] at time 6 watermark 4}, 1970-01-01T00:00:00.006Z]",
                                            "+I[Bob, {Timer null fired at time 5 watermark 5}, 1970-01-01T00:00:00.005Z]",
                                            "+I[Bob, {Clearing all timers at time 5 watermark 5}, 1970-01-01T00:00:00.005Z]")
                                    .build())
                    .runSql(
                            "INSERT INTO sink SELECT * FROM f(r => TABLE t PARTITION BY name, on_time => DESCRIPTOR(ts))")
                    .build();

    public static final TableTestProgram PROCESS_LATE_EVENTS =
            TableTestProgram.of(
                            "process-late-events",
                            "test that late events are dropped in both input and when registering timers")
                    .setupTemporarySystemFunction("f", LateTimersFunction.class)
                    .setupTableSource(TIMED_SOURCE_LATE_EVENTS)
                    .setupTableSink(
                            SinkTestStep.newBuilder("sink")
                                    .addSchema(KEYED_TIMED_BASE_SINK_SCHEMA)
                                    .consumedValues(
                                            "+I[Bob, {Processing input row +I[Bob, 1, 1970-01-01T00:00:00Z] at time 0 watermark null}, 1970-01-01T00:00:00Z]",
                                            "+I[Bob, {Registering timer t for 0 at time 0 watermark null}, 1970-01-01T00:00:00Z]",
                                            "+I[Bob, {Registering timer for 0 at time 0 watermark null}, 1970-01-01T00:00:00Z]",
                                            "+I[Alice, {Processing input row +I[Alice, 1, 1970-01-01T00:00:00.001Z] at time 1 watermark -1}, 1970-01-01T00:00:00.001Z]",
                                            "+I[Alice, {Registering timer t for 0 at time 1 watermark -1}, 1970-01-01T00:00:00.001Z]",
                                            "+I[Alice, {Registering timer for 0 at time 1 watermark -1}, 1970-01-01T00:00:00.001Z]",
                                            "+I[Bob, {Timer null fired at time 0 watermark 0}, 1970-01-01T00:00:00Z]",
                                            "+I[Bob, {Registering timer again for 0 at time 0 watermark 0}, 1970-01-01T00:00:00Z]",
                                            "+I[Alice, {Timer null fired at time 0 watermark 0}, 1970-01-01T00:00:00Z]",
                                            "+I[Alice, {Registering timer again for 0 at time 0 watermark 0}, 1970-01-01T00:00:00Z]",
                                            "+I[Bob, {Timer t fired at time 0 watermark 0}, 1970-01-01T00:00:00Z]",
                                            "+I[Bob, {Registering timer again for 0 at time 0 watermark 0}, 1970-01-01T00:00:00Z]",
                                            "+I[Alice, {Timer t fired at time 0 watermark 0}, 1970-01-01T00:00:00Z]",
                                            "+I[Alice, {Registering timer again for 0 at time 0 watermark 0}, 1970-01-01T00:00:00Z]",
                                            "+I[Bob, {Processing input row +I[Bob, 2, 1970-01-01T00:01:39.999Z] at time 99999 watermark 0}, 1970-01-01T00:01:39.999Z]",
                                            "+I[Bob, {Registering timer t for 0 at time 99999 watermark 0}, 1970-01-01T00:01:39.999Z]",
                                            "+I[Bob, {Registering timer for 0 at time 99999 watermark 0}, 1970-01-01T00:01:39.999Z]")
                                    .build())
                    .runSql(
                            "INSERT INTO sink SELECT * FROM f(r => TABLE t PARTITION BY name, on_time => DESCRIPTOR(ts))")
                    .build();

    public static final TableTestProgram PROCESS_SCALAR_ARGS_TIME =
            TableTestProgram.of(
                            "process-scalar-args-time",
                            "test time is not available for PTFs without tables")
                    .setupTemporarySystemFunction("f", ScalarArgsTimeFunction.class)
                    .setupTableSource(TIMED_SOURCE)
                    .setupTableSink(
                            SinkTestStep.newBuilder("sink")
                                    .addSchema(BASE_SINK_SCHEMA)
                                    .consumedValues("+I[{Time null and watermark null}]")
                                    .build())
                    .runSql("INSERT INTO sink SELECT * FROM f()")
                    .build();

    public static final TableTestProgram PROCESS_OPTIONAL_PARTITION_BY_TIME =
            TableTestProgram.of(
                            "process-optional-partition-by-time",
                            "test time and timers without PARTITION BY")
                    .setupTemporarySystemFunction("f", OptionalPartitionOnTimeFunction.class)
                    .setupTableSource(TIMED_SOURCE)
                    .setupTableSink(
                            SinkTestStep.newBuilder("sink")
                                    .addSchema(TIMED_BASE_SINK_SCHEMA)
                                    .consumedValues(
                                            "+I[{Processing input row +I[Bob, 1, 1970-01-01T00:00:00Z] at time 0 watermark null}, 1970-01-01T00:00:00Z]",
                                            "+I[{Registering timer t for 1 at time 0 watermark null}, 1970-01-01T00:00:00Z]",
                                            "+I[{Processing input row +I[Alice, 1, 1970-01-01T00:00:00.001Z] at time 1 watermark -1}, 1970-01-01T00:00:00.001Z]",
                                            "+I[{Processing input row +I[Bob, 2, 1970-01-01T00:00:00.002Z] at time 2 watermark 0}, 1970-01-01T00:00:00.002Z]",
                                            "+I[{Timer t fired at time 1 watermark 1}, 1970-01-01T00:00:00.001Z]",
                                            "+I[{Registering timer again for 2 at time 1 watermark 1}, 1970-01-01T00:00:00.001Z]",
                                            "+I[{Processing input row +I[Bob, 3, 1970-01-01T00:00:00.003Z] at time 3 watermark 1}, 1970-01-01T00:00:00.003Z]",
                                            "+I[{Timer again fired at time 2 watermark 2}, 1970-01-01T00:00:00.002Z]",
                                            "+I[{Processing input row +I[Bob, 4, 1970-01-01T00:00:00.004Z] at time 4 watermark 2}, 1970-01-01T00:00:00.004Z]",
                                            "+I[{Processing input row +I[Bob, 5, 1970-01-01T00:00:00.005Z] at time 5 watermark 3}, 1970-01-01T00:00:00.005Z]",
                                            "+I[{Processing input row +I[Bob, 6, 1970-01-01T00:00:00.006Z] at time 6 watermark 4}, 1970-01-01T00:00:00.006Z]")
                                    .build())
                    .runSql(
                            "INSERT INTO sink SELECT * FROM f(r => TABLE t, on_time => DESCRIPTOR(ts))")
                    .build();

    public static final TableTestProgram PROCESS_OPTIONAL_ON_TIME =
            TableTestProgram.of(
                            "process-optional-on-time",
                            "test optional time attribute, fire once for constant timer")
                    .setupTemporarySystemFunction("f", OptionalOnTimeFunction.class)
                    .setupTableSource(TIMED_SOURCE)
                    .setupTableSink(
                            SinkTestStep.newBuilder("sink")
                                    .addSchema(KEYED_BASE_SINK_SCHEMA)
                                    .consumedValues(
                                            "+I[Bob, {Processing input row +I[Bob, 1, 1970-01-01T00:00:00Z] at time null watermark null}]",
                                            "+I[Bob, {Registering timer t for 2 at time null watermark null}]",
                                            "+I[Alice, {Processing input row +I[Alice, 1, 1970-01-01T00:00:00.001Z] at time null watermark -1}]",
                                            "+I[Alice, {Registering timer t for 2 at time null watermark -1}]",
                                            "+I[Bob, {Processing input row +I[Bob, 2, 1970-01-01T00:00:00.002Z] at time null watermark 0}]",
                                            "+I[Bob, {Registering timer t for 2 at time null watermark 0}]",
                                            "+I[Bob, {Processing input row +I[Bob, 3, 1970-01-01T00:00:00.003Z] at time null watermark 1}]",
                                            "+I[Bob, {Registering timer t for 2 at time null watermark 1}]",
                                            "+I[Alice, {Timer t fired at time 2 watermark 2}]",
                                            "+I[Bob, {Timer t fired at time 2 watermark 2}]",
                                            "+I[Bob, {Processing input row +I[Bob, 4, 1970-01-01T00:00:00.004Z] at time null watermark 2}]",
                                            "+I[Bob, {Registering timer t for 2 at time null watermark 2}]",
                                            "+I[Bob, {Processing input row +I[Bob, 5, 1970-01-01T00:00:00.005Z] at time null watermark 3}]",
                                            "+I[Bob, {Registering timer t for 2 at time null watermark 3}]",
                                            "+I[Bob, {Processing input row +I[Bob, 6, 1970-01-01T00:00:00.006Z] at time null watermark 4}]",
                                            "+I[Bob, {Registering timer t for 2 at time null watermark 4}]")
                                    .build())
                    .runSql("INSERT INTO sink SELECT * FROM f(r => TABLE t PARTITION BY name)")
                    .build();

    public static final TableTestProgram PROCESS_POJO_STATE_TIME =
            TableTestProgram.of(
                            "process-pojo-state-time",
                            "test state access from both eval() and onTimer()")
                    .setupTemporarySystemFunction("f", PojoStateTimeFunction.class)
                    .setupTableSource(TIMED_SOURCE)
                    .setupTableSink(
                            SinkTestStep.newBuilder("sink")
                                    .addSchema(KEYED_TIMED_BASE_SINK_SCHEMA)
                                    .consumedValues(
                                            "+I[Bob, {Score(s='null', i=null), +I[Bob, 1, 1970-01-01T00:00:00Z]}, 1970-01-01T00:00:00Z]",
                                            "+I[Bob, {Registering timer t for 2 at time 0 watermark null}, 1970-01-01T00:00:00Z]",
                                            "+I[Alice, {Score(s='null', i=null), +I[Alice, 1, 1970-01-01T00:00:00.001Z]}, 1970-01-01T00:00:00.001Z]",
                                            "+I[Alice, {Registering timer t for 3 at time 1 watermark -1}, 1970-01-01T00:00:00.001Z]",
                                            "+I[Bob, {Score(s='null', i=1), +I[Bob, 2, 1970-01-01T00:00:00.002Z]}, 1970-01-01T00:00:00.002Z]",
                                            "+I[Bob, {Score(s='null', i=2), +I[Bob, 3, 1970-01-01T00:00:00.003Z]}, 1970-01-01T00:00:00.003Z]",
                                            "+I[Bob, {Timer t fired at time 2 watermark 2}, 1970-01-01T00:00:00.002Z]",
                                            "+I[Bob, {Score(s='null', i=30), +I[Bob, 4, 1970-01-01T00:00:00.004Z]}, 1970-01-01T00:00:00.004Z]",
                                            "+I[Alice, {Timer t fired at time 3 watermark 3}, 1970-01-01T00:00:00.003Z]",
                                            "+I[Bob, {Score(s='null', i=31), +I[Bob, 5, 1970-01-01T00:00:00.005Z]}, 1970-01-01T00:00:00.005Z]",
                                            "+I[Bob, {Score(s='null', i=32), +I[Bob, 6, 1970-01-01T00:00:00.006Z]}, 1970-01-01T00:00:00.006Z]")
                                    .build())
                    .runSql(
                            "INSERT INTO sink SELECT * FROM f(r => TABLE t PARTITION BY name, on_time => DESCRIPTOR(ts))")
                    .build();

    public static final TableTestProgram PROCESS_CHAINED_TIME =
            TableTestProgram.of(
                            "process-chained-time",
                            "test two chained PTFs where the second (receiving) PTF wraps the events of the first (sending) PTF")
                    .setupTemporarySystemFunction("f1", ChainedSendingFunction.class)
                    .setupTemporarySystemFunction("f2", ChainedReceivingFunction.class)
                    .setupTableSource(TIMED_SOURCE)
                    .setupTableSink(
                            SinkTestStep.newBuilder("sink")
                                    .addSchema(KEYED_TIMED_BASE_SINK_SCHEMA)
                                    .consumedValues(
                                            "+I[Bob, {Processing input row +I[Bob, {Processing input row +I[Bob, 1, 1970-01-01T00:00:00Z] at time 0 watermark null}, 1970-01-01T00:00:00Z] at time 0 watermark null}, 1970-01-01T00:00:00Z]",
                                            "+I[Bob, {Processing input row +I[Bob, {Registering timer t for 1 at time 0 watermark null}, 1970-01-01T00:00:00Z] at time 0 watermark null}, 1970-01-01T00:00:00Z]",
                                            "+I[Alice, {Processing input row +I[Alice, {Processing input row +I[Alice, 1, 1970-01-01T00:00:00.001Z] at time 1 watermark -1}, 1970-01-01T00:00:00.001Z] at time 1 watermark -1}, 1970-01-01T00:00:00.001Z]",
                                            "+I[Alice, {Processing input row +I[Alice, {Registering timer t for 2 at time 1 watermark -1}, 1970-01-01T00:00:00.001Z] at time 1 watermark -1}, 1970-01-01T00:00:00.001Z]",
                                            "+I[Bob, {Processing input row +I[Bob, {Processing input row +I[Bob, 2, 1970-01-01T00:00:00.002Z] at time 2 watermark 0}, 1970-01-01T00:00:00.002Z] at time 2 watermark 0}, 1970-01-01T00:00:00.002Z]",
                                            "+I[Bob, {Processing input row +I[Bob, {Registering timer t for 3 at time 2 watermark 0}, 1970-01-01T00:00:00.002Z] at time 2 watermark 0}, 1970-01-01T00:00:00.002Z]",
                                            "+I[Bob, {Processing input row +I[Bob, {Processing input row +I[Bob, 3, 1970-01-01T00:00:00.003Z] at time 3 watermark 1}, 1970-01-01T00:00:00.003Z] at time 3 watermark 1}, 1970-01-01T00:00:00.003Z]",
                                            "+I[Bob, {Processing input row +I[Bob, {Registering timer t for 4 at time 3 watermark 1}, 1970-01-01T00:00:00.003Z] at time 3 watermark 1}, 1970-01-01T00:00:00.003Z]",
                                            "+I[Alice, {Processing input row +I[Alice, {Timer t fired at time 2 watermark 2}, 1970-01-01T00:00:00.002Z] at time 2 watermark 1}, 1970-01-01T00:00:00.002Z]",
                                            "+I[Alice, {Processing input row +I[Alice, {2}, 1970-01-01T00:00:00.002Z] at time 2 watermark 1}, 1970-01-01T00:00:00.002Z]",
                                            "+I[Bob, {Processing input row +I[Bob, {Processing input row +I[Bob, 4, 1970-01-01T00:00:00.004Z] at time 4 watermark 2}, 1970-01-01T00:00:00.004Z] at time 4 watermark 2}, 1970-01-01T00:00:00.004Z]",
                                            "+I[Bob, {Processing input row +I[Bob, {Registering timer t for 5 at time 4 watermark 2}, 1970-01-01T00:00:00.004Z] at time 4 watermark 2}, 1970-01-01T00:00:00.004Z]",
                                            "+I[Bob, {Processing input row +I[Bob, {Processing input row +I[Bob, 5, 1970-01-01T00:00:00.005Z] at time 5 watermark 3}, 1970-01-01T00:00:00.005Z] at time 5 watermark 3}, 1970-01-01T00:00:00.005Z]",
                                            "+I[Bob, {Processing input row +I[Bob, {Registering timer t for 6 at time 5 watermark 3}, 1970-01-01T00:00:00.005Z] at time 5 watermark 3}, 1970-01-01T00:00:00.005Z]",
                                            "+I[Bob, {Processing input row +I[Bob, {Processing input row +I[Bob, 6, 1970-01-01T00:00:00.006Z] at time 6 watermark 4}, 1970-01-01T00:00:00.006Z] at time 6 watermark 4}, 1970-01-01T00:00:00.006Z]",
                                            "+I[Bob, {Processing input row +I[Bob, {Registering timer t for 7 at time 6 watermark 4}, 1970-01-01T00:00:00.006Z] at time 6 watermark 4}, 1970-01-01T00:00:00.006Z]",
                                            "+I[Bob, {Processing input row +I[Bob, {Timer t fired at time 7 watermark 9223372036854775807}, 1970-01-01T00:00:00.007Z] at time 7 watermark 5}, 1970-01-01T00:00:00.007Z]",
                                            "+I[Bob, {Processing input row +I[Bob, {7}, 1970-01-01T00:00:00.007Z] at time 7 watermark 5}, 1970-01-01T00:00:00.007Z]")
                                    .build())
                    .runSql(
                            "INSERT INTO sink "
                                    + "WITH "
                                    + "ptf1 AS (SELECT * FROM f1(r => TABLE t PARTITION BY name, on_time => DESCRIPTOR(ts))), "
                                    + "ptf2 AS (SELECT * FROM f2(r => TABLE ptf1 PARTITION BY name, on_time => DESCRIPTOR(rowtime))) "
                                    + "SELECT * FROM ptf2")
                    .build();

    public static final TableTestProgram PROCESS_CHAINED_TIME_TABLE_API =
            TableTestProgram.of(
                            "process-chained-time-table-api",
                            "test two chained PTFs where the second (receiving) PTF wraps the events of the first (sending) PTF")
                    .setupTableSource(TIMED_SOURCE)
                    .setupTableSink(
                            SinkTestStep.newBuilder("sink")
                                    .addSchema(KEYED_TIMED_BASE_SINK_SCHEMA)
                                    .consumedValues(
                                            "+I[Bob, {Processing input row +I[Bob, {Processing input row +I[Bob, 1, 1970-01-01T00:00:00Z] at time 0 watermark null}, 1970-01-01T00:00:00Z] at time 0 watermark null}, 1970-01-01T00:00:00Z]",
                                            "+I[Bob, {Processing input row +I[Bob, {Registering timer t for 1 at time 0 watermark null}, 1970-01-01T00:00:00Z] at time 0 watermark null}, 1970-01-01T00:00:00Z]",
                                            "+I[Alice, {Processing input row +I[Alice, {Processing input row +I[Alice, 1, 1970-01-01T00:00:00.001Z] at time 1 watermark -1}, 1970-01-01T00:00:00.001Z] at time 1 watermark -1}, 1970-01-01T00:00:00.001Z]",
                                            "+I[Alice, {Processing input row +I[Alice, {Registering timer t for 2 at time 1 watermark -1}, 1970-01-01T00:00:00.001Z] at time 1 watermark -1}, 1970-01-01T00:00:00.001Z]",
                                            "+I[Bob, {Processing input row +I[Bob, {Processing input row +I[Bob, 2, 1970-01-01T00:00:00.002Z] at time 2 watermark 0}, 1970-01-01T00:00:00.002Z] at time 2 watermark 0}, 1970-01-01T00:00:00.002Z]",
                                            "+I[Bob, {Processing input row +I[Bob, {Registering timer t for 3 at time 2 watermark 0}, 1970-01-01T00:00:00.002Z] at time 2 watermark 0}, 1970-01-01T00:00:00.002Z]",
                                            "+I[Bob, {Processing input row +I[Bob, {Processing input row +I[Bob, 3, 1970-01-01T00:00:00.003Z] at time 3 watermark 1}, 1970-01-01T00:00:00.003Z] at time 3 watermark 1}, 1970-01-01T00:00:00.003Z]",
                                            "+I[Bob, {Processing input row +I[Bob, {Registering timer t for 4 at time 3 watermark 1}, 1970-01-01T00:00:00.003Z] at time 3 watermark 1}, 1970-01-01T00:00:00.003Z]",
                                            "+I[Alice, {Processing input row +I[Alice, {Timer t fired at time 2 watermark 2}, 1970-01-01T00:00:00.002Z] at time 2 watermark 1}, 1970-01-01T00:00:00.002Z]",
                                            "+I[Alice, {Processing input row +I[Alice, {2}, 1970-01-01T00:00:00.002Z] at time 2 watermark 1}, 1970-01-01T00:00:00.002Z]",
                                            "+I[Bob, {Processing input row +I[Bob, {Processing input row +I[Bob, 4, 1970-01-01T00:00:00.004Z] at time 4 watermark 2}, 1970-01-01T00:00:00.004Z] at time 4 watermark 2}, 1970-01-01T00:00:00.004Z]",
                                            "+I[Bob, {Processing input row +I[Bob, {Registering timer t for 5 at time 4 watermark 2}, 1970-01-01T00:00:00.004Z] at time 4 watermark 2}, 1970-01-01T00:00:00.004Z]",
                                            "+I[Bob, {Processing input row +I[Bob, {Processing input row +I[Bob, 5, 1970-01-01T00:00:00.005Z] at time 5 watermark 3}, 1970-01-01T00:00:00.005Z] at time 5 watermark 3}, 1970-01-01T00:00:00.005Z]",
                                            "+I[Bob, {Processing input row +I[Bob, {Registering timer t for 6 at time 5 watermark 3}, 1970-01-01T00:00:00.005Z] at time 5 watermark 3}, 1970-01-01T00:00:00.005Z]",
                                            "+I[Bob, {Processing input row +I[Bob, {Processing input row +I[Bob, 6, 1970-01-01T00:00:00.006Z] at time 6 watermark 4}, 1970-01-01T00:00:00.006Z] at time 6 watermark 4}, 1970-01-01T00:00:00.006Z]",
                                            "+I[Bob, {Processing input row +I[Bob, {Registering timer t for 7 at time 6 watermark 4}, 1970-01-01T00:00:00.006Z] at time 6 watermark 4}, 1970-01-01T00:00:00.006Z]",
                                            "+I[Bob, {Processing input row +I[Bob, {Timer t fired at time 7 watermark 9223372036854775807}, 1970-01-01T00:00:00.007Z] at time 7 watermark 5}, 1970-01-01T00:00:00.007Z]",
                                            "+I[Bob, {Processing input row +I[Bob, {7}, 1970-01-01T00:00:00.007Z] at time 7 watermark 5}, 1970-01-01T00:00:00.007Z]")
                                    .build())
                    .runTableApi(
                            env -> {
                                final Table ptf1 =
                                        env.fromCall(
                                                ChainedSendingFunction.class,
                                                env.from("t")
                                                        .partitionBy($("name"))
                                                        .asArgument("r"),
                                                descriptor("ts").asArgument("on_time"));
                                return env.fromCall(
                                        ChainedReceivingFunction.class,
                                        ptf1.partitionBy($("name")).asArgument("r"),
                                        descriptor("rowtime").asArgument("on_time"));
                            },
                            "sink")
                    .build();

    public static final TableTestProgram PROCESS_INVALID_TABLE_AS_ROW_TIMERS =
            TableTestProgram.of(
                            "process-invalid-table-as-row-timers",
                            "error if timers are registered for PTFs with row semantic tables")
                    .setupTemporarySystemFunction("f", InvalidTableAsRowTimersFunction.class)
                    .setupSql(BASIC_VALUES)
                    .runFailingSql(
                            "SELECT * FROM f(r => TABLE t)",
                            TableRuntimeException.class,
                            "Timers are not supported in the current PTF declaration.")
                    .build();

    public static final TableTestProgram PROCESS_INVALID_PASS_THROUGH_TIMERS =
            TableTestProgram.of(
                            "process-invalid-pass-through-timers",
                            "error if timers are registered for PTFs with pass-through columns")
                    .setupTemporarySystemFunction("f", InvalidPassThroughTimersFunction.class)
                    .setupTableSource(TIMED_SOURCE)
                    .runFailingSql(
                            "SELECT * FROM f(r => TABLE t PARTITION BY name, on_time => DESCRIPTOR(ts))",
                            TableRuntimeException.class,
                            "Timers are not supported in the current PTF declaration.")
                    .build();

    public static final TableTestProgram PROCESS_INVALID_UPDATING_TIMERS =
            TableTestProgram.of(
                            "process-invalid-updating-timers",
                            "error if timers are registered for PTFs with updating inputs")
                    .setupTemporarySystemFunction("f", InvalidUpdatingTimersFunction.class)
                    .setupSql(UPDATING_VALUES)
                    .runFailingSql(
                            "SELECT * FROM f(r => TABLE t PARTITION BY name, uid => DEFAULT)",
                            TableRuntimeException.class,
                            "Timers are not supported in the current PTF declaration.")
                    .build();

    public static final TableTestProgram PROCESS_LIST_STATE =
            TableTestProgram.of("process-list-state", "list view state entry")
                    .setupTemporarySystemFunction("f", ListStateFunction.class)
                    .setupSql(MULTI_VALUES)
                    .setupTableSink(
                            SinkTestStep.newBuilder("sink")
                                    .addSchema(KEYED_BASE_SINK_SCHEMA)
                                    .consumedValues(
                                            "+I[Bob, {[], KeyedStateListView, +I[Bob, 12]}]",
                                            "+I[Alice, {[], KeyedStateListView, +I[Alice, 42]}]",
                                            "+I[Bob, {[0], KeyedStateListView, +I[Bob, 99]}]",
                                            "+I[Bob, {[0, 1], KeyedStateListView, +I[Bob, 100]}]",
                                            "+I[Alice, {[0], KeyedStateListView, +I[Alice, 400]}]")
                                    .build())
                    .runSql("INSERT INTO sink SELECT * FROM f(r => TABLE t PARTITION BY name)")
                    .build();

    public static final TableTestProgram PROCESS_MAP_STATE =
            TableTestProgram.of("process-map-state", "map view state entry")
                    .setupTemporarySystemFunction("f", MapStateFunction.class)
                    .setupSql(MULTI_VALUES)
                    .setupTableSink(
                            SinkTestStep.newBuilder("sink")
                                    .addSchema(KEYED_BASE_SINK_SCHEMA)
                                    .consumedValues(
                                            "+I[Bob, {{}, KeyedStateMapViewWithKeysNotNull, +I[Bob, 12]}]",
                                            "+I[Alice, {{}, KeyedStateMapViewWithKeysNotNull, +I[Alice, 42]}]",
                                            "+I[Bob, {{Bob=2, nullValue=null, oldBob=1}, KeyedStateMapViewWithKeysNotNull, +I[Bob, 99]}]",
                                            "+I[Bob, {{}, KeyedStateMapViewWithKeysNotNull, +I[Bob, 100]}]",
                                            "+I[Alice, {{Alice=2, nullValue=null, oldAlice=1}, KeyedStateMapViewWithKeysNotNull, +I[Alice, 400]}]")
                                    .build())
                    .runSql("INSERT INTO sink SELECT * FROM f(r => TABLE t PARTITION BY name)")
                    .build();
}
