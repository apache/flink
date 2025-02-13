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

import org.apache.flink.table.api.config.ExecutionConfigOptions;
import org.apache.flink.table.planner.plan.nodes.exec.stream.ProcessTableFunctionTestUtils.AtomicTypeWrappingFunction;
import org.apache.flink.table.planner.plan.nodes.exec.stream.ProcessTableFunctionTestUtils.ClearStateFunction;
import org.apache.flink.table.planner.plan.nodes.exec.stream.ProcessTableFunctionTestUtils.ContextFunction;
import org.apache.flink.table.planner.plan.nodes.exec.stream.ProcessTableFunctionTestUtils.EmptyArgFunction;
import org.apache.flink.table.planner.plan.nodes.exec.stream.ProcessTableFunctionTestUtils.MultiStateFunction;
import org.apache.flink.table.planner.plan.nodes.exec.stream.ProcessTableFunctionTestUtils.PojoArgsFunction;
import org.apache.flink.table.planner.plan.nodes.exec.stream.ProcessTableFunctionTestUtils.PojoCreatingFunction;
import org.apache.flink.table.planner.plan.nodes.exec.stream.ProcessTableFunctionTestUtils.PojoStateFunction;
import org.apache.flink.table.planner.plan.nodes.exec.stream.ProcessTableFunctionTestUtils.PojoWithDefaultStateFunction;
import org.apache.flink.table.planner.plan.nodes.exec.stream.ProcessTableFunctionTestUtils.ScalarArgsFunction;
import org.apache.flink.table.planner.plan.nodes.exec.stream.ProcessTableFunctionTestUtils.TableAsRowFunction;
import org.apache.flink.table.planner.plan.nodes.exec.stream.ProcessTableFunctionTestUtils.TableAsRowPassThroughFunction;
import org.apache.flink.table.planner.plan.nodes.exec.stream.ProcessTableFunctionTestUtils.TableAsSetFunction;
import org.apache.flink.table.planner.plan.nodes.exec.stream.ProcessTableFunctionTestUtils.TableAsSetOptionalPartitionFunction;
import org.apache.flink.table.planner.plan.nodes.exec.stream.ProcessTableFunctionTestUtils.TableAsSetPassThroughFunction;
import org.apache.flink.table.planner.plan.nodes.exec.stream.ProcessTableFunctionTestUtils.TableAsSetUpdatingArgFunction;
import org.apache.flink.table.planner.plan.nodes.exec.stream.ProcessTableFunctionTestUtils.TestProcessTableFunctionBase;
import org.apache.flink.table.planner.plan.nodes.exec.stream.ProcessTableFunctionTestUtils.TimeToLiveStateFunction;
import org.apache.flink.table.planner.plan.nodes.exec.stream.ProcessTableFunctionTestUtils.TypedTableAsRowFunction;
import org.apache.flink.table.planner.plan.nodes.exec.stream.ProcessTableFunctionTestUtils.TypedTableAsSetFunction;
import org.apache.flink.table.test.program.SinkTestStep;
import org.apache.flink.table.test.program.TableTestProgram;

import java.time.Duration;

/** {@link TableTestProgram} definitions for testing {@link StreamExecProcessTableFunction}. */
public class ProcessTableFunctionTestPrograms {

    private static final String BASIC_VALUES =
            "CREATE VIEW t AS SELECT * FROM "
                    + "(VALUES ('Bob', 12), ('Alice', 42)) AS T(name, score)";

    private static final String MULTI_VALUES =
            "CREATE VIEW t AS SELECT * FROM "
                    + "(VALUES ('Bob', 12), ('Alice', 42), ('Bob', 99), ('Bob', 100), ('Alice', 400)) AS T(name, score)";

    private static final String UPDATING_VALUES =
            "CREATE VIEW t AS SELECT name, COUNT(*) FROM "
                    + "(VALUES ('Bob', 12), ('Alice', 42), ('Bob', 14)) AS T(name, score) "
                    + "GROUP BY name";

    /** Corresponds to {@link TestProcessTableFunctionBase}. */
    private static final String BASE_SINK_SCHEMA = "`out` STRING";

    /** Corresponds to {@link TestProcessTableFunctionBase}. */
    private static final String KEYED_BASE_SINK_SCHEMA = "`name` STRING, `out` STRING";

    /** Corresponds to {@link TestProcessTableFunctionBase}. */
    private static final String PASS_THROUGH_BASE_SINK_SCHEMA =
            "`name` STRING, `score` INT, `out` STRING";

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
}
