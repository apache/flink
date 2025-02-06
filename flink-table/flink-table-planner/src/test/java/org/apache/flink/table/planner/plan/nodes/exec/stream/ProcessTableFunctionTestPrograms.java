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

import org.apache.flink.table.planner.plan.nodes.exec.stream.ProcessTableFunctionTestUtils.AtomicTypeWrappingFunction;
import org.apache.flink.table.planner.plan.nodes.exec.stream.ProcessTableFunctionTestUtils.ContextFunction;
import org.apache.flink.table.planner.plan.nodes.exec.stream.ProcessTableFunctionTestUtils.EmptyArgFunction;
import org.apache.flink.table.planner.plan.nodes.exec.stream.ProcessTableFunctionTestUtils.PojoArgsFunction;
import org.apache.flink.table.planner.plan.nodes.exec.stream.ProcessTableFunctionTestUtils.PojoCreatingFunction;
import org.apache.flink.table.planner.plan.nodes.exec.stream.ProcessTableFunctionTestUtils.ScalarArgsFunction;
import org.apache.flink.table.planner.plan.nodes.exec.stream.ProcessTableFunctionTestUtils.TableAsRowFunction;
import org.apache.flink.table.planner.plan.nodes.exec.stream.ProcessTableFunctionTestUtils.TableAsRowPassThroughFunction;
import org.apache.flink.table.planner.plan.nodes.exec.stream.ProcessTableFunctionTestUtils.TableAsSetFunction;
import org.apache.flink.table.planner.plan.nodes.exec.stream.ProcessTableFunctionTestUtils.TableAsSetOptionalPartitionFunction;
import org.apache.flink.table.planner.plan.nodes.exec.stream.ProcessTableFunctionTestUtils.TableAsSetPassThroughFunction;
import org.apache.flink.table.planner.plan.nodes.exec.stream.ProcessTableFunctionTestUtils.TableAsSetUpdatingArgFunction;
import org.apache.flink.table.planner.plan.nodes.exec.stream.ProcessTableFunctionTestUtils.TestProcessTableFunctionBase;
import org.apache.flink.table.planner.plan.nodes.exec.stream.ProcessTableFunctionTestUtils.TypedTableAsRowFunction;
import org.apache.flink.table.planner.plan.nodes.exec.stream.ProcessTableFunctionTestUtils.TypedTableAsSetFunction;
import org.apache.flink.table.test.program.SinkTestStep;
import org.apache.flink.table.test.program.TableTestProgram;

/** {@link TableTestProgram} definitions for testing {@link StreamExecProcessTableFunction}. */
public class ProcessTableFunctionTestPrograms {

    private static final String BASIC_VALUES =
            "CREATE VIEW t AS SELECT * FROM (VALUES ('Bob', 12), ('Alice', 42)) AS T(name, score)";

    private static final String UPDATING_VALUES =
            "CREATE VIEW t AS SELECT name, COUNT(*) FROM "
                    + "(VALUES ('Bob', 12), ('Alice', 42), ('Bob', 14)) AS T(name, score) "
                    + "GROUP BY name";

    /** Corresponds to {@link TestProcessTableFunctionBase}. */
    private static final String BASE_SINK_SCHEMA = "`out` STRING, `count` INT";

    /** Corresponds to {@link TestProcessTableFunctionBase}. */
    private static final String KEYED_BASE_SINK_SCHEMA = "`name` STRING, `out` STRING, `count` INT";

    /** Corresponds to {@link TestProcessTableFunctionBase}. */
    private static final String PASS_THROUGH_BASE_SINK_SCHEMA =
            "`name` STRING, `score` INT, `out` STRING, `count` INT";

    public static final TableTestProgram PROCESS_SCALAR_ARGS =
            TableTestProgram.of("process-scalar-args", "no table as input")
                    .setupTemporarySystemFunction("f", ScalarArgsFunction.class)
                    .setupTableSink(
                            SinkTestStep.newBuilder("sink")
                                    .addSchema(BASE_SINK_SCHEMA)
                                    .consumedValues("+I[{42, true}, 1]", "+I[{42, true}, 2]")
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
                                            "+I[{+I[Bob, 12], 1}, 1]",
                                            "+I[{+I[Bob, 12], 1}, 2]",
                                            "+I[{+I[Alice, 42], 1}, 1]",
                                            "+I[{+I[Alice, 42], 1}, 2]")
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
                                            "+I[{User(s='Bob', i=12), 1}, 1]",
                                            "+I[{User(s='Bob', i=12), 1}, 2]",
                                            "+I[{User(s='Alice', i=42), 1}, 1]",
                                            "+I[{User(s='Alice', i=42), 1}, 2]")
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
                                            "+I[Bob, {+I[Bob, 12], 1}, 1]",
                                            "+I[Bob, {+I[Bob, 12], 1}, 2]",
                                            "+I[Alice, {+I[Alice, 42], 1}, 1]",
                                            "+I[Alice, {+I[Alice, 42], 1}, 2]")
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
                                            "+I[Bob, {User(s='Bob', i=12), 1}, 1]",
                                            "+I[Bob, {User(s='Bob', i=12), 1}, 2]",
                                            "+I[Alice, {User(s='Alice', i=42), 1}, 1]",
                                            "+I[Alice, {User(s='Alice', i=42), 1}, 2]")
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
                                            "+I[{User(s='Bob', i=12), User(s='Bob', i=12)}, 1]",
                                            "+I[{User(s='Bob', i=12), User(s='Bob', i=12)}, 2]",
                                            "+I[{User(s='Alice', i=42), User(s='Bob', i=12)}, 1]",
                                            "+I[{User(s='Alice', i=42), User(s='Bob', i=12)}, 2]")
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
                                    .consumedValues("+I[{empty}, 1]", "+I[{empty}, 2]")
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
                                            "+I[Bob, 12, {+I[Bob, 12], 1}, 1]",
                                            "+I[Bob, 12, {+I[Bob, 12], 1}, 2]",
                                            "+I[Alice, 42, {+I[Alice, 42], 1}, 1]",
                                            "+I[Alice, 42, {+I[Alice, 42], 1}, 2]")
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
                                            "+I[Bob, 12, {+I[Bob, 12], 1}, 1]",
                                            "+I[Bob, 12, {+I[Bob, 12], 1}, 2]",
                                            "+I[Alice, 42, {+I[Alice, 42], 1}, 1]",
                                            "+I[Alice, 42, {+I[Alice, 42], 1}, 2]")
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
                                            "+I[{+I[Bob, 1]}, 1]",
                                            "+I[{+I[Bob, 1]}, 2]",
                                            "+I[{+I[Alice, 1]}, 1]",
                                            "+I[{+I[Alice, 1]}, 2]",
                                            "+I[{-U[Bob, 1]}, 1]",
                                            "+I[{-U[Bob, 1]}, 2]",
                                            "+I[{+U[Bob, 2]}, 1]",
                                            "+I[{+U[Bob, 2]}, 2]")
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
                                            "+I[{+I[Bob, 12], 1}, 1]",
                                            "+I[{+I[Bob, 12], 1}, 2]",
                                            "+I[{+I[Alice, 42], 1}, 1]",
                                            "+I[{+I[Alice, 42], 1}, 2]")
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
                                            "+I[Bob, {+I[Bob, 12], param, [0], [INSERT], ROW<`name` VARCHAR(5) NOT NULL, `score` INT NOT NULL>}, 1]",
                                            "+I[Bob, {+I[Bob, 12], param, [0], [INSERT], ROW<`name` VARCHAR(5) NOT NULL, `score` INT NOT NULL>}, 2]",
                                            "+I[Alice, {+I[Alice, 42], param, [0], [INSERT], ROW<`name` VARCHAR(5) NOT NULL, `score` INT NOT NULL>}, 1]",
                                            "+I[Alice, {+I[Alice, 42], param, [0], [INSERT], ROW<`name` VARCHAR(5) NOT NULL, `score` INT NOT NULL>}, 2]")
                                    .build())
                    .runSql(
                            "INSERT INTO sink SELECT * FROM f(r => TABLE t PARTITION BY name, s => 'param')")
                    .build();
}
