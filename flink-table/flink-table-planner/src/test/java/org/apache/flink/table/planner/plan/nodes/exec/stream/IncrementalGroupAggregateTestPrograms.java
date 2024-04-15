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
import org.apache.flink.table.api.config.OptimizerConfigOptions;
import org.apache.flink.table.test.program.SinkTestStep;
import org.apache.flink.table.test.program.SourceTestStep;
import org.apache.flink.table.test.program.TableTestProgram;
import org.apache.flink.types.Row;

import java.time.Duration;

/** {@link TableTestProgram} definitions for testing {@link StreamExecGroupAggregate}. */
public class IncrementalGroupAggregateTestPrograms {

    static final Row[] BEFORE_DATA = {
        Row.of(1, 1L, "hi"), Row.of(2, 2L, "hello"), Row.of(3, 2L, "hello world")
    };

    static final Row[] AFTER_DATA = {
        Row.of(3, 2L, "foo"), Row.of(4, 4L, "bar"), Row.of(5, 2L, "foo bar")
    };

    static final String[] SOURCE_SCHEMA = {"a INT", "b BIGINT", "c VARCHAR"};

    static final TableTestProgram INCREMENTAL_GROUP_AGGREGATE_SIMPLE =
            TableTestProgram.of(
                            "incremental-group-aggregate-simple",
                            "validates incremental group aggregation")
                    .setupTableSource(
                            SourceTestStep.newBuilder("source_t")
                                    .addSchema(SOURCE_SCHEMA)
                                    .producedBeforeRestore(BEFORE_DATA)
                                    .producedAfterRestore(AFTER_DATA)
                                    .build())
                    .setupConfig(ExecutionConfigOptions.TABLE_EXEC_MINIBATCH_ENABLED, true)
                    .setupConfig(
                            ExecutionConfigOptions.TABLE_EXEC_MINIBATCH_ALLOW_LATENCY,
                            Duration.ofSeconds(10))
                    .setupConfig(ExecutionConfigOptions.TABLE_EXEC_MINIBATCH_SIZE, 1L)
                    .setupConfig(
                            OptimizerConfigOptions.TABLE_OPTIMIZER_DISTINCT_AGG_SPLIT_ENABLED, true)
                    .setupTableSink(
                            SinkTestStep.newBuilder("sink_t")
                                    .addSchema("b BIGINT", "a BIGINT")
                                    .consumedBeforeRestore(
                                            "+I[1, 1]", "+I[2, 1]", "-U[2, 1]", "+U[2, 2]")
                                    .consumedAfterRestore("-U[2, 2]", "+U[2, 3]", "+I[4, 1]")
                                    .build())
                    .runSql(
                            "INSERT INTO sink_t\n"
                                    + "     SELECT\n"
                                    + "         b,\n"
                                    + "         COUNT(DISTINCT a) AS a\n"
                                    + "     FROM source_t\n"
                                    + "     GROUP BY b")
                    .build();

    static final TableTestProgram INCREMENTAL_GROUP_AGGREGATE_COMPLEX =
            TableTestProgram.of(
                            "incremental-group-aggregate-complex",
                            "validates incremental group aggregation with multiple aggregations")
                    .setupTableSource(
                            SourceTestStep.newBuilder("source_t")
                                    .addSchema(SOURCE_SCHEMA)
                                    .producedBeforeRestore(BEFORE_DATA)
                                    .producedAfterRestore(AFTER_DATA)
                                    .build())
                    .setupConfig(ExecutionConfigOptions.TABLE_EXEC_MINIBATCH_ENABLED, true)
                    .setupConfig(
                            ExecutionConfigOptions.TABLE_EXEC_MINIBATCH_ALLOW_LATENCY,
                            Duration.ofSeconds(10))
                    .setupConfig(ExecutionConfigOptions.TABLE_EXEC_MINIBATCH_SIZE, 1L)
                    .setupConfig(
                            OptimizerConfigOptions.TABLE_OPTIMIZER_DISTINCT_AGG_SPLIT_ENABLED, true)
                    .setupTableSink(
                            SinkTestStep.newBuilder("sink_t")
                                    .addSchema(
                                            "b BIGINT",
                                            "sum_b BIGINT",
                                            "cnt_distinct_b BIGINT",
                                            "cnt_1 BIGINT")
                                    .consumedBeforeRestore(
                                            "+I[1, 1, 1, 1]",
                                            "-U[1, 1, 1, 1]",
                                            "+U[1, 3, 2, 2]",
                                            "-U[1, 3, 2, 2]",
                                            "+U[1, 5, 2, 3]")
                                    .consumedAfterRestore(
                                            "-U[1, 5, 2, 3]",
                                            "+U[1, 3, 2, 2]",
                                            "+I[2, 2, 1, 1]",
                                            "-U[1, 3, 2, 2]",
                                            "+U[1, 7, 3, 3]",
                                            "-U[1, 7, 3, 3]",
                                            "+U[1, 9, 3, 4]")
                                    .build())
                    .runSql(
                            "INSERT INTO sink_t SELECT\n"
                                    + "         b,\n"
                                    + "         SUM(b1) AS sum_b,\n"
                                    + "         COUNT(DISTINCT b1) AS cnt_distinct_b,\n"
                                    + "         COUNT(1) AS cnt_1\n"
                                    + "     FROM\n"
                                    + "         (\n"
                                    + "             SELECT\n"
                                    + "                     a,\n"
                                    + "                     COUNT(b) AS b,\n"
                                    + "                     MAX(b) AS b1\n"
                                    + "             FROM source_t GROUP BY a\n"
                                    + "         )\n"
                                    + "     GROUP BY b")
                    .build();
}
