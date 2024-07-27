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
import org.apache.flink.table.test.program.SinkTestStep;
import org.apache.flink.table.test.program.SourceTestStep;
import org.apache.flink.table.test.program.TableTestProgram;
import org.apache.flink.types.Row;

import java.time.Duration;

/** {@link TableTestProgram} definitions for testing {@link StreamExecMiniBatchAssigner}. */
public class MiniBatchAssignerTestPrograms {

    static final String[] ROW_TIME_SCHEMA = {
        "ts STRING",
        "id STRING",
        "num INT",
        "name STRING",
        "row_time AS TO_TIMESTAMP(`ts`)",
        "WATERMARK for `row_time` AS `row_time` - INTERVAL '1' SECOND"
    };

    static final TableTestProgram MINI_BATCH_ASSIGNER_ROW_TIME =
            TableTestProgram.of(
                            "mini-batch-assigner-row-time",
                            "validates mini batch assigner with row time")
                    .setupConfig(ExecutionConfigOptions.TABLE_EXEC_MINIBATCH_ENABLED, true)
                    .setupConfig(
                            ExecutionConfigOptions.TABLE_EXEC_MINIBATCH_ALLOW_LATENCY,
                            Duration.ofSeconds(1))
                    .setupConfig(ExecutionConfigOptions.TABLE_EXEC_MINIBATCH_SIZE, 5L)
                    .setupTableSource(
                            SourceTestStep.newBuilder("source_one_t")
                                    .addSchema(ROW_TIME_SCHEMA)
                                    .producedBeforeRestore(
                                            Row.of("2020-10-10 00:00:01", "L1", 1, "a"),
                                            Row.of("2020-10-10 00:00:02", "L2", 2, "c"),
                                            Row.of("2020-10-10 00:00:03", "L3", 2, "x"))
                                    .producedAfterRestore(
                                            Row.of("2020-10-10 00:00:41", "L41", 10, "a"),
                                            Row.of("2020-10-10 00:00:42", "L42", 11, "c"))
                                    .build())
                    .setupTableSource(
                            SourceTestStep.newBuilder("source_two_t")
                                    .addSchema(ROW_TIME_SCHEMA)
                                    .producedBeforeRestore(
                                            Row.of("2020-10-10 00:00:01", "R1", 5, "a"),
                                            Row.of("2020-10-10 00:00:02", "R2", 7, "b"),
                                            Row.of("2020-10-10 00:00:03", "R3", 7, "f"))
                                    .producedAfterRestore(
                                            Row.of("2020-10-10 00:00:41", "R41", 10, "y"),
                                            Row.of("2020-10-10 00:00:42", "R42", 11, "c"))
                                    .build())
                    .setupTableSink(
                            SinkTestStep.newBuilder("sink_t")
                                    .addSchema(
                                            "window_start TIMESTAMP(3)",
                                            "window_end TIMESTAMP(3)",
                                            "name STRING",
                                            "L_id STRING",
                                            "L_num INT",
                                            "R_id STRING",
                                            "R_num INT")
                                    .consumedBeforeRestore(
                                            "+I[2020-10-10T00:00:01, 2020-10-10T00:00:02, a, L1, 1, R1, 5]")
                                    .consumedAfterRestore(
                                            "+I[2020-10-10T00:00:42, 2020-10-10T00:00:43, c, L42, 11, R42, 11]")
                                    .build())
                    .runSql(
                            "INSERT INTO sink_t SELECT\n"
                                    + "L.window_start AS window_start,\n"
                                    + "L.window_end AS window_end,\n"
                                    + "L.name AS name,\n"
                                    + "L.id AS L_id,\n"
                                    + "L.num AS L_num,\n"
                                    + "R.id AS R_id,\n"
                                    + "R.num AS R_num\n"
                                    + "FROM\n"
                                    + "(\n"
                                    + "    SELECT * FROM TABLE(TUMBLE(TABLE source_one_t, DESCRIPTOR(row_time), INTERVAL '1' SECOND))\n"
                                    + ") L\n"
                                    + "JOIN\n"
                                    + "(\n"
                                    + "    SELECT * FROM TABLE(TUMBLE(TABLE source_two_t, DESCRIPTOR(row_time), INTERVAL '1' SECOND))\n"
                                    + ") R\n"
                                    + "ON L.name = R.name\n"
                                    + "AND L.window_start = R.window_start\n"
                                    + "AND L.window_end = R.window_end")
                    .build();

    static final TableTestProgram MINI_BATCH_ASSIGNER_PROC_TIME =
            TableTestProgram.of(
                            "mini-batch-assigner-proc-time",
                            "validates mini batch assigner with proc time")
                    .setupTableSource(
                            SourceTestStep.newBuilder("source_t")
                                    .addSchema("a INT", "b BIGINT", "c VARCHAR")
                                    .producedBeforeRestore(
                                            Row.of(1, 1L, "hi"),
                                            Row.of(2, 2L, "hello"),
                                            Row.of(3, 2L, "hello world"))
                                    .producedAfterRestore(
                                            Row.of(3, 2L, "foo"),
                                            Row.of(4, 4L, "bar"),
                                            Row.of(5, 2L, "foo bar"))
                                    .build())
                    .setupConfig(ExecutionConfigOptions.TABLE_EXEC_MINIBATCH_ENABLED, true)
                    .setupConfig(
                            ExecutionConfigOptions.TABLE_EXEC_MINIBATCH_ALLOW_LATENCY,
                            Duration.ofSeconds(1))
                    .setupConfig(ExecutionConfigOptions.TABLE_EXEC_MINIBATCH_SIZE, 5L)
                    .setupTableSink(
                            SinkTestStep.newBuilder("sink_t")
                                    .addSchema("b BIGINT", "a BIGINT")
                                    .consumedBeforeRestore("+I[1, 1]", "+I[2, 2]")
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
}
