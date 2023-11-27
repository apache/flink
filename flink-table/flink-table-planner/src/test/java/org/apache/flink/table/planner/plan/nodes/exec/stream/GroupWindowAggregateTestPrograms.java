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

import org.apache.flink.table.test.program.SinkTestStep;
import org.apache.flink.table.test.program.SourceTestStep;
import org.apache.flink.table.test.program.TableTestProgram;
import org.apache.flink.types.Row;

import java.math.BigDecimal;

/** {@link TableTestProgram} definitions for testing {@link StreamExecGroupWindowAggregate}. */
public class GroupWindowAggregateTestPrograms {

    static final Row[] BEFORE_DATA = {
        Row.of("2020-10-10 00:00:01", 1, 1d, 1f, new BigDecimal("1.11"), "Hi", "a"),
        Row.of("2020-10-10 00:00:02", 2, 2d, 2f, new BigDecimal("2.22"), "Comment#1", "a"),
        Row.of("2020-10-10 00:00:03", 2, 2d, 2f, new BigDecimal("2.22"), "Comment#1", "a"),
        Row.of("2020-10-10 00:00:04", 5, 5d, 5f, new BigDecimal("5.55"), null, "a"),
        Row.of("2020-10-10 00:00:07", 3, 3d, 3f, null, "Hello", "b"),
        // out of order
        Row.of("2020-10-10 00:00:06", 6, 6d, 6f, new BigDecimal("6.66"), "Hi", "b"),
        Row.of("2020-10-10 00:00:08", 3, null, 3f, new BigDecimal("3.33"), "Comment#2", "a"),
        // late event
        Row.of("2020-10-10 00:00:04", 5, 5d, null, new BigDecimal("5.55"), "Hi", "a"),
        Row.of("2020-10-10 00:00:16", 4, 4d, 4f, new BigDecimal("4.44"), "Hi", "b"),
        Row.of("2020-10-10 00:00:32", 7, 7d, 7f, new BigDecimal("7.77"), null, null),
        Row.of("2020-10-10 00:00:34", 1, 3d, 3f, new BigDecimal("3.33"), "Comment#3", "b")
    };

    static final Row[] AFTER_DATA = {
        Row.of("2020-10-10 00:00:41", 10, 3d, 3f, new BigDecimal("4.44"), "Comment#4", "a"),
        Row.of("2020-10-10 00:00:42", 11, 4d, 4f, new BigDecimal("5.44"), "Comment#5", "d"),
        Row.of("2020-10-10 00:00:43", 12, 5d, 5f, new BigDecimal("6.44"), "Comment#6", "c"),
        Row.of("2020-10-10 00:00:44", 13, 6d, 6f, new BigDecimal("7.44"), "Comment#7", "d")
    };

    static final SourceTestStep SOURCE =
            SourceTestStep.newBuilder("source_t")
                    .addSchema(
                            "ts STRING",
                            "a_int INT",
                            "b_double DOUBLE",
                            "c_float FLOAT",
                            "d_bigdec DECIMAL(10, 2)",
                            "`comment` STRING",
                            "name STRING",
                            "`rowtime` AS TO_TIMESTAMP(`ts`)",
                            "`proctime` AS PROCTIME()",
                            "WATERMARK for `rowtime` AS `rowtime` - INTERVAL '1' SECOND")
                    .producedBeforeRestore(BEFORE_DATA)
                    .producedAfterRestore(AFTER_DATA)
                    .build();

    static final TableTestProgram GROUP_TUMBLE_WINDOW_EVENT_TIME =
            TableTestProgram.of(
                            "group-window-aggregate-tumble-event-time",
                            "validates group by using tumbling window with event time")
                    .setupTableSource(SOURCE)
                    .setupTableSink(
                            SinkTestStep.newBuilder("sink_t")
                                    .addSchema(
                                            "name STRING",
                                            "window_start TIMESTAMP(3)",
                                            "window_end TIMESTAMP(3)",
                                            "cnt BIGINT",
                                            "sum_int INT",
                                            "distinct_cnt BIGINT")
                                    .consumedBeforeRestore(
                                            "+I[a, 2020-10-10T00:00, 2020-10-10T00:00:05, 4, 10, 2]",
                                            "+I[a, 2020-10-10T00:00:05, 2020-10-10T00:00:10, 1, 3, 1]",
                                            "+I[b, 2020-10-10T00:00:05, 2020-10-10T00:00:10, 2, 9, 2]",
                                            "+I[b, 2020-10-10T00:00:15, 2020-10-10T00:00:20, 1, 4, 1]")
                                    .consumedAfterRestore(
                                            "+I[b, 2020-10-10T00:00:30, 2020-10-10T00:00:35, 1, 1, 1]",
                                            "+I[null, 2020-10-10T00:00:30, 2020-10-10T00:00:35, 1, 7, 0]",
                                            "+I[a, 2020-10-10T00:00:40, 2020-10-10T00:00:45, 1, 10, 1]",
                                            "+I[c, 2020-10-10T00:00:40, 2020-10-10T00:00:45, 1, 12, 1]",
                                            "+I[d, 2020-10-10T00:00:40, 2020-10-10T00:00:45, 2, 24, 2]")
                                    .build())
                    .runSql(
                            "INSERT INTO sink_t SELECT "
                                    + "name, "
                                    + "TUMBLE_START(rowtime, INTERVAL '5' SECOND) AS window_start, "
                                    + "TUMBLE_END(rowtime, INTERVAL '5' SECOND) AS window_end, "
                                    + "COUNT(*), "
                                    + "SUM(a_int), "
                                    + "COUNT(DISTINCT `comment`) "
                                    + "FROM source_t "
                                    + "GROUP BY name, TUMBLE(rowtime, INTERVAL '5' SECOND)")
                    .build();

    static final TableTestProgram GROUP_TUMBLE_WINDOW_PROC_TIME =
            TableTestProgram.of(
                            "group-window-aggregate-tumble-proc-time",
                            "validates group by using tumbling window with processing time")
                    .setupTableSource(SOURCE)
                    .setupTableSink(
                            SinkTestStep.newBuilder("sink_t")
                                    .addSchema(
                                            "name STRING",
                                            "cnt BIGINT",
                                            "sum_int INT",
                                            "distinct_cnt BIGINT")
                                    .consumedBeforeRestore(
                                            "+I[a, 6, 18, 3]",
                                            "+I[null, 1, 7, 0]",
                                            "+I[b, 4, 14, 3]")
                                    .consumedAfterRestore(
                                            "+I[a, 1, 10, 1]", "+I[c, 1, 12, 1]", "+I[d, 2, 24, 2]")
                                    .build())
                    .runSql(
                            "INSERT INTO sink_t SELECT "
                                    + "name, "
                                    + "COUNT(*), "
                                    + "SUM(a_int), "
                                    + "COUNT(DISTINCT `comment`) "
                                    + "FROM source_t "
                                    + "GROUP BY name, TUMBLE(proctime, INTERVAL '5' SECOND)")
                    .build();

    static final TableTestProgram GROUP_HOP_WINDOW_EVENT_TIME =
            TableTestProgram.of(
                            "group-window-aggregate-hop-event-time",
                            "validates group by using hopping window with event time")
                    .setupTableSource(SOURCE)
                    .setupTableSink(
                            SinkTestStep.newBuilder("sink_t")
                                    .addSchema("name STRING", "cnt BIGINT")
                                    .consumedBeforeRestore(
                                            "+I[a, 4]",
                                            "+I[b, 2]",
                                            "+I[a, 6]",
                                            "+I[a, 1]",
                                            "+I[b, 2]",
                                            "+I[b, 1]",
                                            "+I[b, 1]")
                                    .consumedAfterRestore(
                                            "+I[b, 1]",
                                            "+I[null, 1]",
                                            "+I[b, 1]",
                                            "+I[null, 1]",
                                            "+I[a, 1]",
                                            "+I[d, 2]",
                                            "+I[c, 1]",
                                            "+I[a, 1]",
                                            "+I[c, 1]",
                                            "+I[d, 2]")
                                    .build())
                    .runSql(
                            "INSERT INTO sink_t SELECT "
                                    + "name, "
                                    + "COUNT(*) "
                                    + "FROM source_t "
                                    + "GROUP BY name, HOP(rowtime, INTERVAL '5' SECOND, INTERVAL '10' SECOND)")
                    .build();

    static final TableTestProgram GROUP_HOP_WINDOW_PROC_TIME =
            TableTestProgram.of(
                            "group-window-aggregate-hop-proc-time",
                            "validates group by using hopping window with processing time")
                    .setupTableSource(SOURCE)
                    .setupTableSink(
                            SinkTestStep.newBuilder("sink_t")
                                    .addSchema("name STRING", "cnt BIGINT")
                                    .consumedBeforeRestore(
                                            "+I[a, 6]",
                                            "+I[b, 4]",
                                            "+I[null, 1]",
                                            "+I[a, 6]",
                                            "+I[null, 1]",
                                            "+I[b, 4]")
                                    .consumedAfterRestore(
                                            "+I[a, 1]",
                                            "+I[d, 2]",
                                            "+I[c, 1]",
                                            "+I[a, 1]",
                                            "+I[c, 1]",
                                            "+I[d, 2]")
                                    .build())
                    .runSql(
                            "INSERT INTO sink_t SELECT "
                                    + "name, "
                                    + "COUNT(*) "
                                    + "FROM source_t "
                                    + "GROUP BY name, HOP(proctime, INTERVAL '5' SECOND, INTERVAL '10' SECOND)")
                    .build();

    static final TableTestProgram GROUP_SESSION_WINDOW_EVENT_TIME =
            TableTestProgram.of(
                            "group-window-aggregate-session-event-time",
                            "validates group by using session window with event time")
                    .setupTableSource(SOURCE)
                    .setupTableSink(
                            SinkTestStep.newBuilder("sink_t")
                                    .addSchema("name STRING", "cnt BIGINT")
                                    .consumedBeforeRestore(
                                            "+I[a, 4]", "+I[b, 2]", "+I[a, 1]", "+I[b, 1]")
                                    .consumedAfterRestore(
                                            "+I[null, 1]",
                                            "+I[b, 1]",
                                            "+I[a, 1]",
                                            "+I[c, 1]",
                                            "+I[d, 2]")
                                    .build())
                    .runSql(
                            "INSERT INTO sink_t SELECT "
                                    + "name, "
                                    + "COUNT(*) "
                                    + "FROM source_t "
                                    + "GROUP BY name, SESSION(rowtime, INTERVAL '3' SECOND)")
                    .build();

    static final TableTestProgram GROUP_SESSION_WINDOW_PROC_TIME =
            TableTestProgram.of(
                            "group-window-aggregate-session-proc-time",
                            "validates group by using session window with processing time")
                    .setupTableSource(SOURCE)
                    .setupTableSink(
                            SinkTestStep.newBuilder("sink_t")
                                    .addSchema("name STRING", "cnt BIGINT")
                                    .consumedBeforeRestore("+I[a, 6]", "+I[null, 1]", "+I[b, 4]")
                                    .consumedAfterRestore("+I[a, 1]", "+I[c, 1]", "+I[d, 2]")
                                    .build())
                    .runSql(
                            "INSERT INTO sink_t SELECT "
                                    + "name, "
                                    + "COUNT(*) "
                                    + "FROM source_t "
                                    + "GROUP BY name, SESSION(proctime, INTERVAL '3' SECOND)")
                    .build();
}
