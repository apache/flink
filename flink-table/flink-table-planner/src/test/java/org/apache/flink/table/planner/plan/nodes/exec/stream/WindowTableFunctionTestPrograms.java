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

import org.apache.flink.table.api.config.TableConfigOptions;
import org.apache.flink.table.test.program.SinkTestStep;
import org.apache.flink.table.test.program.SourceTestStep;
import org.apache.flink.table.test.program.TableTestProgram;
import org.apache.flink.types.Row;

import java.math.BigDecimal;

/** {@link TableTestProgram} definitions for testing {@link StreamExecWindowJoin}. */
public class WindowTableFunctionTestPrograms {

    static final Row[] BEFORE_DATA = {
        Row.of("2020-04-15 08:00:05", new BigDecimal(4.00), "C"),
        Row.of("2020-04-15 08:00:07", new BigDecimal(2.00), "A"),
        Row.of("2020-04-15 08:00:09", new BigDecimal(5.00), "D"),
        Row.of("2020-04-15 08:00:11", new BigDecimal(3.00), "B"),
        Row.of("2020-04-15 08:00:13", new BigDecimal(1.00), "E"),
        Row.of("2020-04-15 08:00:17", new BigDecimal(6.00), "F")
    };

    static final Row[] AFTER_DATA = {
        Row.of("2020-04-15 08:00:21", new BigDecimal(4.00), "A"),
        Row.of("2020-04-15 08:00:27", new BigDecimal(5.00), "C")
    };

    static final SourceTestStep SOURCE =
            SourceTestStep.newBuilder("bid_t")
                    .addSchema(
                            "ts STRING",
                            "price DECIMAL(10,2)",
                            "item STRING",
                            "`bid_time` AS TO_TIMESTAMP(`ts`)",
                            "`proc_time` AS PROCTIME()",
                            "WATERMARK for `bid_time` AS `bid_time` - INTERVAL '1' SECOND")
                    .producedBeforeRestore(BEFORE_DATA)
                    .producedAfterRestore(AFTER_DATA)
                    .build();

    static final String TUMBLE_TVF =
            "TABLE(TUMBLE(TABLE bid_t, DESCRIPTOR(%s), INTERVAL '10' SECOND))";

    static final String TUMBLE_TVF_OFFSET =
            "TABLE(TUMBLE(TABLE bid_t, DESCRIPTOR(%s), INTERVAL '10' SECOND, INTERVAL '%s' SECOND))";

    static final String HOP_TVF =
            "TABLE(HOP(TABLE bid_t, DESCRIPTOR(%s), INTERVAL '5' SECOND, INTERVAL '10' SECOND))";

    static final String CUMULATE_TVF =
            "TABLE(CUMULATE(TABLE bid_t, DESCRIPTOR(%s), INTERVAL '5' SECOND, INTERVAL '10' SECOND))";

    static final String[] SINK_TVF_SCHEMA = {
        "bid_time TIMESTAMP(3)",
        "price DECIMAL(10,2)",
        "item STRING",
        "window_start TIMESTAMP(3)",
        "window_end TIMESTAMP(3)",
        "window_time TIMESTAMP_LTZ"
    };

    static final String[] SINK_TVF_AGG_SCHEMA = {
        "window_start TIMESTAMP(3)", "window_end TIMESTAMP(3)", "price DECIMAL(10,2)"
    };

    static final String[] SINK_TVF_AGG_PROC_TIME_SCHEMA = {"price DECIMAL(10,2)"};

    static final String QUERY_TVF =
            "INSERT INTO sink_t SELECT\n "
                    + "     bid_time,\n"
                    + "     price,\n"
                    + "     item,\n"
                    + "     window_start,\n"
                    + "     window_end,\n"
                    + "     window_time\n"
                    + " FROM\n"
                    + "     %s";

    static final String QUERY_TVF_AGG =
            "INSERT INTO sink_t SELECT\n"
                    + "     window_start,\n"
                    + "     window_end,\n"
                    + "     SUM(price)\n"
                    + " FROM\n"
                    + "     %s\n"
                    + " GROUP BY window_start, window_end";

    static final String QUERY_TVF_AGG_PROC_TIME =
            "INSERT INTO sink_t SELECT\n"
                    + "     SUM(price)\n"
                    + " FROM\n"
                    + "     %s\n"
                    + " GROUP BY window_start, window_end";

    static final TableTestProgram WINDOW_TABLE_FUNCTION_TUMBLE_TVF =
            TableTestProgram.of(
                            "window-table-function-tumble-tvf",
                            "validates window table function using tumble tvf windows")
                    .setupTableSource(SOURCE)
                    .setupTableSink(
                            SinkTestStep.newBuilder("sink_t")
                                    .addSchema(SINK_TVF_SCHEMA)
                                    .consumedBeforeRestore(
                                            "+I[2020-04-15T08:00:05, 4.00, C, 2020-04-15T08:00, 2020-04-15T08:00:10, 2020-04-15T08:00:09.999Z]",
                                            "+I[2020-04-15T08:00:07, 2.00, A, 2020-04-15T08:00, 2020-04-15T08:00:10, 2020-04-15T08:00:09.999Z]",
                                            "+I[2020-04-15T08:00:09, 5.00, D, 2020-04-15T08:00, 2020-04-15T08:00:10, 2020-04-15T08:00:09.999Z]",
                                            "+I[2020-04-15T08:00:11, 3.00, B, 2020-04-15T08:00:10, 2020-04-15T08:00:20, 2020-04-15T08:00:19.999Z]",
                                            "+I[2020-04-15T08:00:13, 1.00, E, 2020-04-15T08:00:10, 2020-04-15T08:00:20, 2020-04-15T08:00:19.999Z]",
                                            "+I[2020-04-15T08:00:17, 6.00, F, 2020-04-15T08:00:10, 2020-04-15T08:00:20, 2020-04-15T08:00:19.999Z]")
                                    .consumedAfterRestore(
                                            "+I[2020-04-15T08:00:21, 4.00, A, 2020-04-15T08:00:20, 2020-04-15T08:00:30, 2020-04-15T08:00:29.999Z]",
                                            "+I[2020-04-15T08:00:27, 5.00, C, 2020-04-15T08:00:20, 2020-04-15T08:00:30, 2020-04-15T08:00:29.999Z]")
                                    .build())
                    .setupConfig(TableConfigOptions.LOCAL_TIME_ZONE, "UTC")
                    .runSql(String.format(QUERY_TVF, String.format(TUMBLE_TVF, "bid_time")))
                    .build();

    static final TableTestProgram WINDOW_TABLE_FUNCTION_TUMBLE_TVF_POSITIVE_OFFSET =
            TableTestProgram.of(
                            "window-table-function-tumble-tvf-positive-offset",
                            "validates window table function using tumble tvf windows with positive offset")
                    .setupTableSource(SOURCE)
                    .setupTableSink(
                            SinkTestStep.newBuilder("sink_t")
                                    .addSchema(SINK_TVF_SCHEMA)
                                    .consumedBeforeRestore(
                                            "+I[2020-04-15T08:00:05, 4.00, C, 2020-04-15T07:59:56, 2020-04-15T08:00:06, 2020-04-15T08:00:05.999Z]",
                                            "+I[2020-04-15T08:00:07, 2.00, A, 2020-04-15T08:00:06, 2020-04-15T08:00:16, 2020-04-15T08:00:15.999Z]",
                                            "+I[2020-04-15T08:00:09, 5.00, D, 2020-04-15T08:00:06, 2020-04-15T08:00:16, 2020-04-15T08:00:15.999Z]",
                                            "+I[2020-04-15T08:00:11, 3.00, B, 2020-04-15T08:00:06, 2020-04-15T08:00:16, 2020-04-15T08:00:15.999Z]",
                                            "+I[2020-04-15T08:00:13, 1.00, E, 2020-04-15T08:00:06, 2020-04-15T08:00:16, 2020-04-15T08:00:15.999Z]",
                                            "+I[2020-04-15T08:00:17, 6.00, F, 2020-04-15T08:00:16, 2020-04-15T08:00:26, 2020-04-15T08:00:25.999Z]")
                                    .consumedAfterRestore(
                                            "+I[2020-04-15T08:00:21, 4.00, A, 2020-04-15T08:00:16, 2020-04-15T08:00:26, 2020-04-15T08:00:25.999Z]",
                                            "+I[2020-04-15T08:00:27, 5.00, C, 2020-04-15T08:00:26, 2020-04-15T08:00:36, 2020-04-15T08:00:35.999Z]")
                                    .build())
                    .setupConfig(TableConfigOptions.LOCAL_TIME_ZONE, "UTC")
                    .runSql(
                            String.format(
                                    QUERY_TVF, String.format(TUMBLE_TVF_OFFSET, "bid_time", "6")))
                    .build();

    static final TableTestProgram WINDOW_TABLE_FUNCTION_TUMBLE_TVF_NEGATIVE_OFFSET =
            TableTestProgram.of(
                            "window-table-function-tumble-tvf-negative-offset",
                            "validates window table function using tumble tvf windows with negative offset")
                    .setupTableSource(SOURCE)
                    .setupTableSink(
                            SinkTestStep.newBuilder("sink_t")
                                    .addSchema(SINK_TVF_SCHEMA)
                                    .consumedBeforeRestore(
                                            "+I[2020-04-15T08:00:05, 4.00, C, 2020-04-15T08:00:04, 2020-04-15T08:00:14, 2020-04-15T08:00:13.999Z]",
                                            "+I[2020-04-15T08:00:07, 2.00, A, 2020-04-15T08:00:04, 2020-04-15T08:00:14, 2020-04-15T08:00:13.999Z]",
                                            "+I[2020-04-15T08:00:09, 5.00, D, 2020-04-15T08:00:04, 2020-04-15T08:00:14, 2020-04-15T08:00:13.999Z]",
                                            "+I[2020-04-15T08:00:11, 3.00, B, 2020-04-15T08:00:04, 2020-04-15T08:00:14, 2020-04-15T08:00:13.999Z]",
                                            "+I[2020-04-15T08:00:13, 1.00, E, 2020-04-15T08:00:04, 2020-04-15T08:00:14, 2020-04-15T08:00:13.999Z]",
                                            "+I[2020-04-15T08:00:17, 6.00, F, 2020-04-15T08:00:14, 2020-04-15T08:00:24, 2020-04-15T08:00:23.999Z]")
                                    .consumedAfterRestore(
                                            "+I[2020-04-15T08:00:21, 4.00, A, 2020-04-15T08:00:14, 2020-04-15T08:00:24, 2020-04-15T08:00:23.999Z]",
                                            "+I[2020-04-15T08:00:27, 5.00, C, 2020-04-15T08:00:24, 2020-04-15T08:00:34, 2020-04-15T08:00:33.999Z]")
                                    .build())
                    .setupConfig(TableConfigOptions.LOCAL_TIME_ZONE, "UTC")
                    .runSql(
                            String.format(
                                    QUERY_TVF, String.format(TUMBLE_TVF_OFFSET, "bid_time", "-6")))
                    .build();

    static final TableTestProgram WINDOW_TABLE_FUNCTION_TUMBLE_TVF_AGG =
            TableTestProgram.of(
                            "window-table-function-tumble-tvf-agg",
                            "validates window table function using tumble tvf windows with aggregation")
                    .setupTableSource(SOURCE)
                    .setupTableSink(
                            SinkTestStep.newBuilder("sink_t")
                                    .addSchema(SINK_TVF_AGG_SCHEMA)
                                    .consumedBeforeRestore(
                                            "+I[2020-04-15T08:00, 2020-04-15T08:00:10, 11.00]")
                                    .consumedAfterRestore(
                                            "+I[2020-04-15T08:00:10, 2020-04-15T08:00:20, 10.00]",
                                            "+I[2020-04-15T08:00:20, 2020-04-15T08:00:30, 9.00]")
                                    .build())
                    .runSql(String.format(QUERY_TVF_AGG, String.format(TUMBLE_TVF, "bid_time")))
                    .build();

    static final TableTestProgram WINDOW_TABLE_FUNCTION_TUMBLE_TVF_AGG_PROC_TIME =
            TableTestProgram.of(
                            "window-table-function-tumble-tvf-agg-proc-time",
                            "validates window table function using tumble tvf windows with aggregation and processing time")
                    .setupTableSource(SOURCE)
                    .setupTableSink(
                            SinkTestStep.newBuilder("sink_t")
                                    .addSchema(SINK_TVF_AGG_PROC_TIME_SCHEMA)
                                    .consumedBeforeRestore("+I[21.00]")
                                    .consumedAfterRestore("+I[9.00]")
                                    .build())
                    .runSql(
                            String.format(
                                    QUERY_TVF_AGG_PROC_TIME,
                                    String.format(TUMBLE_TVF, "proc_time")))
                    .build();

    static final TableTestProgram WINDOW_TABLE_FUNCTION_HOP_TVF =
            TableTestProgram.of(
                            "window-table-function-hop-tvf",
                            "validates window table function using hop tvf windows")
                    .setupTableSource(SOURCE)
                    .setupTableSink(
                            SinkTestStep.newBuilder("sink_t")
                                    .addSchema(SINK_TVF_SCHEMA)
                                    .consumedBeforeRestore(
                                            "+I[2020-04-15T08:00:05, 4.00, C, 2020-04-15T08:00:05, 2020-04-15T08:00:15, 2020-04-15T08:00:14.999Z]",
                                            "+I[2020-04-15T08:00:05, 4.00, C, 2020-04-15T08:00, 2020-04-15T08:00:10, 2020-04-15T08:00:09.999Z]",
                                            "+I[2020-04-15T08:00:07, 2.00, A, 2020-04-15T08:00:05, 2020-04-15T08:00:15, 2020-04-15T08:00:14.999Z]",
                                            "+I[2020-04-15T08:00:07, 2.00, A, 2020-04-15T08:00, 2020-04-15T08:00:10, 2020-04-15T08:00:09.999Z]",
                                            "+I[2020-04-15T08:00:09, 5.00, D, 2020-04-15T08:00:05, 2020-04-15T08:00:15, 2020-04-15T08:00:14.999Z]",
                                            "+I[2020-04-15T08:00:09, 5.00, D, 2020-04-15T08:00, 2020-04-15T08:00:10, 2020-04-15T08:00:09.999Z]",
                                            "+I[2020-04-15T08:00:11, 3.00, B, 2020-04-15T08:00:10, 2020-04-15T08:00:20, 2020-04-15T08:00:19.999Z]",
                                            "+I[2020-04-15T08:00:11, 3.00, B, 2020-04-15T08:00:05, 2020-04-15T08:00:15, 2020-04-15T08:00:14.999Z]",
                                            "+I[2020-04-15T08:00:13, 1.00, E, 2020-04-15T08:00:10, 2020-04-15T08:00:20, 2020-04-15T08:00:19.999Z]",
                                            "+I[2020-04-15T08:00:13, 1.00, E, 2020-04-15T08:00:05, 2020-04-15T08:00:15, 2020-04-15T08:00:14.999Z]",
                                            "+I[2020-04-15T08:00:17, 6.00, F, 2020-04-15T08:00:15, 2020-04-15T08:00:25, 2020-04-15T08:00:24.999Z]",
                                            "+I[2020-04-15T08:00:17, 6.00, F, 2020-04-15T08:00:10, 2020-04-15T08:00:20, 2020-04-15T08:00:19.999Z]")
                                    .consumedAfterRestore(
                                            "+I[2020-04-15T08:00:21, 4.00, A, 2020-04-15T08:00:20, 2020-04-15T08:00:30, 2020-04-15T08:00:29.999Z]",
                                            "+I[2020-04-15T08:00:21, 4.00, A, 2020-04-15T08:00:15, 2020-04-15T08:00:25, 2020-04-15T08:00:24.999Z]",
                                            "+I[2020-04-15T08:00:27, 5.00, C, 2020-04-15T08:00:25, 2020-04-15T08:00:35, 2020-04-15T08:00:34.999Z]",
                                            "+I[2020-04-15T08:00:27, 5.00, C, 2020-04-15T08:00:20, 2020-04-15T08:00:30, 2020-04-15T08:00:29.999Z]")
                                    .build())
                    .setupConfig(TableConfigOptions.LOCAL_TIME_ZONE, "UTC")
                    .runSql(String.format(QUERY_TVF, String.format(HOP_TVF, "bid_time")))
                    .build();

    static final TableTestProgram WINDOW_TABLE_FUNCTION_HOP_TVF_AGG =
            TableTestProgram.of(
                            "window-table-function-hop-tvf-agg",
                            "validates window table function using hop tvf windows with aggregation")
                    .setupTableSource(SOURCE)
                    .setupTableSink(
                            SinkTestStep.newBuilder("sink_t")
                                    .addSchema(SINK_TVF_AGG_SCHEMA)
                                    .consumedBeforeRestore(
                                            "+I[2020-04-15T08:00, 2020-04-15T08:00:10, 11.00]",
                                            "+I[2020-04-15T08:00:05, 2020-04-15T08:00:15, 15.00]")
                                    .consumedAfterRestore(
                                            "+I[2020-04-15T08:00:10, 2020-04-15T08:00:20, 10.00]",
                                            "+I[2020-04-15T08:00:15, 2020-04-15T08:00:25, 10.00]",
                                            "+I[2020-04-15T08:00:20, 2020-04-15T08:00:30, 9.00]",
                                            "+I[2020-04-15T08:00:25, 2020-04-15T08:00:35, 5.00]")
                                    .build())
                    .runSql(String.format(QUERY_TVF_AGG, String.format(HOP_TVF, "bid_time")))
                    .build();

    static final TableTestProgram WINDOW_TABLE_FUNCTION_HOP_TVF_AGG_PROC_TIME =
            TableTestProgram.of(
                            "window-table-function-hop-tvf-agg-proc-time",
                            "validates window table function using hop tvf windows with aggregation and processing time")
                    .setupTableSource(SOURCE)
                    .setupTableSink(
                            SinkTestStep.newBuilder("sink_t")
                                    .addSchema(SINK_TVF_AGG_PROC_TIME_SCHEMA)
                                    .consumedBeforeRestore("+I[21.00]", "+I[21.00]")
                                    .consumedAfterRestore("+I[9.00]")
                                    .build())
                    .runSql(
                            String.format(
                                    QUERY_TVF_AGG_PROC_TIME, String.format(HOP_TVF, "proc_time")))
                    .build();

    static final TableTestProgram WINDOW_TABLE_FUNCTION_CUMULATE_TVF =
            TableTestProgram.of(
                            "window-table-function-cumulate-tvf",
                            "validates window table function using cumulate tvf windows")
                    .setupTableSource(SOURCE)
                    .setupTableSink(
                            SinkTestStep.newBuilder("sink_t")
                                    .addSchema(SINK_TVF_SCHEMA)
                                    .consumedBeforeRestore(
                                            "+I[2020-04-15T08:00:05, 4.00, C, 2020-04-15T08:00, 2020-04-15T08:00:10, 2020-04-15T08:00:09.999Z]",
                                            "+I[2020-04-15T08:00:07, 2.00, A, 2020-04-15T08:00, 2020-04-15T08:00:10, 2020-04-15T08:00:09.999Z]",
                                            "+I[2020-04-15T08:00:09, 5.00, D, 2020-04-15T08:00, 2020-04-15T08:00:10, 2020-04-15T08:00:09.999Z]",
                                            "+I[2020-04-15T08:00:11, 3.00, B, 2020-04-15T08:00:10, 2020-04-15T08:00:15, 2020-04-15T08:00:14.999Z]",
                                            "+I[2020-04-15T08:00:11, 3.00, B, 2020-04-15T08:00:10, 2020-04-15T08:00:20, 2020-04-15T08:00:19.999Z]",
                                            "+I[2020-04-15T08:00:13, 1.00, E, 2020-04-15T08:00:10, 2020-04-15T08:00:15, 2020-04-15T08:00:14.999Z]",
                                            "+I[2020-04-15T08:00:13, 1.00, E, 2020-04-15T08:00:10, 2020-04-15T08:00:20, 2020-04-15T08:00:19.999Z]",
                                            "+I[2020-04-15T08:00:17, 6.00, F, 2020-04-15T08:00:10, 2020-04-15T08:00:20, 2020-04-15T08:00:19.999Z]")
                                    .consumedAfterRestore(
                                            "+I[2020-04-15T08:00:21, 4.00, A, 2020-04-15T08:00:20, 2020-04-15T08:00:25, 2020-04-15T08:00:24.999Z]",
                                            "+I[2020-04-15T08:00:21, 4.00, A, 2020-04-15T08:00:20, 2020-04-15T08:00:30, 2020-04-15T08:00:29.999Z]",
                                            "+I[2020-04-15T08:00:27, 5.00, C, 2020-04-15T08:00:20, 2020-04-15T08:00:30, 2020-04-15T08:00:29.999Z]")
                                    .build())
                    .setupConfig(TableConfigOptions.LOCAL_TIME_ZONE, "UTC")
                    .runSql(String.format(QUERY_TVF, String.format(CUMULATE_TVF, "bid_time")))
                    .build();

    static final TableTestProgram WINDOW_TABLE_FUNCTION_CUMULATE_TVF_AGG =
            TableTestProgram.of(
                            "window-table-function-cumulate-tvf-agg",
                            "validates window table function using cumulate tvf windows with aggregation")
                    .setupTableSource(SOURCE)
                    .setupTableSink(
                            SinkTestStep.newBuilder("sink_t")
                                    .addSchema(SINK_TVF_AGG_SCHEMA)
                                    .consumedBeforeRestore(
                                            "+I[2020-04-15T08:00, 2020-04-15T08:00:10, 11.00]",
                                            "+I[2020-04-15T08:00:10, 2020-04-15T08:00:15, 4.00]")
                                    .consumedAfterRestore(
                                            "+I[2020-04-15T08:00:10, 2020-04-15T08:00:20, 10.00]",
                                            "+I[2020-04-15T08:00:20, 2020-04-15T08:00:25, 4.00]",
                                            "+I[2020-04-15T08:00:20, 2020-04-15T08:00:30, 9.00]")
                                    .build())
                    .runSql(String.format(QUERY_TVF_AGG, String.format(CUMULATE_TVF, "bid_time")))
                    .build();

    static final TableTestProgram WINDOW_TABLE_FUNCTION_CUMULATE_TVF_AGG_PROC_TIME =
            TableTestProgram.of(
                            "window-table-function-cumulate-tvf-agg-proc-time",
                            "validates window table function using cumulate tvf windows with aggregation")
                    .setupTableSource(SOURCE)
                    .setupTableSink(
                            SinkTestStep.newBuilder("sink_t")
                                    .addSchema(SINK_TVF_AGG_PROC_TIME_SCHEMA)
                                    .consumedBeforeRestore("+I[21.00]", "+I[21.00]")
                                    .consumedAfterRestore("+I[9.00]")
                                    .build())
                    .runSql(
                            String.format(
                                    QUERY_TVF_AGG_PROC_TIME,
                                    String.format(CUMULATE_TVF, "proc_time")))
                    .build();
}
