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

/** {@link TableTestProgram} definitions for testing {@link StreamExecWindowDeduplicate}. */
public class WindowDeduplicateTestPrograms {

    static final Row[] BEFORE_DATA = {
        Row.of("2020-04-15 08:00:05", new BigDecimal(4.00), "A", "supplier1"),
        Row.of("2020-04-15 08:00:06", new BigDecimal(4.00), "A", "supplier2"),
        Row.of("2020-04-15 08:00:07", new BigDecimal(2.00), "G", "supplier1"),
        Row.of("2020-04-15 08:00:08", new BigDecimal(2.00), "A", "supplier3"),
        Row.of("2020-04-15 08:00:09", new BigDecimal(5.00), "D", "supplier4"),
        Row.of("2020-04-15 08:00:11", new BigDecimal(2.00), "B", "supplier3"),
        Row.of("2020-04-15 08:00:13", new BigDecimal(1.00), "E", "supplier1"),
        Row.of("2020-04-15 08:00:15", new BigDecimal(3.00), "B", "supplier2"),
        Row.of("2020-04-15 08:00:17", new BigDecimal(6.00), "D", "supplier5")
    };

    static final Row[] AFTER_DATA = {
        Row.of("2020-04-15 08:00:21", new BigDecimal(2.00), "B", "supplier7"),
        Row.of("2020-04-15 08:00:23", new BigDecimal(1.00), "A", "supplier4"),
        Row.of("2020-04-15 08:00:25", new BigDecimal(3.00), "C", "supplier3"),
        Row.of("2020-04-15 08:00:28", new BigDecimal(6.00), "A", "supplier8")
    };

    static final SourceTestStep SOURCE =
            SourceTestStep.newBuilder("bid_t")
                    .addSchema(
                            "ts STRING",
                            "price DECIMAL(10,2)",
                            "item STRING",
                            "supplier_id STRING",
                            "`bid_time` AS TO_TIMESTAMP(`ts`)",
                            "`proc_time` AS PROCTIME()",
                            "WATERMARK for `bid_time` AS `bid_time` - INTERVAL '1' SECOND")
                    .producedBeforeRestore(BEFORE_DATA)
                    .producedAfterRestore(AFTER_DATA)
                    .build();

    static final String[] SINK_SCHEMA = {
        "bid_time TIMESTAMP(3)",
        "price DECIMAL(10,2)",
        "item STRING",
        "supplier_id STRING",
        "window_start TIMESTAMP(3)",
        "window_end TIMESTAMP(3)",
        "row_num BIGINT"
    };

    static final String TUMBLE_TVF =
            "TABLE(TUMBLE(TABLE bid_t, DESCRIPTOR(bid_time), INTERVAL '10' SECOND))";

    static final String HOP_TVF =
            "TABLE(HOP(TABLE bid_t, DESCRIPTOR(bid_time), INTERVAL '5' SECOND, INTERVAL '10' SECOND))";

    static final String CUMULATIVE_TVF =
            "TABLE(CUMULATE(TABLE bid_t, DESCRIPTOR(bid_time), INTERVAL '5' SECOND, INTERVAL '10' SECOND))";

    static final String CONDITION_ONE = "row_num = 1";

    static final String CONDITION_TWO = "row_num <= 1";

    static final String CONDITION_THREE = "row_num < 2";

    static final String QUERY =
            "INSERT INTO sink_t SELECT *\n"
                    + "  FROM (\n"
                    + "    SELECT\n"
                    + "         bid_time,\n"
                    + "         price,\n"
                    + "         item,\n"
                    + "         supplier_id,\n"
                    + "         window_start,\n"
                    + "         window_end, \n"
                    + "         ROW_NUMBER() OVER (PARTITION BY %s ORDER BY bid_time %s) AS row_num\n" // PARTITION BY must contain window_start, window_end, and optionally other columns
                    + "    FROM %s\n" // Window TVF
                    + "  ) WHERE %s"; // row_num = 1 | row_num <= 1 | row_num < 2

    static final TableTestProgram WINDOW_DEDUPLICATE_ASC_TUMBLE_FIRST_ROW =
            TableTestProgram.of(
                            "window-deduplicate-asc-tumble-first-row",
                            "validates window deduplicate in ascending order with at most one row for each tumbling window")
                    .setupTableSource(SOURCE)
                    .setupTableSink(
                            SinkTestStep.newBuilder("sink_t")
                                    .addSchema(SINK_SCHEMA)
                                    .consumedBeforeRestore(
                                            "+I[2020-04-15T08:00:05, 4.00, A, supplier1, 2020-04-15T08:00, 2020-04-15T08:00:10, 1]")
                                    .consumedAfterRestore(
                                            "+I[2020-04-15T08:00:11, 2.00, B, supplier3, 2020-04-15T08:00:10, 2020-04-15T08:00:20, 1]",
                                            "+I[2020-04-15T08:00:21, 2.00, B, supplier7, 2020-04-15T08:00:20, 2020-04-15T08:00:30, 1]")
                                    .build())
                    .runSql(
                            String.format(
                                    QUERY,
                                    "window_start, window_end",
                                    "ASC",
                                    TUMBLE_TVF,
                                    CONDITION_TWO))
                    .build();

    static final TableTestProgram WINDOW_DEDUPLICATE_ASC_TUMBLE_FIRST_ROW_CONDITION_ONE =
            TableTestProgram.of(
                            "window-deduplicate-asc-tumble-first-row-condition-1",
                            "validates window deduplicate in ascending order with at most one row for each tumbling window")
                    .setupTableSource(SOURCE)
                    .setupTableSink(
                            SinkTestStep.newBuilder("sink_t")
                                    .addSchema(SINK_SCHEMA)
                                    .consumedBeforeRestore(
                                            "+I[2020-04-15T08:00:05, 4.00, A, supplier1, 2020-04-15T08:00, 2020-04-15T08:00:10, 1]")
                                    .consumedAfterRestore(
                                            "+I[2020-04-15T08:00:11, 2.00, B, supplier3, 2020-04-15T08:00:10, 2020-04-15T08:00:20, 1]",
                                            "+I[2020-04-15T08:00:21, 2.00, B, supplier7, 2020-04-15T08:00:20, 2020-04-15T08:00:30, 1]")
                                    .build())
                    .runSql(
                            String.format(
                                    QUERY,
                                    "window_start, window_end",
                                    "ASC",
                                    TUMBLE_TVF,
                                    CONDITION_ONE))
                    .build();

    static final TableTestProgram WINDOW_DEDUPLICATE_ASC_TUMBLE_FIRST_ROW_CONDITION_THREE =
            TableTestProgram.of(
                            "window-deduplicate-asc-tumble-first-row-condition-3",
                            "validates window deduplicate in ascending order with at most one row for each tumbling window")
                    .setupTableSource(SOURCE)
                    .setupTableSink(
                            SinkTestStep.newBuilder("sink_t")
                                    .addSchema(SINK_SCHEMA)
                                    .consumedBeforeRestore(
                                            "+I[2020-04-15T08:00:05, 4.00, A, supplier1, 2020-04-15T08:00, 2020-04-15T08:00:10, 1]")
                                    .consumedAfterRestore(
                                            "+I[2020-04-15T08:00:11, 2.00, B, supplier3, 2020-04-15T08:00:10, 2020-04-15T08:00:20, 1]",
                                            "+I[2020-04-15T08:00:21, 2.00, B, supplier7, 2020-04-15T08:00:20, 2020-04-15T08:00:30, 1]")
                                    .build())
                    .runSql(
                            String.format(
                                    QUERY,
                                    "window_start, window_end",
                                    "ASC",
                                    TUMBLE_TVF,
                                    CONDITION_THREE))
                    .build();

    static final TableTestProgram WINDOW_DEDUPLICATE_ASC_PARTITION_BY_ITEM_TUMBLE_FIRST_ROW =
            TableTestProgram.of(
                            "window-deduplicate-asc-partition-by-item-tumble-first-row",
                            "validates window deduplicate in ascending order with at most one row per item for each tumbling window")
                    .setupTableSource(SOURCE)
                    .setupTableSink(
                            SinkTestStep.newBuilder("sink_t")
                                    .addSchema(SINK_SCHEMA)
                                    .consumedBeforeRestore(
                                            "+I[2020-04-15T08:00:05, 4.00, A, supplier1, 2020-04-15T08:00, 2020-04-15T08:00:10, 1]",
                                            "+I[2020-04-15T08:00:07, 2.00, G, supplier1, 2020-04-15T08:00, 2020-04-15T08:00:10, 1]",
                                            "+I[2020-04-15T08:00:09, 5.00, D, supplier4, 2020-04-15T08:00, 2020-04-15T08:00:10, 1]")
                                    .consumedAfterRestore(
                                            "+I[2020-04-15T08:00:11, 2.00, B, supplier3, 2020-04-15T08:00:10, 2020-04-15T08:00:20, 1]",
                                            "+I[2020-04-15T08:00:13, 1.00, E, supplier1, 2020-04-15T08:00:10, 2020-04-15T08:00:20, 1]",
                                            "+I[2020-04-15T08:00:17, 6.00, D, supplier5, 2020-04-15T08:00:10, 2020-04-15T08:00:20, 1]",
                                            "+I[2020-04-15T08:00:21, 2.00, B, supplier7, 2020-04-15T08:00:20, 2020-04-15T08:00:30, 1]",
                                            "+I[2020-04-15T08:00:23, 1.00, A, supplier4, 2020-04-15T08:00:20, 2020-04-15T08:00:30, 1]",
                                            "+I[2020-04-15T08:00:25, 3.00, C, supplier3, 2020-04-15T08:00:20, 2020-04-15T08:00:30, 1]")
                                    .build())
                    .runSql(
                            String.format(
                                    QUERY,
                                    "window_start, window_end, item",
                                    "ASC",
                                    TUMBLE_TVF,
                                    CONDITION_TWO))
                    .build();

    static final TableTestProgram WINDOW_DEDUPLICATE_DESC_TUMBLE_LAST_ROW =
            TableTestProgram.of(
                            "window-deduplicate-desc-tumble-last-row",
                            "validates window deduplicate in descending order with at most one row for each tumbling window")
                    .setupTableSource(SOURCE)
                    .setupTableSink(
                            SinkTestStep.newBuilder("sink_t")
                                    .addSchema(SINK_SCHEMA)
                                    .consumedBeforeRestore(
                                            "+I[2020-04-15T08:00:09, 5.00, D, supplier4, 2020-04-15T08:00, 2020-04-15T08:00:10, 1]")
                                    .consumedAfterRestore(
                                            "+I[2020-04-15T08:00:17, 6.00, D, supplier5, 2020-04-15T08:00:10, 2020-04-15T08:00:20, 1]",
                                            "+I[2020-04-15T08:00:28, 6.00, A, supplier8, 2020-04-15T08:00:20, 2020-04-15T08:00:30, 1]")
                                    .build())
                    .runSql(
                            String.format(
                                    QUERY,
                                    "window_start, window_end",
                                    "DESC",
                                    TUMBLE_TVF,
                                    CONDITION_TWO))
                    .build();

    static final TableTestProgram WINDOW_DEDUPLICATE_DESC_PARTITION_BY_ITEM_TUMBLE_FIRST_ROW =
            TableTestProgram.of(
                            "window-deduplicate-desc-partition-by-item-tumble-first-row",
                            "validates window deduplicate in descending order with at most one row per item for each tumbling window")
                    .setupTableSource(SOURCE)
                    .setupTableSink(
                            SinkTestStep.newBuilder("sink_t")
                                    .addSchema(SINK_SCHEMA)
                                    .consumedBeforeRestore(
                                            "+I[2020-04-15T08:00:08, 2.00, A, supplier3, 2020-04-15T08:00, 2020-04-15T08:00:10, 1]",
                                            "+I[2020-04-15T08:00:07, 2.00, G, supplier1, 2020-04-15T08:00, 2020-04-15T08:00:10, 1]",
                                            "+I[2020-04-15T08:00:09, 5.00, D, supplier4, 2020-04-15T08:00, 2020-04-15T08:00:10, 1]")
                                    .consumedAfterRestore(
                                            "+I[2020-04-15T08:00:17, 6.00, D, supplier5, 2020-04-15T08:00:10, 2020-04-15T08:00:20, 1]",
                                            "+I[2020-04-15T08:00:13, 1.00, E, supplier1, 2020-04-15T08:00:10, 2020-04-15T08:00:20, 1]",
                                            "+I[2020-04-15T08:00:15, 3.00, B, supplier2, 2020-04-15T08:00:10, 2020-04-15T08:00:20, 1]",
                                            "+I[2020-04-15T08:00:21, 2.00, B, supplier7, 2020-04-15T08:00:20, 2020-04-15T08:00:30, 1]",
                                            "+I[2020-04-15T08:00:28, 6.00, A, supplier8, 2020-04-15T08:00:20, 2020-04-15T08:00:30, 1]",
                                            "+I[2020-04-15T08:00:25, 3.00, C, supplier3, 2020-04-15T08:00:20, 2020-04-15T08:00:30, 1]")
                                    .build())
                    .runSql(
                            String.format(
                                    QUERY,
                                    "window_start, window_end, item",
                                    "DESC",
                                    TUMBLE_TVF,
                                    CONDITION_TWO))
                    .build();

    static final TableTestProgram WINDOW_DEDUPLICATE_ASC_HOP_FIRST_ROW =
            TableTestProgram.of(
                            "window-deduplicate-asc-hop-first-row",
                            "validates window deduplicate in ascending order with one row for each hopping window")
                    .setupTableSource(SOURCE)
                    .setupTableSink(
                            SinkTestStep.newBuilder("sink_t")
                                    .addSchema(SINK_SCHEMA)
                                    .consumedBeforeRestore(
                                            "+I[2020-04-15T08:00:05, 4.00, A, supplier1, 2020-04-15T08:00, 2020-04-15T08:00:10, 1]",
                                            "+I[2020-04-15T08:00:05, 4.00, A, supplier1, 2020-04-15T08:00:05, 2020-04-15T08:00:15, 1]")
                                    .consumedAfterRestore(
                                            "+I[2020-04-15T08:00:11, 2.00, B, supplier3, 2020-04-15T08:00:10, 2020-04-15T08:00:20, 1]",
                                            "+I[2020-04-15T08:00:15, 3.00, B, supplier2, 2020-04-15T08:00:15, 2020-04-15T08:00:25, 1]",
                                            "+I[2020-04-15T08:00:21, 2.00, B, supplier7, 2020-04-15T08:00:20, 2020-04-15T08:00:30, 1]",
                                            "+I[2020-04-15T08:00:25, 3.00, C, supplier3, 2020-04-15T08:00:25, 2020-04-15T08:00:35, 1]")
                                    .build())
                    .runSql(
                            String.format(
                                    QUERY,
                                    "window_start, window_end",
                                    "ASC",
                                    HOP_TVF,
                                    CONDITION_TWO))
                    .build();

    static final TableTestProgram WINDOW_DEDUPLICATE_ASC_CUMULATE_FIRST_ROW =
            TableTestProgram.of(
                            "window-deduplicate-asc-cumulate-first-row",
                            "validates window deduplicate in ascending order with one row for each cumulate window")
                    .setupTableSource(SOURCE)
                    .setupTableSink(
                            SinkTestStep.newBuilder("sink_t")
                                    .addSchema(SINK_SCHEMA)
                                    .consumedBeforeRestore(
                                            "+I[2020-04-15T08:00:05, 4.00, A, supplier1, 2020-04-15T08:00, 2020-04-15T08:00:10, 1]",
                                            "+I[2020-04-15T08:00:11, 2.00, B, supplier3, 2020-04-15T08:00:10, 2020-04-15T08:00:15, 1]")
                                    .consumedAfterRestore(
                                            "+I[2020-04-15T08:00:11, 2.00, B, supplier3, 2020-04-15T08:00:10, 2020-04-15T08:00:20, 1]",
                                            "+I[2020-04-15T08:00:21, 2.00, B, supplier7, 2020-04-15T08:00:20, 2020-04-15T08:00:25, 1]",
                                            "+I[2020-04-15T08:00:21, 2.00, B, supplier7, 2020-04-15T08:00:20, 2020-04-15T08:00:30, 1]")
                                    .build())
                    .runSql(
                            String.format(
                                    QUERY,
                                    "window_start, window_end",
                                    "ASC",
                                    CUMULATIVE_TVF,
                                    CONDITION_TWO))
                    .build();
}
