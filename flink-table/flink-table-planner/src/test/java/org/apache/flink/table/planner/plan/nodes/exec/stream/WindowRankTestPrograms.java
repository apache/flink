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

/** {@link TableTestProgram} definitions for testing {@link StreamExecWindowRank}. */
public class WindowRankTestPrograms {

    static final Row[] BEFORE_DATA = {
        Row.of("2020-04-15 08:00:05", new BigDecimal(4.00), "A", "supplier1"),
        Row.of("2020-04-15 08:00:06", new BigDecimal(4.00), "C", "supplier2"),
        Row.of("2020-04-15 08:00:07", new BigDecimal(2.00), "G", "supplier1"),
        Row.of("2020-04-15 08:00:08", new BigDecimal(2.00), "B", "supplier3"),
        Row.of("2020-04-15 08:00:09", new BigDecimal(5.00), "D", "supplier4"),
        Row.of("2020-04-15 08:00:11", new BigDecimal(2.00), "B", "supplier3"),
        Row.of("2020-04-15 08:00:13", new BigDecimal(1.00), "E", "supplier1"),
        Row.of("2020-04-15 08:00:15", new BigDecimal(3.00), "H", "supplier2"),
        Row.of("2020-04-15 08:00:17", new BigDecimal(6.00), "F", "supplier5")
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
        "window_start TIMESTAMP(3)",
        "window_end TIMESTAMP(3)",
        "bid_time TIMESTAMP(3)",
        "supplier_id STRING",
        "price DECIMAL(10,2)",
        "item STRING",
        "row_num BIGINT"
    };

    static final String[] SINK_TVF_AGG_SCHEMA = {
        "window_start TIMESTAMP(3)",
        "window_end TIMESTAMP(3)",
        "supplier_id STRING",
        "total_price DECIMAL(10,2)",
        "cnt BIGINT",
        "row_num BIGINT"
    };

    static final String TUMBLE_TVF =
            "TABLE(TUMBLE(TABLE bid_t, DESCRIPTOR(bid_time), INTERVAL '10' SECOND))";

    static final String HOP_TVF =
            "TABLE(HOP(TABLE bid_t, DESCRIPTOR(bid_time), INTERVAL '5' SECOND, INTERVAL '10' SECOND))";

    static final String CUMULATE_TVF =
            "TABLE(CUMULATE(TABLE bid_t, DESCRIPTOR(bid_time), INTERVAL '5' SECOND, INTERVAL '10' SECOND))";

    static final String QUERY_TVF_TOP_N =
            "INSERT INTO sink_t SELECT *\n"
                    + "  FROM (\n"
                    + "    SELECT\n"
                    + "         window_start,\n"
                    + "         window_end, \n"
                    + "         bid_time,\n"
                    + "         supplier_id,\n"
                    + "         price,\n"
                    + "         item,\n"
                    + "         ROW_NUMBER() OVER (PARTITION BY window_start, window_end ORDER BY price %s) AS row_num\n"
                    + "    FROM %s\n" // Window TVF
                    + "  ) WHERE row_num <= 3"; // row_num must be greater than 1

    static final String QUERY_TVF_AGG_TOP_N =
            "INSERT INTO sink_t SELECT *\n"
                    + "  FROM (\n"
                    + "    SELECT *, ROW_NUMBER() OVER (PARTITION BY window_start, window_end ORDER BY price %s) as row_num\n"
                    + "    FROM (\n"
                    + "      SELECT window_start, window_end, supplier_id, SUM(price) as price, COUNT(*) as cnt\n"
                    + "      FROM %s\n" // Window TVF
                    + "      GROUP BY window_start, window_end, supplier_id\n"
                    + "    )\n"
                    + "  ) WHERE row_num <= 3"; // row_num must be greater than 1

    static final TableTestProgram WINDOW_RANK_TUMBLE_TVF_MIN_TOP_N =
            TableTestProgram.of(
                            "window-rank-tumble-tvf-min-top-n",
                            "validates window min top-n follows after tumbling window")
                    .setupTableSource(SOURCE)
                    .setupTableSink(
                            SinkTestStep.newBuilder("sink_t")
                                    .addSchema(SINK_SCHEMA)
                                    .consumedBeforeRestore(
                                            "+I[2020-04-15T08:00, 2020-04-15T08:00:10, 2020-04-15T08:00:07, supplier1, 2.00, G, 1]",
                                            "+I[2020-04-15T08:00, 2020-04-15T08:00:10, 2020-04-15T08:00:08, supplier3, 2.00, B, 2]",
                                            "+I[2020-04-15T08:00, 2020-04-15T08:00:10, 2020-04-15T08:00:05, supplier1, 4.00, A, 3]")
                                    .consumedAfterRestore(
                                            "+I[2020-04-15T08:00:10, 2020-04-15T08:00:20, 2020-04-15T08:00:13, supplier1, 1.00, E, 1]",
                                            "+I[2020-04-15T08:00:10, 2020-04-15T08:00:20, 2020-04-15T08:00:11, supplier3, 2.00, B, 2]",
                                            "+I[2020-04-15T08:00:10, 2020-04-15T08:00:20, 2020-04-15T08:00:15, supplier2, 3.00, H, 3]",
                                            "+I[2020-04-15T08:00:20, 2020-04-15T08:00:30, 2020-04-15T08:00:23, supplier4, 1.00, A, 1]",
                                            "+I[2020-04-15T08:00:20, 2020-04-15T08:00:30, 2020-04-15T08:00:21, supplier7, 2.00, B, 2]",
                                            "+I[2020-04-15T08:00:20, 2020-04-15T08:00:30, 2020-04-15T08:00:25, supplier3, 3.00, C, 3]")
                                    .build())
                    .runSql(String.format(QUERY_TVF_TOP_N, "ASC", TUMBLE_TVF))
                    .build();

    static final TableTestProgram WINDOW_RANK_TUMBLE_TVF_AGG_MIN_TOP_N =
            TableTestProgram.of(
                            "window-rank-tumble-tvf-agg-min-top-n",
                            "validates window min top-n with tumbling window follows after window aggregation")
                    .setupTableSource(SOURCE)
                    .setupTableSink(
                            SinkTestStep.newBuilder("sink_t")
                                    .addSchema(SINK_TVF_AGG_SCHEMA)
                                    .consumedBeforeRestore(
                                            "+I[2020-04-15T08:00, 2020-04-15T08:00:10, supplier3, 2.00, 1, 1]",
                                            "+I[2020-04-15T08:00, 2020-04-15T08:00:10, supplier2, 4.00, 1, 2]",
                                            "+I[2020-04-15T08:00, 2020-04-15T08:00:10, supplier4, 5.00, 1, 3]")
                                    .consumedAfterRestore(
                                            "+I[2020-04-15T08:00:10, 2020-04-15T08:00:20, supplier1, 1.00, 1, 1]",
                                            "+I[2020-04-15T08:00:10, 2020-04-15T08:00:20, supplier3, 2.00, 1, 2]",
                                            "+I[2020-04-15T08:00:10, 2020-04-15T08:00:20, supplier2, 3.00, 1, 3]",
                                            "+I[2020-04-15T08:00:20, 2020-04-15T08:00:30, supplier4, 1.00, 1, 1]",
                                            "+I[2020-04-15T08:00:20, 2020-04-15T08:00:30, supplier7, 2.00, 1, 2]",
                                            "+I[2020-04-15T08:00:20, 2020-04-15T08:00:30, supplier3, 3.00, 1, 3]")
                                    .build())
                    .runSql(String.format(QUERY_TVF_AGG_TOP_N, "ASC", TUMBLE_TVF))
                    .build();

    static final TableTestProgram WINDOW_RANK_TUMBLE_TVF_MAX_TOP_N =
            TableTestProgram.of(
                            "window-rank-tumble-tvf-max-top-n",
                            "validates window max top-n follows after tumbling window")
                    .setupTableSource(SOURCE)
                    .setupTableSink(
                            SinkTestStep.newBuilder("sink_t")
                                    .addSchema(SINK_SCHEMA)
                                    .consumedBeforeRestore(
                                            "+I[2020-04-15T08:00, 2020-04-15T08:00:10, 2020-04-15T08:00:09, supplier4, 5.00, D, 1]",
                                            "+I[2020-04-15T08:00, 2020-04-15T08:00:10, 2020-04-15T08:00:05, supplier1, 4.00, A, 2]",
                                            "+I[2020-04-15T08:00, 2020-04-15T08:00:10, 2020-04-15T08:00:06, supplier2, 4.00, C, 3]")
                                    .consumedAfterRestore(
                                            "+I[2020-04-15T08:00:10, 2020-04-15T08:00:20, 2020-04-15T08:00:17, supplier5, 6.00, F, 1]",
                                            "+I[2020-04-15T08:00:10, 2020-04-15T08:00:20, 2020-04-15T08:00:15, supplier2, 3.00, H, 2]",
                                            "+I[2020-04-15T08:00:10, 2020-04-15T08:00:20, 2020-04-15T08:00:11, supplier3, 2.00, B, 3]",
                                            "+I[2020-04-15T08:00:20, 2020-04-15T08:00:30, 2020-04-15T08:00:28, supplier8, 6.00, A, 1]",
                                            "+I[2020-04-15T08:00:20, 2020-04-15T08:00:30, 2020-04-15T08:00:25, supplier3, 3.00, C, 2]",
                                            "+I[2020-04-15T08:00:20, 2020-04-15T08:00:30, 2020-04-15T08:00:21, supplier7, 2.00, B, 3]")
                                    .build())
                    .runSql(String.format(QUERY_TVF_TOP_N, "DESC", TUMBLE_TVF))
                    .build();

    static final TableTestProgram WINDOW_RANK_TUMBLE_TVF_AGG_MAX_TOP_N =
            TableTestProgram.of(
                            "window-rank-tumble-tvf-agg-max-top-n",
                            "validates window max top-n with tumbling window follows after window aggregation")
                    .setupTableSource(SOURCE)
                    .setupTableSink(
                            SinkTestStep.newBuilder("sink_t")
                                    .addSchema(SINK_TVF_AGG_SCHEMA)
                                    .consumedBeforeRestore(
                                            "+I[2020-04-15T08:00, 2020-04-15T08:00:10, supplier1, 6.00, 2, 1]",
                                            "+I[2020-04-15T08:00, 2020-04-15T08:00:10, supplier4, 5.00, 1, 2]",
                                            "+I[2020-04-15T08:00, 2020-04-15T08:00:10, supplier2, 4.00, 1, 3]")
                                    .consumedAfterRestore(
                                            "+I[2020-04-15T08:00:10, 2020-04-15T08:00:20, supplier5, 6.00, 1, 1]",
                                            "+I[2020-04-15T08:00:10, 2020-04-15T08:00:20, supplier2, 3.00, 1, 2]",
                                            "+I[2020-04-15T08:00:10, 2020-04-15T08:00:20, supplier3, 2.00, 1, 3]",
                                            "+I[2020-04-15T08:00:20, 2020-04-15T08:00:30, supplier8, 6.00, 1, 1]",
                                            "+I[2020-04-15T08:00:20, 2020-04-15T08:00:30, supplier3, 3.00, 1, 2]",
                                            "+I[2020-04-15T08:00:20, 2020-04-15T08:00:30, supplier7, 2.00, 1, 3]")
                                    .build())
                    .runSql(String.format(QUERY_TVF_AGG_TOP_N, "DESC", TUMBLE_TVF))
                    .build();

    static final TableTestProgram WINDOW_RANK_HOP_TVF_MIN_TOP_N =
            TableTestProgram.of(
                            "window-rank-hop-tvf-min-top-n",
                            "validates window min top-n follows after hop window")
                    .setupTableSource(SOURCE)
                    .setupTableSink(
                            SinkTestStep.newBuilder("sink_t")
                                    .addSchema(SINK_SCHEMA)
                                    .consumedBeforeRestore(
                                            "+I[2020-04-15T08:00, 2020-04-15T08:00:10, 2020-04-15T08:00:07, supplier1, 2.00, G, 1]",
                                            "+I[2020-04-15T08:00, 2020-04-15T08:00:10, 2020-04-15T08:00:08, supplier3, 2.00, B, 2]",
                                            "+I[2020-04-15T08:00, 2020-04-15T08:00:10, 2020-04-15T08:00:05, supplier1, 4.00, A, 3]",
                                            "+I[2020-04-15T08:00:05, 2020-04-15T08:00:15, 2020-04-15T08:00:13, supplier1, 1.00, E, 1]",
                                            "+I[2020-04-15T08:00:05, 2020-04-15T08:00:15, 2020-04-15T08:00:07, supplier1, 2.00, G, 2]",
                                            "+I[2020-04-15T08:00:05, 2020-04-15T08:00:15, 2020-04-15T08:00:08, supplier3, 2.00, B, 3]")
                                    .consumedAfterRestore(
                                            "+I[2020-04-15T08:00:10, 2020-04-15T08:00:20, 2020-04-15T08:00:13, supplier1, 1.00, E, 1]",
                                            "+I[2020-04-15T08:00:10, 2020-04-15T08:00:20, 2020-04-15T08:00:11, supplier3, 2.00, B, 2]",
                                            "+I[2020-04-15T08:00:10, 2020-04-15T08:00:20, 2020-04-15T08:00:15, supplier2, 3.00, H, 3]",
                                            "+I[2020-04-15T08:00:15, 2020-04-15T08:00:25, 2020-04-15T08:00:23, supplier4, 1.00, A, 1]",
                                            "+I[2020-04-15T08:00:15, 2020-04-15T08:00:25, 2020-04-15T08:00:21, supplier7, 2.00, B, 2]",
                                            "+I[2020-04-15T08:00:15, 2020-04-15T08:00:25, 2020-04-15T08:00:15, supplier2, 3.00, H, 3]",
                                            "+I[2020-04-15T08:00:20, 2020-04-15T08:00:30, 2020-04-15T08:00:23, supplier4, 1.00, A, 1]",
                                            "+I[2020-04-15T08:00:20, 2020-04-15T08:00:30, 2020-04-15T08:00:21, supplier7, 2.00, B, 2]",
                                            "+I[2020-04-15T08:00:20, 2020-04-15T08:00:30, 2020-04-15T08:00:25, supplier3, 3.00, C, 3]",
                                            "+I[2020-04-15T08:00:25, 2020-04-15T08:00:35, 2020-04-15T08:00:25, supplier3, 3.00, C, 1]",
                                            "+I[2020-04-15T08:00:25, 2020-04-15T08:00:35, 2020-04-15T08:00:28, supplier8, 6.00, A, 2]")
                                    .build())
                    .runSql(String.format(QUERY_TVF_TOP_N, "ASC", HOP_TVF))
                    .build();

    static final TableTestProgram WINDOW_RANK_CUMULATE_TVF_MIN_TOP_N =
            TableTestProgram.of(
                            "window-rank-cumulate-tvf-min-top-n",
                            "validates window min top-n follows after cumulate window")
                    .setupTableSource(SOURCE)
                    .setupTableSink(
                            SinkTestStep.newBuilder("sink_t")
                                    .addSchema(SINK_SCHEMA)
                                    .consumedBeforeRestore(
                                            "+I[2020-04-15T08:00, 2020-04-15T08:00:10, 2020-04-15T08:00:07, supplier1, 2.00, G, 1]",
                                            "+I[2020-04-15T08:00, 2020-04-15T08:00:10, 2020-04-15T08:00:08, supplier3, 2.00, B, 2]",
                                            "+I[2020-04-15T08:00, 2020-04-15T08:00:10, 2020-04-15T08:00:05, supplier1, 4.00, A, 3]",
                                            "+I[2020-04-15T08:00:10, 2020-04-15T08:00:15, 2020-04-15T08:00:13, supplier1, 1.00, E, 1]",
                                            "+I[2020-04-15T08:00:10, 2020-04-15T08:00:15, 2020-04-15T08:00:11, supplier3, 2.00, B, 2]")
                                    .consumedAfterRestore(
                                            "+I[2020-04-15T08:00:10, 2020-04-15T08:00:20, 2020-04-15T08:00:13, supplier1, 1.00, E, 1]",
                                            "+I[2020-04-15T08:00:10, 2020-04-15T08:00:20, 2020-04-15T08:00:11, supplier3, 2.00, B, 2]",
                                            "+I[2020-04-15T08:00:10, 2020-04-15T08:00:20, 2020-04-15T08:00:15, supplier2, 3.00, H, 3]",
                                            "+I[2020-04-15T08:00:20, 2020-04-15T08:00:25, 2020-04-15T08:00:23, supplier4, 1.00, A, 1]",
                                            "+I[2020-04-15T08:00:20, 2020-04-15T08:00:25, 2020-04-15T08:00:21, supplier7, 2.00, B, 2]",
                                            "+I[2020-04-15T08:00:20, 2020-04-15T08:00:30, 2020-04-15T08:00:23, supplier4, 1.00, A, 1]",
                                            "+I[2020-04-15T08:00:20, 2020-04-15T08:00:30, 2020-04-15T08:00:21, supplier7, 2.00, B, 2]",
                                            "+I[2020-04-15T08:00:20, 2020-04-15T08:00:30, 2020-04-15T08:00:25, supplier3, 3.00, C, 3]")
                                    .build())
                    .runSql(String.format(QUERY_TVF_TOP_N, "ASC", CUMULATE_TVF))
                    .build();
}
