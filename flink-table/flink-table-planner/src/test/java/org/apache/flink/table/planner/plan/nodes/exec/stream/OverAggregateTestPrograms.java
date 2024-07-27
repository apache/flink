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

import org.apache.flink.table.planner.plan.utils.JavaUserDefinedAggFunctions;
import org.apache.flink.table.test.program.SinkTestStep;
import org.apache.flink.table.test.program.SourceTestStep;
import org.apache.flink.table.test.program.TableTestProgram;
import org.apache.flink.types.Row;

import static org.apache.flink.table.api.config.TableConfigOptions.LOCAL_TIME_ZONE;

/** {@link TableTestProgram} definitions for testing {@link StreamExecOverAggregate}. */
public class OverAggregateTestPrograms {

    private static final Row[] DATA = {
        Row.of(10L, 1L, 1, "Hello"),
        Row.of(15L, 1L, 15, "Hello"),
        Row.of(16L, 1L, 16, "Hello"),
        Row.of(20L, 2L, 2, "Hello"),
        Row.of(20L, 2L, 2, "Hello"),
        Row.of(20L, 2L, 3, "Hello"),
        Row.of(30L, 3L, 3, "Hello"),
        Row.of(40L, 4L, 4, "Hello"),
        Row.of(50L, 5L, 5, "Hello"),
        Row.of(60L, 6L, 6, "Hello"),
        Row.of(65L, 6L, 65, "Hello"),
        Row.of(51L, 19L, 15, "Hello"), // Late?
        Row.of(90L, 6L, 9, "Hello"),
        Row.of(95L, 6L, 18, "Hello"), // out of order
        Row.of(90L, 6L, 10, "Hello"),
        Row.of(90L, 7L, 9, "Hello"),
        Row.of(92L, 7L, 9, "Hello"),
        Row.of(100L, 7L, 7, "Hello World"),
        Row.of(99L, 19L, 15, "Hello"), // Late?
        Row.of(110L, 7L, 17, "Hello World"),
        Row.of(110L, 7L, 77, "Hello World"), // dropped?
        Row.of(140L, 7L, 18, "Hello World"),
        Row.of(53L, 19L, 15, "Hello"), // Late?
        Row.of(150L, 8L, 8, "Hello World"),
        Row.of(200L, 20L, 20, "Hello World"),
        Row.of(12L, 1L, 1, "Hello World"), // dropped
        Row.of(33L, 17L, 1, "Hello"),
        Row.of(37L, 19L, 15, "Hello"),
        Row.of(52L, 19L, 15, "Hello"),
        Row.of(13L, 1L, 1, "Hello"),
        Row.of(19L, 1L, 15, "Hello")
    };

    private static final Row[] AFTER_DATA = {
        Row.of(150L, 8L, 8, "Hello World"),
        Row.of(149L, 8L, 8, "Hello World"),
        Row.of(148L, 8L, 8, "Hello World"),
        Row.of(151L, 8L, 8, "Hello World"),
        Row.of(202L, 20L, 20, "Hello World")
    };

    private static final SourceTestStep SOURCE =
            SourceTestStep.newBuilder("MyTable")
                    .addSchema(
                            "ts bigint",
                            "a bigint",
                            "b int",
                            "c string",
                            "rowtime as TO_TIMESTAMP(FROM_UNIXTIME(ts))",
                            "watermark for rowtime as rowtime")
                    .producedBeforeRestore(DATA)
                    .producedAfterRestore(AFTER_DATA)
                    .build();

    private static final String[] BEFORE_RESTORE_DATA = {
        "+I[Hello, 10, 1970-01-01T00:00:10, 1, 0, 1, 1]",
        "+I[Hello, 15, 1970-01-01T00:00:15, 15, 0, 2, 2]",
        "+I[Hello, 16, 1970-01-01T00:00:16, 16, 0, 3, 3]",
        "+I[Hello, 20, 1970-01-01T00:00:20, 2, 0, 4, 5]",
        "+I[Hello, 30, 1970-01-01T00:00:30, 3, 0, 2, 5]",
        "+I[Hello, 40, 1970-01-01T00:00:40, 4, 0, 2, 7]",
        "+I[Hello, 50, 1970-01-01T00:00:50, 5, 1, 2, 9]",
        "+I[Hello, 60, 1970-01-01T00:01, 6, 2, 2, 11]",
        "+I[Hello, 65, 1970-01-01T00:01:05, 65, 2, 2, 12]",
        "+I[Hello, 90, 1970-01-01T00:01:30, 9, 1, 1, 6]",
        "+I[Hello, 95, 1970-01-01T00:01:35, 18, 2, 2, 12]",
        "+I[Hello World, 100, 1970-01-01T00:01:40, 7, 1, 1, 7]",
        "+I[Hello, 99, 1970-01-01T00:01:39, 15, 3, 3, 31]",
        "+I[Hello World, 110, 1970-01-01T00:01:50, 17, 2, 2, 14]",
        "+I[Hello World, 140, 1970-01-01T00:02:20, 18, 1, 1, 7]",
        "+I[Hello, 53, 1970-01-01T00:00:53, 15, 1, 1, 19]",
        "+I[Hello World, 150, 1970-01-01T00:02:30, 8, 2, 2, 15]",
        "+I[Hello World, 200, 1970-01-01T00:03:20, 20, 1, 1, 20]"
    };
    private static final String[] AFTER_RESTORE_OUTPUT = {
        "+I[Hello, 13, 1970-01-01T00:00:13, 1, 0, 1, 1]",
        "+I[Hello, 19, 1970-01-01T00:00:19, 15, 0, 2, 2]",
        "+I[Hello, 33, 1970-01-01T00:00:33, 1, 1, 1, 17]",
        "+I[Hello, 37, 1970-01-01T00:00:37, 15, 2, 2, 36]",
        "+I[Hello, 52, 1970-01-01T00:00:52, 15, 1, 1, 19]",
        "+I[Hello World, 202, 1970-01-01T00:03:22, 20, 2, 2, 40]"
    };

    static final TableTestProgram OVER_AGGREGATE_TIME_BOUNDED_PARTITIONED_ROWS =
            getTableTestProgram(
                    "over-aggregate-bounded-partitioned-rows",
                    "validates over aggregate node with time range and partitioning",
                    "PARTITION BY c ORDER BY rowtime RANGE BETWEEN INTERVAL '10' SECOND PRECEDING AND CURRENT ROW",
                    BEFORE_RESTORE_DATA,
                    AFTER_RESTORE_OUTPUT);

    static final TableTestProgram OVER_AGGREGATE_TIME_BOUNDED_NON_PARTITIONED_ROWS =
            getTableTestProgram(
                    "over-aggregate-bounded-non-partitioned-rows",
                    "validates over aggregate node with time range and no partitioning",
                    "ORDER BY rowtime RANGE BETWEEN INTERVAL '10' SECOND PRECEDING AND CURRENT ROW",
                    BEFORE_RESTORE_DATA,
                    AFTER_RESTORE_OUTPUT);

    private static final String[] BEFORE_RESTORE_DATA_UNBOUNDED = {
        "+I[Hello, 10, 1970-01-01T00:00:10, 1, 0, 1, 1]",
        "+I[Hello, 15, 1970-01-01T00:00:15, 15, 0, 2, 2]",
        "+I[Hello, 16, 1970-01-01T00:00:16, 16, 0, 3, 3]",
        "+I[Hello, 20, 1970-01-01T00:00:20, 2, 0, 4, 5]",
        "+I[Hello, 30, 1970-01-01T00:00:30, 3, 0, 5, 8]",
        "+I[Hello, 40, 1970-01-01T00:00:40, 4, 0, 6, 12]",
        "+I[Hello, 50, 1970-01-01T00:00:50, 5, 1, 7, 17]",
        "+I[Hello, 60, 1970-01-01T00:01, 6, 2, 8, 23]",
        "+I[Hello, 65, 1970-01-01T00:01:05, 65, 3, 9, 29]",
        "+I[Hello, 90, 1970-01-01T00:01:30, 9, 4, 10, 35]",
        "+I[Hello, 95, 1970-01-01T00:01:35, 18, 5, 11, 41]",
        "+I[Hello World, 100, 1970-01-01T00:01:40, 7, 1, 1, 7]",
        "+I[Hello World, 110, 1970-01-01T00:01:50, 17, 2, 2, 14]",
        "+I[Hello World, 140, 1970-01-01T00:02:20, 18, 3, 3, 21]",
        "+I[Hello World, 150, 1970-01-01T00:02:30, 8, 4, 4, 29]",
        "+I[Hello World, 200, 1970-01-01T00:03:20, 20, 5, 5, 49]"
    };

    private static final String[] AFTER_RESTORE_DATA_UNBOUNDED = {
        "+I[Hello World, 150, 1970-01-01T00:02:30, 8, 6, 6, 57]",
        "+I[Hello World, 151, 1970-01-01T00:02:31, 8, 7, 7, 65]",
        "+I[Hello World, 202, 1970-01-01T00:03:22, 20, 8, 8, 85]"
    };

    static final TableTestProgram OVER_AGGREGATE_UNBOUNDED_PARTITIONED_ROWS =
            getTableTestProgram(
                    "over-aggregate-unbounded-partitioned-rows",
                    "validates over aggregate node with no bounds and partitioning",
                    "PARTITION BY c ORDER BY rowtime RANGE UNBOUNDED PRECEDING",
                    BEFORE_RESTORE_DATA_UNBOUNDED,
                    AFTER_RESTORE_DATA_UNBOUNDED);

    private static final String[] BEFORE_RESTORE_DATA_PRECEDING_ROWS = {
        "+I[Hello, 10, 1970-01-01T00:00:10, 1, 0, 1, 1]",
        "+I[Hello, 15, 1970-01-01T00:00:15, 15, 0, 2, 2]",
        "+I[Hello, 16, 1970-01-01T00:00:16, 16, 0, 3, 3]",
        "+I[Hello, 20, 1970-01-01T00:00:20, 2, 0, 4, 5]",
        "+I[Hello, 30, 1970-01-01T00:00:30, 3, 0, 5, 8]",
        "+I[Hello, 40, 1970-01-01T00:00:40, 4, 0, 6, 12]",
        "+I[Hello, 50, 1970-01-01T00:00:50, 5, 1, 6, 16]",
        "+I[Hello, 60, 1970-01-01T00:01, 6, 2, 6, 21]",
        "+I[Hello, 65, 1970-01-01T00:01:05, 65, 3, 6, 26]",
        "+I[Hello, 90, 1970-01-01T00:01:30, 9, 4, 6, 30]",
        "+I[Hello, 95, 1970-01-01T00:01:35, 18, 5, 6, 33]",
        "+I[Hello World, 100, 1970-01-01T00:01:40, 7, 1, 1, 7]",
        "+I[Hello, 99, 1970-01-01T00:01:39, 15, 6, 6, 48]",
        "+I[Hello World, 110, 1970-01-01T00:01:50, 17, 2, 2, 14]",
        "+I[Hello World, 140, 1970-01-01T00:02:20, 18, 3, 3, 21]",
        "+I[Hello World, 150, 1970-01-01T00:02:30, 8, 4, 4, 29]",
        "+I[Hello World, 200, 1970-01-01T00:03:20, 20, 5, 5, 49]"
    };

    private static final String[] AFTER_RESTORE_DATA_PRECEDING_ROWS = {
        "+I[Hello World, 202, 1970-01-01T00:03:22, 20, 6, 6, 69]"
    };

    static final TableTestProgram OVER_AGGREGATE_ROW_BOUNDED_PARTITIONED_PRECEDING_ROWS =
            getTableTestProgram(
                    "over-aggregate-bounded-partitioned-preceding-rows",
                    "validates over aggregate node partitioned and bounded by prior rows",
                    "PARTITION BY c ORDER BY rowtime ROWS BETWEEN 5 preceding AND CURRENT ROW",
                    BEFORE_RESTORE_DATA_PRECEDING_ROWS,
                    AFTER_RESTORE_DATA_PRECEDING_ROWS);

    private static TableTestProgram getTableTestProgram(
            final String id,
            final String description,
            final String windowSql,
            final String[] beforeRows,
            final String[] afterRows) {
        final String sql =
                String.format(
                        "insert into MySink SELECT "
                                + "  c, ts, rowtime, b,"
                                + "  LTCNT(a, CAST('4' AS BIGINT)) OVER w,"
                                + "  COUNT(a) OVER w,"
                                + "  SUM(a) OVER w"
                                + " FROM MyTable"
                                + " WINDOW w as (%s)",
                        windowSql);

        return TableTestProgram.of(id, description)
                .setupConfig(LOCAL_TIME_ZONE, "UTC")
                .setupTemporarySystemFunction(
                        "LTCNT", JavaUserDefinedAggFunctions.LargerThanCount.class)
                .setupTableSource(SOURCE)
                .setupTableSink(
                        SinkTestStep.newBuilder("MySink")
                                .addSchema(
                                        "a string",
                                        "ts bigint",
                                        "rowtime TIMESTAMP(3)",
                                        "b int",
                                        "c bigint",
                                        "d bigint",
                                        "e bigint")
                                .consumedBeforeRestore(beforeRows)
                                .consumedAfterRestore(afterRows)
                                .build())
                .runSql(sql)
                .build();
    }
}
