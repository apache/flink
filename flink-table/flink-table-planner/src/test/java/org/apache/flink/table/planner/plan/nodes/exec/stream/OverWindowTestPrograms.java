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
import org.apache.flink.types.RowKind;

import java.util.Collections;

/** Programs for verifying {@link StreamExecOverAggregate}. */
public class OverWindowTestPrograms {
    static final TableTestProgram LAG_OVER_FUNCTION =
            TableTestProgram.of("over-aggregate-lag", "validates restoring a lag function")
                    .setupTableSource(
                            SourceTestStep.newBuilder("t")
                                    .addSchema(
                                            "ts STRING",
                                            "b MAP<DOUBLE, DOUBLE>",
                                            "`r_time` AS TO_TIMESTAMP(`ts`)",
                                            "WATERMARK for `r_time` AS `r_time`")
                                    .producedBeforeRestore(
                                            Row.of(
                                                    "2020-04-15 08:00:05",
                                                    Collections.singletonMap(42.0, 42.0)))
                                    .producedAfterRestore(
                                            Row.of(
                                                    "2020-04-15 08:00:06",
                                                    Collections.singletonMap(42.1, 42.1)))
                                    .build())
                    .setupTableSink(
                            SinkTestStep.newBuilder("sink_t")
                                    .addSchema("ts STRING", "b MAP<DOUBLE, DOUBLE>")
                                    .consumedBeforeRestore(Row.of("2020-04-15 08:00:05", null))
                                    .consumedAfterRestore(
                                            Row.of(
                                                    "2020-04-15 08:00:06",
                                                    Collections.singletonMap(42.0, 42.0)))
                                    .build())
                    .runSql(
                            "INSERT INTO sink_t SELECT ts, LAG(b, 1) over (order by r_time) AS "
                                    + "bLag FROM t")
                    .build();

    static final TableTestProgram OVER_AGGREGATE_NON_TIME_UNBOUNDED_RETRACT_MODE =
            TableTestProgram.of(
                            "over-aggregate-sum-retract-mode",
                            "validates restoring an unbounded preceding sum function in retract mode")
                    .setupTableSource(
                            SourceTestStep.newBuilder("source_t")
                                    .addSchema("key STRING", "val BIGINT", "ts BIGINT")
                                    .addOption("changelog-mode", "I,UB,UA")
                                    .producedBeforeRestore(
                                            Row.of("key1", 1L, 100L),
                                            Row.of("key1", 2L, 200L),
                                            Row.of("key1", 5L, 500L),
                                            Row.of("key1", 6L, 600L),
                                            Row.ofKind(RowKind.UPDATE_BEFORE, "key1", 2L, 200L),
                                            Row.ofKind(RowKind.UPDATE_AFTER, "key1", 3L, 200L),
                                            Row.of("key2", 1L, 100L),
                                            Row.of("key2", 2L, 200L))
                                    .producedAfterRestore(
                                            Row.of("key3", 1L, 100L),
                                            Row.of("key1", 4L, 400L),
                                            Row.ofKind(RowKind.UPDATE_BEFORE, "key1", 3L, 200L),
                                            Row.ofKind(RowKind.UPDATE_AFTER, "key1", 3L, 300L))
                                    .build())
                    .setupTableSink(
                            SinkTestStep.newBuilder("sink_t")
                                    .addSchema(
                                            "key STRING",
                                            "val BIGINT",
                                            "ts BIGINT",
                                            "sum_val BIGINT")
                                    .consumedBeforeRestore(
                                            Row.of("key1", 1L, 100L, 1L),
                                            Row.of("key1", 2L, 200L, 3L),
                                            Row.of("key1", 5L, 500L, 8L),
                                            Row.of("key1", 6L, 600L, 14L),
                                            Row.ofKind(RowKind.UPDATE_BEFORE, "key1", 2L, 200L, 3L),
                                            Row.ofKind(RowKind.UPDATE_BEFORE, "key1", 5L, 500L, 8L),
                                            Row.ofKind(RowKind.UPDATE_AFTER, "key1", 5L, 500L, 6L),
                                            Row.ofKind(
                                                    RowKind.UPDATE_BEFORE, "key1", 6L, 600L, 14L),
                                            Row.ofKind(RowKind.UPDATE_AFTER, "key1", 6L, 600L, 12L),
                                            Row.ofKind(RowKind.UPDATE_AFTER, "key1", 3L, 200L, 4L),
                                            Row.ofKind(RowKind.UPDATE_BEFORE, "key1", 5L, 500L, 6L),
                                            Row.ofKind(RowKind.UPDATE_AFTER, "key1", 5L, 500L, 9L),
                                            Row.ofKind(
                                                    RowKind.UPDATE_BEFORE, "key1", 6L, 600L, 12L),
                                            Row.ofKind(RowKind.UPDATE_AFTER, "key1", 6L, 600L, 15L),
                                            Row.of("key2", 1L, 100L, 1L),
                                            Row.of("key2", 2L, 200L, 3L))
                                    .consumedAfterRestore(
                                            Row.of("key3", 1L, 100L, 1L),
                                            Row.of("key1", 4L, 400L, 8L),
                                            Row.ofKind(RowKind.UPDATE_BEFORE, "key1", 5L, 500L, 9L),
                                            Row.ofKind(RowKind.UPDATE_AFTER, "key1", 5L, 500L, 13L),
                                            Row.ofKind(
                                                    RowKind.UPDATE_BEFORE, "key1", 6L, 600L, 15L),
                                            Row.ofKind(RowKind.UPDATE_AFTER, "key1", 6L, 600L, 19L),
                                            Row.ofKind(RowKind.UPDATE_BEFORE, "key1", 3L, 200L, 4L),
                                            Row.ofKind(RowKind.UPDATE_BEFORE, "key1", 4L, 400L, 8L),
                                            Row.ofKind(RowKind.UPDATE_AFTER, "key1", 4L, 400L, 5L),
                                            Row.ofKind(
                                                    RowKind.UPDATE_BEFORE, "key1", 5L, 500L, 13L),
                                            Row.ofKind(RowKind.UPDATE_AFTER, "key1", 5L, 500L, 10L),
                                            Row.ofKind(
                                                    RowKind.UPDATE_BEFORE, "key1", 6L, 600L, 19L),
                                            Row.ofKind(RowKind.UPDATE_AFTER, "key1", 6L, 600L, 16L),
                                            Row.ofKind(RowKind.UPDATE_AFTER, "key1", 3L, 300L, 4L),
                                            Row.ofKind(RowKind.UPDATE_BEFORE, "key1", 4L, 400L, 5L),
                                            Row.ofKind(RowKind.UPDATE_AFTER, "key1", 4L, 400L, 8L),
                                            Row.ofKind(
                                                    RowKind.UPDATE_BEFORE, "key1", 5L, 500L, 10L),
                                            Row.ofKind(RowKind.UPDATE_AFTER, "key1", 5L, 500L, 13L),
                                            Row.ofKind(
                                                    RowKind.UPDATE_BEFORE, "key1", 6L, 600L, 16L),
                                            Row.ofKind(RowKind.UPDATE_AFTER, "key1", 6L, 600L, 19L))
                                    .build())
                    .runSql(
                            "INSERT INTO sink_t SELECT key, val, ts, SUM(val) OVER ("
                                    + "PARTITION BY key "
                                    + "ORDER BY val "
                                    + "RANGE BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) "
                                    + "AS sum_val "
                                    + "FROM source_t")
                    .build();

    static final TableTestProgram OVER_AGGREGATE_NON_TIME_UNBOUNDED_APPEND_MODE =
            TableTestProgram.of(
                            "over-aggregate-sum-append-mode",
                            "validates restoring an unbounded preceding sum function in append mode")
                    .setupTableSource(
                            SourceTestStep.newBuilder("source_t")
                                    .addSchema("key STRING", "val BIGINT", "ts BIGINT")
                                    .addOption("changelog-mode", "I")
                                    .producedBeforeRestore(
                                            Row.of("key1", 1L, 100L),
                                            Row.of("key1", 2L, 200L),
                                            Row.of("key1", 5L, 500L),
                                            Row.of("key1", 6L, 600L),
                                            Row.of("key2", 1L, 100L),
                                            Row.of("key2", 2L, 200L))
                                    .producedAfterRestore(Row.of("key1", 4L, 400L))
                                    .build())
                    .setupTableSink(
                            SinkTestStep.newBuilder("sink_t")
                                    .addSchema(
                                            "key STRING",
                                            "val BIGINT",
                                            "ts BIGINT",
                                            "sum_val BIGINT")
                                    .consumedBeforeRestore(
                                            Row.of("key1", 1L, 100L, 1L),
                                            Row.of("key1", 2L, 200L, 3L),
                                            Row.of("key1", 5L, 500L, 8L),
                                            Row.of("key1", 6L, 600L, 14L),
                                            Row.of("key2", 1L, 100L, 1L),
                                            Row.of("key2", 2L, 200L, 3L))
                                    .consumedAfterRestore(
                                            Row.of("key1", 4L, 400L, 7L),
                                            Row.ofKind(RowKind.UPDATE_BEFORE, "key1", 5L, 500L, 8L),
                                            Row.ofKind(RowKind.UPDATE_AFTER, "key1", 5L, 500L, 12L),
                                            Row.ofKind(
                                                    RowKind.UPDATE_BEFORE, "key1", 6L, 600L, 14L),
                                            Row.ofKind(RowKind.UPDATE_AFTER, "key1", 6L, 600L, 18L))
                                    .build())
                    .runSql(
                            "INSERT INTO sink_t SELECT key, val, ts, SUM(val) OVER ("
                                    + "PARTITION BY key "
                                    + "ORDER BY val "
                                    + "RANGE BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) "
                                    + "AS sum_val "
                                    + "FROM source_t")
                    .build();

    static final TableTestProgram OVER_AGGREGATE_NON_TIME_UNBOUNDED_APPEND_MODE_MULTIPLE_AGGS =
            TableTestProgram.of(
                            "over-aggregate-sum-append-mode-multiple-aggs",
                            "validates restoring an unbounded preceding sum function in append mode")
                    .setupTableSource(
                            SourceTestStep.newBuilder("source_t")
                                    .addSchema("key STRING", "val BIGINT", "ts BIGINT")
                                    .addOption("changelog-mode", "I")
                                    .producedBeforeRestore(
                                            Row.of("key1", 1L, 100L),
                                            Row.of("key1", 2L, 200L),
                                            Row.of("key1", 5L, 500L),
                                            Row.of("key1", 6L, 600L),
                                            Row.of("key2", 1L, 100L),
                                            Row.of("key2", 2L, 200L))
                                    .producedAfterRestore(Row.of("key1", 4L, 400L))
                                    .build())
                    .setupTableSink(
                            SinkTestStep.newBuilder("sink_t")
                                    .addSchema(
                                            "key STRING",
                                            "val BIGINT",
                                            "ts BIGINT",
                                            "sum_val BIGINT",
                                            "cnt_key BIGINT")
                                    .consumedBeforeRestore(
                                            Row.of("key1", 1L, 100L, 1L, 1L),
                                            Row.of("key1", 2L, 200L, 3L, 2L),
                                            Row.of("key1", 5L, 500L, 8L, 3L),
                                            Row.of("key1", 6L, 600L, 14L, 4L),
                                            Row.of("key2", 1L, 100L, 1L, 1L),
                                            Row.of("key2", 2L, 200L, 3L, 2L))
                                    .consumedAfterRestore(
                                            Row.of("key1", 4L, 400L, 7L, 3L),
                                            Row.ofKind(
                                                    RowKind.UPDATE_BEFORE,
                                                    "key1",
                                                    5L,
                                                    500L,
                                                    8L,
                                                    3L),
                                            Row.ofKind(
                                                    RowKind.UPDATE_AFTER,
                                                    "key1",
                                                    5L,
                                                    500L,
                                                    12L,
                                                    4L),
                                            Row.ofKind(
                                                    RowKind.UPDATE_BEFORE,
                                                    "key1",
                                                    6L,
                                                    600L,
                                                    14L,
                                                    4L),
                                            Row.ofKind(
                                                    RowKind.UPDATE_AFTER,
                                                    "key1",
                                                    6L,
                                                    600L,
                                                    18L,
                                                    5L))
                                    .build())
                    .runSql(
                            "INSERT INTO sink_t SELECT key, val, ts, "
                                    + "SUM(val) OVER ("
                                    + "PARTITION BY key "
                                    + "ORDER BY val "
                                    + "RANGE BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) "
                                    + "AS sum_val, "
                                    + "COUNT(key) OVER ("
                                    + "PARTITION BY key "
                                    + "ORDER BY val "
                                    + "RANGE BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) "
                                    + "AS cnt_key "
                                    + "FROM source_t")
                    .build();

    static final TableTestProgram OVER_AGGREGATE_NON_TIME_UNBOUNDED_NO_PARTITION_BY =
            TableTestProgram.of(
                            "over-aggregate-sum-no-partition-by",
                            "validates restoring an unbounded preceding sum function without partition by")
                    .setupTableSource(
                            SourceTestStep.newBuilder("source_t")
                                    .addSchema("key STRING", "val BIGINT", "ts BIGINT")
                                    .addOption("changelog-mode", "I")
                                    .producedBeforeRestore(
                                            Row.of("key1", 1L, 100L),
                                            Row.of("key1", 2L, 200L),
                                            Row.of("key1", 5L, 500L),
                                            Row.of("key1", 6L, 600L),
                                            Row.of("key2", 1L, 100L),
                                            Row.of("key2", 2L, 200L))
                                    .producedAfterRestore(Row.of("key1", 4L, 400L))
                                    .build())
                    .setupTableSink(
                            SinkTestStep.newBuilder("sink_t")
                                    .addSchema(
                                            "key STRING",
                                            "val BIGINT",
                                            "ts BIGINT",
                                            "sum_val BIGINT")
                                    .consumedBeforeRestore(
                                            Row.of("key1", 1L, 100L, 1L),
                                            Row.of("key1", 2L, 200L, 3L),
                                            Row.of("key1", 5L, 500L, 8L),
                                            Row.of("key1", 6L, 600L, 14L),
                                            Row.of("key2", 1L, 100L, 2L),
                                            Row.ofKind(RowKind.UPDATE_BEFORE, "key1", 2L, 200L, 3L),
                                            Row.ofKind(RowKind.UPDATE_AFTER, "key1", 2L, 200L, 4L),
                                            Row.ofKind(RowKind.UPDATE_BEFORE, "key1", 5L, 500L, 8L),
                                            Row.ofKind(RowKind.UPDATE_AFTER, "key1", 5L, 500L, 9L),
                                            Row.ofKind(
                                                    RowKind.UPDATE_BEFORE, "key1", 6L, 600L, 14L),
                                            Row.ofKind(RowKind.UPDATE_AFTER, "key1", 6L, 600L, 15L),
                                            Row.of("key2", 2L, 200L, 6L),
                                            Row.ofKind(RowKind.UPDATE_BEFORE, "key1", 5L, 500L, 9L),
                                            Row.ofKind(RowKind.UPDATE_AFTER, "key1", 5L, 500L, 11L),
                                            Row.ofKind(
                                                    RowKind.UPDATE_BEFORE, "key1", 6L, 600L, 15L),
                                            Row.ofKind(RowKind.UPDATE_AFTER, "key1", 6L, 600L, 17L))
                                    .consumedAfterRestore(
                                            Row.of("key1", 4L, 400L, 10L),
                                            Row.ofKind(
                                                    RowKind.UPDATE_BEFORE, "key1", 5L, 500L, 11L),
                                            Row.ofKind(RowKind.UPDATE_AFTER, "key1", 5L, 500L, 15L),
                                            Row.ofKind(
                                                    RowKind.UPDATE_BEFORE, "key1", 6L, 600L, 17L),
                                            Row.ofKind(RowKind.UPDATE_AFTER, "key1", 6L, 600L, 21L))
                                    .build())
                    .runSql(
                            "INSERT INTO sink_t SELECT key, val, ts, SUM(val) OVER ("
                                    + "ORDER BY val "
                                    + "RANGE BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) "
                                    + "AS sum_val "
                                    + "FROM source_t")
                    .build();
}
