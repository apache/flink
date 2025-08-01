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

    static SourceTestStep getSource(final String[] schema) {
        return SourceTestStep.newBuilder("source_t")
                .addSchema(schema)
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
                .build();
    }

    static final SinkTestStep getSink(String[] schema) {
        return SinkTestStep.newBuilder("sink_t")
                .addSchema(schema)
                .consumedBeforeRestore(
                        Row.of("key1", 1L, 100L, 1L),
                        Row.of("key1", 2L, 200L, 3L),
                        Row.of("key1", 5L, 500L, 8L),
                        Row.of("key1", 6L, 600L, 14L),
                        Row.ofKind(RowKind.DELETE, "key1", 2L, 200L, 3L),
                        Row.ofKind(RowKind.UPDATE_BEFORE, "key1", 5L, 500L, 8L),
                        Row.ofKind(RowKind.UPDATE_AFTER, "key1", 5L, 500L, 6L),
                        Row.ofKind(RowKind.UPDATE_BEFORE, "key1", 6L, 600L, 14L),
                        Row.ofKind(RowKind.UPDATE_AFTER, "key1", 6L, 600L, 12L),
                        Row.ofKind(RowKind.UPDATE_AFTER, "key1", 3L, 200L, 4L),
                        Row.ofKind(RowKind.UPDATE_BEFORE, "key1", 5L, 500L, 6L),
                        Row.ofKind(RowKind.UPDATE_AFTER, "key1", 5L, 500L, 9L),
                        Row.ofKind(RowKind.UPDATE_BEFORE, "key1", 6L, 600L, 12L),
                        Row.ofKind(RowKind.UPDATE_AFTER, "key1", 6L, 600L, 15L),
                        Row.of("key2", 1L, 100L, 1L),
                        Row.of("key2", 2L, 200L, 3L))
                .consumedAfterRestore(
                        Row.of("key3", 1L, 100L, 1L),
                        Row.of("key1", 4L, 400L, 8L),
                        Row.ofKind(RowKind.UPDATE_BEFORE, "key1", 5L, 500L, 9L),
                        Row.ofKind(RowKind.UPDATE_AFTER, "key1", 5L, 500L, 13L),
                        Row.ofKind(RowKind.UPDATE_BEFORE, "key1", 6L, 600L, 15L),
                        Row.ofKind(RowKind.UPDATE_AFTER, "key1", 6L, 600L, 19L),
                        Row.ofKind(RowKind.DELETE, "key1", 3L, 200L, 4L),
                        Row.ofKind(RowKind.UPDATE_BEFORE, "key1", 4L, 400L, 8L),
                        Row.ofKind(RowKind.UPDATE_AFTER, "key1", 4L, 400L, 5L),
                        Row.ofKind(RowKind.UPDATE_BEFORE, "key1", 5L, 500L, 13L),
                        Row.ofKind(RowKind.UPDATE_AFTER, "key1", 5L, 500L, 10L),
                        Row.ofKind(RowKind.UPDATE_BEFORE, "key1", 6L, 600L, 19L),
                        Row.ofKind(RowKind.UPDATE_AFTER, "key1", 6L, 600L, 16L),
                        Row.ofKind(RowKind.UPDATE_AFTER, "key1", 3L, 300L, 4L),
                        Row.ofKind(RowKind.UPDATE_BEFORE, "key1", 4L, 400L, 5L),
                        Row.ofKind(RowKind.UPDATE_AFTER, "key1", 4L, 400L, 8L),
                        Row.ofKind(RowKind.UPDATE_BEFORE, "key1", 5L, 500L, 10L),
                        Row.ofKind(RowKind.UPDATE_AFTER, "key1", 5L, 500L, 13L),
                        Row.ofKind(RowKind.UPDATE_BEFORE, "key1", 6L, 600L, 16L),
                        Row.ofKind(RowKind.UPDATE_AFTER, "key1", 6L, 600L, 19L))
                .build();
    }

    static SinkTestStep getSink(String[] schema, Row[] beforeData, Row[] afterData) {
        return SinkTestStep.newBuilder("sink_t")
                .addSchema(schema)
                .consumedBeforeRestore(beforeData)
                .consumedAfterRestore(afterData)
                .build();
    }

    static final TableTestProgram OVER_AGGREGATE_NON_TIME_RANGE_UNBOUNDED_SUM_RETRACT_MODE =
            TableTestProgram.of(
                            "over-aggregate-non-time-range-unbounded-sum-retract-mode",
                            "validates restoring a non-time unbounded preceding sum function in retract mode")
                    .setupTableSource(
                            getSource(new String[] {"key STRING", "val BIGINT", "ts BIGINT"}))
                    .setupTableSink(
                            getSink(
                                    new String[] {
                                        "key STRING", "val BIGINT", "ts BIGINT", "sum_val BIGINT"
                                    }))
                    .runSql(
                            "INSERT INTO sink_t SELECT key, val, ts, SUM(val) OVER ("
                                    + "PARTITION BY key "
                                    + "ORDER BY val "
                                    + "RANGE BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) "
                                    + "AS sum_val "
                                    + "FROM source_t")
                    .build();

    static final TableTestProgram
            OVER_AGGREGATE_NON_TIME_RANGE_UNBOUNDED_SUM_RETRACT_MODE_SORT_BY_KEY =
                    TableTestProgram.of(
                                    "over-aggregate-non-time-range-unbounded-sum-retract-mode-sort-by-key",
                                    "validates restoring a non-time unbounded preceding sum function in retract mode")
                            .setupTableSource(
                                    getSource(
                                            new String[] {"key STRING", "val BIGINT", "ts BIGINT"}))
                            .setupTableSink(
                                    SinkTestStep.newBuilder("sink_t")
                                            .addSchema(
                                                    "key STRING",
                                                    "val BIGINT",
                                                    "ts BIGINT",
                                                    "sum_val BIGINT")
                                            .consumedBeforeRestore(
                                                    Row.of("key1", 1L, 100L, 1L),
                                                    Row.of("key1", 2L, 200L, 2L),
                                                    Row.of("key1", 5L, 500L, 5L),
                                                    Row.of("key1", 6L, 600L, 6L),
                                                    Row.ofKind(
                                                            RowKind.DELETE, "key1", 2L, 200L, 2L),
                                                    Row.ofKind(
                                                            RowKind.UPDATE_AFTER,
                                                            "key1",
                                                            3L,
                                                            200L,
                                                            3L),
                                                    Row.of("key2", 1L, 100L, 2L),
                                                    Row.of("key2", 2L, 200L, 2L))
                                            .consumedAfterRestore(
                                                    Row.of("key3", 1L, 100L, 3L),
                                                    Row.of("key1", 4L, 400L, 4L),
                                                    Row.ofKind(
                                                            RowKind.DELETE, "key1", 3L, 200L, 3L),
                                                    Row.ofKind(
                                                            RowKind.UPDATE_AFTER,
                                                            "key1",
                                                            3L,
                                                            300L,
                                                            3L))
                                            .build())
                            .runSql(
                                    "INSERT INTO sink_t SELECT key, val, ts, SUM(val) OVER ("
                                            + "PARTITION BY val "
                                            + "ORDER BY key "
                                            + "RANGE BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) "
                                            + "AS sum_val "
                                            + "FROM source_t")
                            .build();

    static final TableTestProgram OVER_AGGREGATE_NON_TIME_ROWS_UNBOUNDED_SUM_RETRACT_MODE =
            TableTestProgram.of(
                            "over-aggregate-non-time-rows-unbounded-sum-retract-mode",
                            "validates restoring a non-time unbounded preceding sum function in retract mode")
                    .setupTableSource(
                            getSource(new String[] {"key STRING", "val BIGINT", "ts BIGINT"}))
                    .setupTableSink(
                            getSink(
                                    new String[] {
                                        "key STRING", "val BIGINT", "ts BIGINT", "sum_val BIGINT"
                                    }))
                    .runSql(
                            "INSERT INTO sink_t SELECT key, val, ts, SUM(val) OVER ("
                                    + "PARTITION BY key "
                                    + "ORDER BY val "
                                    + "ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) "
                                    + "AS sum_val "
                                    + "FROM source_t")
                    .build();

    static final TableTestProgram
            OVER_AGGREGATE_NON_TIME_ROWS_UNBOUNDED_SUM_RETRACT_MODE_SORT_BY_KEY =
                    TableTestProgram.of(
                                    "over-aggregate-non-time-rows-unbounded-sum-retract-mode-sort-by-key",
                                    "validates restoring a non-time unbounded preceding sum function in retract mode")
                            .setupTableSource(
                                    getSource(
                                            new String[] {"key STRING", "val BIGINT", "ts BIGINT"}))
                            .setupTableSink(
                                    SinkTestStep.newBuilder("sink_t")
                                            .addSchema(
                                                    "key STRING",
                                                    "val BIGINT",
                                                    "ts BIGINT",
                                                    "sum_val BIGINT")
                                            .consumedBeforeRestore(
                                                    Row.of("key1", 1L, 100L, 1L),
                                                    Row.of("key1", 2L, 200L, 2L),
                                                    Row.of("key1", 5L, 500L, 5L),
                                                    Row.of("key1", 6L, 600L, 6L),
                                                    Row.ofKind(
                                                            RowKind.DELETE, "key1", 2L, 200L, 2L),
                                                    Row.ofKind(
                                                            RowKind.UPDATE_AFTER,
                                                            "key1",
                                                            3L,
                                                            200L,
                                                            3L),
                                                    Row.of("key2", 1L, 100L, 2L),
                                                    Row.of("key2", 2L, 200L, 2L))
                                            .consumedAfterRestore(
                                                    Row.of("key3", 1L, 100L, 3L),
                                                    Row.of("key1", 4L, 400L, 4L),
                                                    Row.ofKind(
                                                            RowKind.DELETE, "key1", 3L, 200L, 3L),
                                                    Row.ofKind(
                                                            RowKind.UPDATE_AFTER,
                                                            "key1",
                                                            3L,
                                                            300L,
                                                            3L))
                                            .build())
                            .runSql(
                                    "INSERT INTO sink_t SELECT key, val, ts, SUM(val) OVER ("
                                            + "PARTITION BY val "
                                            + "ORDER BY key "
                                            + "ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) "
                                            + "AS sum_val "
                                            + "FROM source_t")
                            .build();

    static final TableTestProgram
            OVER_AGGREGATE_NON_TIME_RANGE_UNBOUNDED_SUM_RETRACT_MODE_SOURCE_PRIMARY_KEY =
                    TableTestProgram.of(
                                    "over-aggregate-non-time-range-unbounded-sum-retract-mode-source-primary-key",
                                    "validates restoring a non-time unbounded preceding sum function in retract mode with source table having primary key")
                            .setupTableSource(
                                    getSource(
                                            new String[] {
                                                "key STRING",
                                                "val BIGINT",
                                                "ts BIGINT",
                                                "PRIMARY KEY(key) NOT ENFORCED"
                                            }))
                            .setupTableSink(
                                    getSink(
                                            new String[] {
                                                "key STRING",
                                                "val BIGINT",
                                                "ts BIGINT",
                                                "sum_val BIGINT"
                                            }))
                            .runSql(
                                    "INSERT INTO sink_t SELECT key, val, ts, SUM(val) OVER ("
                                            + "PARTITION BY key "
                                            + "ORDER BY val "
                                            + "RANGE BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) "
                                            + "AS sum_val "
                                            + "FROM source_t")
                            .build();

    static final TableTestProgram
            OVER_AGGREGATE_NON_TIME_ROWS_UNBOUNDED_SUM_RETRACT_MODE_SOURCE_PRIMARY_KEY =
                    TableTestProgram.of(
                                    "over-aggregate-non-time-rows-unbounded-sum-retract-mode-source-primary-key",
                                    "validates restoring a non-time unbounded preceding sum function in retract mode with source table having primary key")
                            .setupTableSource(
                                    getSource(
                                            new String[] {
                                                "key STRING",
                                                "val BIGINT",
                                                "ts BIGINT",
                                                "PRIMARY KEY(key) NOT ENFORCED"
                                            }))
                            .setupTableSink(
                                    getSink(
                                            new String[] {
                                                "key STRING",
                                                "val BIGINT",
                                                "ts BIGINT",
                                                "sum_val BIGINT"
                                            }))
                            .runSql(
                                    "INSERT INTO sink_t SELECT key, val, ts, SUM(val) OVER ("
                                            + "PARTITION BY key "
                                            + "ORDER BY val "
                                            + "ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) "
                                            + "AS sum_val "
                                            + "FROM source_t")
                            .build();

    static final Row[] SINK_PRIMARY_KEY_BEFORE_DATA =
            new Row[] {
                Row.of("key1", 1L, 100L, 1L),
                Row.ofKind(RowKind.UPDATE_AFTER, "key1", 2L, 200L, 3L),
                Row.ofKind(RowKind.UPDATE_AFTER, "key1", 5L, 500L, 8L),
                Row.ofKind(RowKind.UPDATE_AFTER, "key1", 6L, 600L, 14L),
                Row.ofKind(RowKind.UPDATE_AFTER, "key1", 5L, 500L, 6L),
                Row.ofKind(RowKind.UPDATE_AFTER, "key1", 6L, 600L, 12L),
                Row.ofKind(RowKind.UPDATE_AFTER, "key1", 3L, 200L, 4L),
                Row.ofKind(RowKind.UPDATE_AFTER, "key1", 5L, 500L, 9L),
                Row.ofKind(RowKind.UPDATE_AFTER, "key1", 6L, 600L, 15L),
                Row.of("key2", 1L, 100L, 1L),
                Row.ofKind(RowKind.UPDATE_AFTER, "key2", 2L, 200L, 3L)
            };

    static final Row[] SINK_PRIMARY_KEY_AFTER_DATA =
            new Row[] {
                Row.of("key3", 1L, 100L, 1L),
                Row.ofKind(RowKind.UPDATE_AFTER, "key1", 4L, 400L, 8L),
                Row.ofKind(RowKind.UPDATE_AFTER, "key1", 5L, 500L, 13L),
                Row.ofKind(RowKind.UPDATE_AFTER, "key1", 6L, 600L, 19L),
                Row.ofKind(RowKind.UPDATE_AFTER, "key1", 4L, 400L, 5L),
                Row.ofKind(RowKind.UPDATE_AFTER, "key1", 5L, 500L, 10L),
                Row.ofKind(RowKind.UPDATE_AFTER, "key1", 6L, 600L, 16L),
                Row.ofKind(RowKind.UPDATE_AFTER, "key1", 3L, 300L, 4L),
                Row.ofKind(RowKind.UPDATE_AFTER, "key1", 4L, 400L, 8L),
                Row.ofKind(RowKind.UPDATE_AFTER, "key1", 5L, 500L, 13L),
                Row.ofKind(RowKind.UPDATE_AFTER, "key1", 6L, 600L, 19L)
            };

    static final TableTestProgram
            OVER_AGGREGATE_NON_TIME_RANGE_UNBOUNDED_SUM_RETRACT_MODE_SINK_PRIMARY_KEY =
                    TableTestProgram.of(
                                    "over-aggregate-non-time-range-unbounded-sum-retract-mode-sink-primary-key",
                                    "validates restoring a non-time unbounded preceding sum function in retract mode with sink table having primary key")
                            .setupTableSource(
                                    getSource(
                                            new String[] {"key STRING", "val BIGINT", "ts BIGINT"}))
                            .setupTableSink(
                                    getSink(
                                            new String[] {
                                                "key STRING",
                                                "val BIGINT",
                                                "ts BIGINT",
                                                "sum_val BIGINT",
                                                "PRIMARY KEY(key) NOT ENFORCED"
                                            },
                                            SINK_PRIMARY_KEY_BEFORE_DATA,
                                            SINK_PRIMARY_KEY_AFTER_DATA))
                            .runSql(
                                    "INSERT INTO sink_t SELECT key, val, ts, SUM(val) OVER ("
                                            + "PARTITION BY key "
                                            + "ORDER BY val "
                                            + "RANGE BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) "
                                            + "AS sum_val "
                                            + "FROM source_t")
                            .build();

    static final TableTestProgram
            OVER_AGGREGATE_NON_TIME_ROWS_UNBOUNDED_SUM_RETRACT_MODE_SINK_PRIMARY_KEY =
                    TableTestProgram.of(
                                    "over-aggregate-non-time-rows-unbounded-sum-retract-mode-sink-primary-key",
                                    "validates restoring a non-time unbounded preceding sum function in retract mode with sink table having primary key")
                            .setupTableSource(
                                    getSource(
                                            new String[] {"key STRING", "val BIGINT", "ts BIGINT"}))
                            .setupTableSink(
                                    getSink(
                                            new String[] {
                                                "key STRING",
                                                "val BIGINT",
                                                "ts BIGINT",
                                                "sum_val BIGINT",
                                                "PRIMARY KEY(key) NOT ENFORCED"
                                            },
                                            SINK_PRIMARY_KEY_BEFORE_DATA,
                                            SINK_PRIMARY_KEY_AFTER_DATA))
                            .runSql(
                                    "INSERT INTO sink_t SELECT key, val, ts, SUM(val) OVER ("
                                            + "PARTITION BY key "
                                            + "ORDER BY val "
                                            + "ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) "
                                            + "AS sum_val "
                                            + "FROM source_t")
                            .build();

    static final Row[] SOURCE_SINK_PRIMARY_KEY_SINK_BEFORE_DATA =
            new Row[] {
                Row.of("key1", 1L, 100L, 1L),
                Row.ofKind(RowKind.DELETE, "key1", 1L, 100L, 1L),
                Row.ofKind(RowKind.UPDATE_AFTER, "key1", 2L, 200L, 2L),
                Row.ofKind(RowKind.DELETE, "key1", 2L, 200L, 2L),
                Row.ofKind(RowKind.UPDATE_AFTER, "key1", 5L, 500L, 5L),
                Row.ofKind(RowKind.DELETE, "key1", 5L, 500L, 5L),
                Row.ofKind(RowKind.UPDATE_AFTER, "key1", 6L, 600L, 6L),
                Row.ofKind(RowKind.DELETE, "key1", 6L, 600L, 6L),
                Row.ofKind(RowKind.UPDATE_AFTER, "key1", 3L, 200L, 3L),
                Row.of("key2", 1L, 100L, 1L),
                Row.ofKind(RowKind.DELETE, "key2", 1L, 100L, 1L),
                Row.ofKind(RowKind.UPDATE_AFTER, "key2", 2L, 200L, 2L)
            };

    static final Row[] SOURCE_SINK_PRIMARY_KEY_SINK_AFTER_DATA =
            new Row[] {
                Row.of("key3", 1L, 100L, 1L),
                Row.ofKind(RowKind.DELETE, "key1", 3L, 200L, 3L),
                Row.ofKind(RowKind.UPDATE_AFTER, "key1", 4L, 400L, 4L),
                Row.ofKind(RowKind.DELETE, "key1", 4L, 400L, 4L),
                Row.ofKind(RowKind.UPDATE_AFTER, "key1", 3L, 300L, 3L)
            };

    static final TableTestProgram
            OVER_AGGREGATE_NON_TIME_RANGE_UNBOUNDED_SUM_RETRACT_MODE_SOURCE_SINK_PRIMARY_KEY =
                    TableTestProgram.of(
                                    "over-aggregate-non-time-range-unbounded-sum-retract-mode-source-sink-primary-key",
                                    "validates restoring a non-time unbounded preceding sum function in retract mode with source and sink tables having primary key")
                            .setupConfig(
                                    // This option helps create a ChangelogNormalize node after the
                                    // source
                                    // which interprets duplicate input records correctly and
                                    // produces -U and +U correctly
                                    ExecutionConfigOptions.TABLE_EXEC_SOURCE_CDC_EVENTS_DUPLICATE,
                                    true)
                            .setupTableSource(
                                    // The following record is dropped due to
                                    // DropUpdateBefore and ChangelogNormalize
                                    // Row.ofKind(RowKind.UPDATE_BEFORE, "key1", 2L, 200L)
                                    getSource(
                                            new String[] {
                                                "key STRING",
                                                "val BIGINT",
                                                "ts BIGINT",
                                                "PRIMARY KEY(key) NOT ENFORCED"
                                            }))
                            .setupTableSink(
                                    SinkTestStep.newBuilder("sink_t")
                                            .addSchema(
                                                    "key STRING",
                                                    "val BIGINT",
                                                    "ts BIGINT",
                                                    "sum_val BIGINT",
                                                    "PRIMARY KEY(key) NOT ENFORCED")
                                            .consumedBeforeRestore(
                                                    SOURCE_SINK_PRIMARY_KEY_SINK_BEFORE_DATA)
                                            .consumedAfterRestore(
                                                    SOURCE_SINK_PRIMARY_KEY_SINK_AFTER_DATA)
                                            .build())
                            .runSql(
                                    "INSERT INTO sink_t SELECT key, val, ts, SUM(val) OVER ("
                                            + "PARTITION BY key "
                                            + "ORDER BY val "
                                            + "RANGE BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) "
                                            + "AS sum_val "
                                            + "FROM source_t")
                            .build();

    static final TableTestProgram
            OVER_AGGREGATE_NON_TIME_ROWS_UNBOUNDED_SUM_RETRACT_MODE_SOURCE_SINK_PRIMARY_KEY =
                    TableTestProgram.of(
                                    "over-aggregate-non-time-rows-unbounded-sum-retract-mode-source-sink-primary-key",
                                    "validates restoring a non-time unbounded preceding sum function in retract mode with source and sink tables having primary key")
                            .setupConfig(
                                    // This option helps create a ChangelogNormalize node after the
                                    // source
                                    // which interprets duplicate input records correctly and
                                    // produces -U and +U correctly
                                    ExecutionConfigOptions.TABLE_EXEC_SOURCE_CDC_EVENTS_DUPLICATE,
                                    true)
                            .setupTableSource(
                                    // The following record is dropped due to
                                    // DropUpdateBefore and ChangelogNormalize
                                    // Row.ofKind(RowKind.UPDATE_BEFORE, "key1", 2L, 200L)
                                    getSource(
                                            new String[] {
                                                "key STRING",
                                                "val BIGINT",
                                                "ts BIGINT",
                                                "PRIMARY KEY(key) NOT ENFORCED"
                                            }))
                            .setupTableSink(
                                    SinkTestStep.newBuilder("sink_t")
                                            .addSchema(
                                                    "key STRING",
                                                    "val BIGINT",
                                                    "ts BIGINT",
                                                    "sum_val BIGINT",
                                                    "PRIMARY KEY(key) NOT ENFORCED")
                                            .consumedBeforeRestore(
                                                    SOURCE_SINK_PRIMARY_KEY_SINK_BEFORE_DATA)
                                            .consumedAfterRestore(
                                                    SOURCE_SINK_PRIMARY_KEY_SINK_AFTER_DATA)
                                            .build())
                            .runSql(
                                    "INSERT INTO sink_t SELECT key, val, ts, SUM(val) OVER ("
                                            + "PARTITION BY key "
                                            + "ORDER BY val "
                                            + "ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) "
                                            + "AS sum_val "
                                            + "FROM source_t")
                            .build();

    static final Row[] PARTITION_BY_NON_PK_BEFORE_DATA =
            new Row[] {
                // This is a +I because the sinkMaterialize PK
                // is val
                Row.of("key1", 1L, 100L, 100L),
                // This is a +I because the sinkMaterialize PK
                // is val
                Row.of("key1", 2L, 200L, 300L),
                // This is a +I because the sinkMaterialize PK
                // is val
                Row.of("key1", 5L, 500L, 800L),
                // This is a +I because the sinkMaterialize PK
                // is val
                Row.of("key1", 6L, 600L, 1400L),
                Row.ofKind(RowKind.DELETE, "key1", 2L, 200L, 300L),
                Row.ofKind(RowKind.DELETE, "key1", 5L, 500L, 800L),
                Row.of("key1", 5L, 500L, 600L),
                Row.ofKind(RowKind.DELETE, "key1", 6L, 600L, 1400L),
                Row.of("key1", 6L, 600L, 1200L),
                Row.of("key1", 3L, 200L, 300L),
                Row.ofKind(RowKind.DELETE, "key1", 5L, 500L, 600L),
                Row.of("key1", 5L, 500L, 800L),
                Row.ofKind(RowKind.DELETE, "key1", 6L, 600L, 1200L),
                Row.of("key1", 6L, 600L, 1400L),
                // The following is a +U since val=1L has been
                // inserted before
                Row.ofKind(RowKind.UPDATE_AFTER, "key2", 1L, 100L, 100L),
                // The following is +I because previously val=2L
                // was deleted
                Row.of("key2", 2L, 200L, 300L)
            };

    static final Row[] PARTITION_BY_NON_PK_AFTER_DATA =
            new Row[] {
                Row.ofKind(RowKind.UPDATE_AFTER, "key3", 1L, 100L, 100L),
                Row.of("key1", 4L, 400L, 700L),
                Row.ofKind(RowKind.DELETE, "key1", 5L, 500L, 800L),
                Row.of("key1", 5L, 500L, 1200L),
                Row.ofKind(RowKind.DELETE, "key1", 6L, 600L, 1400L),
                Row.of("key1", 6L, 600L, 1800L),
                Row.ofKind(RowKind.DELETE, "key1", 3L, 200L, 300L),
                Row.ofKind(RowKind.DELETE, "key1", 4L, 400L, 700L),
                Row.of("key1", 4L, 400L, 500L),
                Row.ofKind(RowKind.DELETE, "key1", 5L, 500L, 1200L),
                Row.of("key1", 5L, 500L, 1000L),
                Row.ofKind(RowKind.DELETE, "key1", 6L, 600L, 1800L),
                Row.of("key1", 6L, 600L, 1600L),
                Row.of("key1", 3L, 300L, 400L),
                Row.ofKind(RowKind.DELETE, "key1", 4L, 400L, 500L),
                Row.of("key1", 4L, 400L, 800L),
                Row.ofKind(RowKind.DELETE, "key1", 5L, 500L, 1000L),
                Row.of("key1", 5L, 500L, 1300L),
                Row.ofKind(RowKind.DELETE, "key1", 6L, 600L, 1600L),
                Row.of("key1", 6L, 600L, 1900L)
            };

    static final TableTestProgram
            OVER_AGGREGATE_NON_TIME_RANGE_UNBOUNDED_SUM_RETRACT_MODE_SOURCE_SINK_PRIMARY_KEY_PARTITION_BY_NON_PK =
                    TableTestProgram.of(
                                    "over-aggregate-non-time-range-unbounded-sum-retract-mode-source-sink-primary-key-partition-by-non-pk",
                                    "validates restoring a non-time unbounded preceding sum function in retract mode with source and sink table having primary key but partition by non-primary key")
                            .setupTableSource(
                                    getSource(
                                            new String[] {
                                                "key STRING",
                                                "val BIGINT",
                                                "ts BIGINT",
                                                "PRIMARY KEY(val) NOT ENFORCED"
                                            }))
                            .setupTableSink(
                                    SinkTestStep.newBuilder("sink_t")
                                            .addSchema(
                                                    "key STRING",
                                                    "val BIGINT",
                                                    "ts BIGINT",
                                                    "sum_val BIGINT",
                                                    "PRIMARY KEY(val) NOT ENFORCED")
                                            .consumedBeforeRestore(PARTITION_BY_NON_PK_BEFORE_DATA)
                                            .consumedAfterRestore(PARTITION_BY_NON_PK_AFTER_DATA)
                                            .build())
                            .runSql(
                                    "INSERT INTO sink_t SELECT key, val, ts, SUM(ts) OVER ("
                                            + "PARTITION BY key "
                                            + "ORDER BY val "
                                            + "RANGE BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) "
                                            + "AS sum_ts "
                                            + "FROM source_t")
                            .build();

    static final TableTestProgram
            OVER_AGGREGATE_NON_TIME_ROWS_UNBOUNDED_SUM_RETRACT_MODE_SOURCE_SINK_PRIMARY_KEY_PARTITION_BY_NON_PK =
                    TableTestProgram.of(
                                    "over-aggregate-non-time-rows-unbounded-sum-retract-mode-source-sink-primary-key-partition-by-non-pk",
                                    "validates restoring a non-time unbounded preceding sum function in retract mode with source and sink table having primary key but partition by non-primary key")
                            .setupTableSource(
                                    getSource(
                                            new String[] {
                                                "key STRING",
                                                "val BIGINT",
                                                "ts BIGINT",
                                                "PRIMARY KEY(val) NOT ENFORCED"
                                            }))
                            .setupTableSink(
                                    SinkTestStep.newBuilder("sink_t")
                                            .addSchema(
                                                    "key STRING",
                                                    "val BIGINT",
                                                    "ts BIGINT",
                                                    "sum_val BIGINT",
                                                    "PRIMARY KEY(val) NOT ENFORCED")
                                            .consumedBeforeRestore(PARTITION_BY_NON_PK_BEFORE_DATA)
                                            .consumedAfterRestore(PARTITION_BY_NON_PK_AFTER_DATA)
                                            .build())
                            .runSql(
                                    "INSERT INTO sink_t SELECT key, val, ts, SUM(ts) OVER ("
                                            + "PARTITION BY key "
                                            + "ORDER BY val "
                                            + "ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) "
                                            + "AS sum_ts "
                                            + "FROM source_t")
                            .build();

    static final SourceTestStep APPEND_SOURCE =
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
                    .build();

    static final Row[] SUM_APPEND_MODE_BEFORE_DATA =
            new Row[] {
                Row.of("key1", 1L, 100L, 1L),
                Row.of("key1", 2L, 200L, 3L),
                Row.of("key1", 5L, 500L, 8L),
                Row.of("key1", 6L, 600L, 14L),
                Row.of("key2", 1L, 100L, 1L),
                Row.of("key2", 2L, 200L, 3L)
            };

    static final Row[] SUM_APPEND_MODE_AFTER_DATA =
            new Row[] {
                Row.of("key1", 4L, 400L, 7L),
                Row.ofKind(RowKind.UPDATE_BEFORE, "key1", 5L, 500L, 8L),
                Row.ofKind(RowKind.UPDATE_AFTER, "key1", 5L, 500L, 12L),
                Row.ofKind(RowKind.UPDATE_BEFORE, "key1", 6L, 600L, 14L),
                Row.ofKind(RowKind.UPDATE_AFTER, "key1", 6L, 600L, 18L)
            };

    static final TableTestProgram OVER_AGGREGATE_NON_TIME_RANGE_UNBOUNDED_SUM_APPEND_MODE =
            TableTestProgram.of(
                            "over-aggregate-non-time-range-unbounded-sum-append-mode",
                            "validates restoring a non-time unbounded preceding sum function in append mode")
                    .setupTableSource(APPEND_SOURCE)
                    .setupTableSink(
                            SinkTestStep.newBuilder("sink_t")
                                    .addSchema(
                                            "key STRING",
                                            "val BIGINT",
                                            "ts BIGINT",
                                            "sum_val BIGINT")
                                    .consumedBeforeRestore(SUM_APPEND_MODE_BEFORE_DATA)
                                    .consumedAfterRestore(SUM_APPEND_MODE_AFTER_DATA)
                                    .build())
                    .runSql(
                            "INSERT INTO sink_t SELECT key, val, ts, SUM(val) OVER ("
                                    + "PARTITION BY key "
                                    + "ORDER BY val "
                                    + "RANGE BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) "
                                    + "AS sum_val "
                                    + "FROM source_t")
                    .build();

    static final TableTestProgram OVER_AGGREGATE_NON_TIME_ROWS_UNBOUNDED_SUM_APPEND_MODE =
            TableTestProgram.of(
                            "over-aggregate-non-time-rows-unbounded-sum-append-mode",
                            "validates restoring a non-time unbounded preceding sum function in append mode")
                    .setupTableSource(APPEND_SOURCE)
                    .setupTableSink(
                            SinkTestStep.newBuilder("sink_t")
                                    .addSchema(
                                            "key STRING",
                                            "val BIGINT",
                                            "ts BIGINT",
                                            "sum_val BIGINT")
                                    .consumedBeforeRestore(SUM_APPEND_MODE_BEFORE_DATA)
                                    .consumedAfterRestore(SUM_APPEND_MODE_AFTER_DATA)
                                    .build())
                    .runSql(
                            "INSERT INTO sink_t SELECT key, val, ts, SUM(val) OVER ("
                                    + "PARTITION BY key "
                                    + "ORDER BY val "
                                    + "ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) "
                                    + "AS sum_val "
                                    + "FROM source_t")
                    .build();

    static final Row[] AVG_APPEND_MODE_BEFORE_DATA =
            new Row[] {
                Row.of("key1", 1L, 100L, 1L),
                Row.of("key1", 2L, 200L, 1L),
                Row.of("key1", 5L, 500L, 2L),
                Row.of("key1", 6L, 600L, 3L),
                Row.of("key2", 1L, 100L, 1L),
                Row.of("key2", 2L, 200L, 1L)
            };

    static final Row[] AVG_APPEND_MODE_AFTER_DATA =
            new Row[] {
                Row.of("key1", 4L, 400L, 2L),
                Row.ofKind(RowKind.UPDATE_BEFORE, "key1", 5L, 500L, 2L),
                Row.ofKind(RowKind.UPDATE_AFTER, "key1", 5L, 500L, 3L),
                Row.ofKind(RowKind.UPDATE_BEFORE, "key1", 6L, 600L, 3L),
                Row.ofKind(RowKind.UPDATE_AFTER, "key1", 6L, 600L, 3L)
            };

    static final TableTestProgram OVER_AGGREGATE_NON_TIME_RANGE_UNBOUNDED_AVG_APPEND_MODE =
            TableTestProgram.of(
                            "over-aggregate-non-time-range-unbounded-avg-append-mode",
                            "validates restoring a non-time unbounded preceding avg function in append mode")
                    .setupTableSource(APPEND_SOURCE)
                    .setupTableSink(
                            SinkTestStep.newBuilder("sink_t")
                                    .addSchema(
                                            "key STRING",
                                            "val BIGINT",
                                            "ts BIGINT",
                                            "avg_val BIGINT")
                                    .consumedBeforeRestore(AVG_APPEND_MODE_BEFORE_DATA)
                                    .consumedAfterRestore(AVG_APPEND_MODE_AFTER_DATA)
                                    .build())
                    .runSql(
                            "INSERT INTO sink_t SELECT key, val, ts, AVG(val) OVER ("
                                    + "PARTITION BY key "
                                    + "ORDER BY val "
                                    + "RANGE BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) "
                                    + "AS avg_val "
                                    + "FROM source_t")
                    .build();

    static final TableTestProgram OVER_AGGREGATE_NON_TIME_ROWS_UNBOUNDED_AVG_APPEND_MODE =
            TableTestProgram.of(
                            "over-aggregate-non-time-rows-unbounded-avg-append-mode",
                            "validates restoring a non-time unbounded preceding avg function in append mode")
                    .setupTableSource(APPEND_SOURCE)
                    .setupTableSink(
                            SinkTestStep.newBuilder("sink_t")
                                    .addSchema(
                                            "key STRING",
                                            "val BIGINT",
                                            "ts BIGINT",
                                            "avg_val BIGINT")
                                    .consumedBeforeRestore(AVG_APPEND_MODE_BEFORE_DATA)
                                    .consumedAfterRestore(AVG_APPEND_MODE_AFTER_DATA)
                                    .build())
                    .runSql(
                            "INSERT INTO sink_t SELECT key, val, ts, AVG(val) OVER ("
                                    + "PARTITION BY key "
                                    + "ORDER BY val "
                                    + "ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) "
                                    + "AS avg_val "
                                    + "FROM source_t")
                    .build();

    static final Row[] MULTIPLE_AGGS_APPEND_MODE_BEFORE_DATA =
            new Row[] {
                Row.of("key1", 1L, 100L, 1L, 1L),
                Row.of("key1", 2L, 200L, 3L, 2L),
                Row.of("key1", 5L, 500L, 8L, 3L),
                Row.of("key1", 6L, 600L, 14L, 4L),
                Row.of("key2", 1L, 100L, 1L, 1L),
                Row.of("key2", 2L, 200L, 3L, 2L)
            };

    static final Row[] MULTIPLE_AGGS_APPEND_MODE_AFTER_DATA =
            new Row[] {
                Row.of("key1", 4L, 400L, 7L, 3L),
                Row.ofKind(RowKind.UPDATE_BEFORE, "key1", 5L, 500L, 8L, 3L),
                Row.ofKind(RowKind.UPDATE_AFTER, "key1", 5L, 500L, 12L, 4L),
                Row.ofKind(RowKind.UPDATE_BEFORE, "key1", 6L, 600L, 14L, 4L),
                Row.ofKind(RowKind.UPDATE_AFTER, "key1", 6L, 600L, 18L, 5L)
            };

    static final TableTestProgram
            OVER_AGGREGATE_NON_TIME_RANGE_UNBOUNDED_MULTIPLE_AGGS_APPEND_MODE =
                    TableTestProgram.of(
                                    "over-aggregate-non-time-range-unbounded-multiple-aggs-append-mode",
                                    "validates restoring a non-time unbounded preceding sum function in append mode with multiple aggregations")
                            .setupTableSource(APPEND_SOURCE)
                            .setupTableSink(
                                    SinkTestStep.newBuilder("sink_t")
                                            .addSchema(
                                                    "key STRING",
                                                    "val BIGINT",
                                                    "ts BIGINT",
                                                    "sum_val BIGINT",
                                                    "cnt_key BIGINT")
                                            .consumedBeforeRestore(
                                                    MULTIPLE_AGGS_APPEND_MODE_BEFORE_DATA)
                                            .consumedAfterRestore(
                                                    MULTIPLE_AGGS_APPEND_MODE_AFTER_DATA)
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

    static final TableTestProgram OVER_AGGREGATE_NON_TIME_ROWS_UNBOUNDED_MULTIPLE_AGGS_APPEND_MODE =
            TableTestProgram.of(
                            "over-aggregate-non-time-rows-unbounded-multiple-aggs-append-mode",
                            "validates restoring a non-time unbounded preceding sum function in append mode with multiple aggregations")
                    .setupTableSource(APPEND_SOURCE)
                    .setupTableSink(
                            SinkTestStep.newBuilder("sink_t")
                                    .addSchema(
                                            "key STRING",
                                            "val BIGINT",
                                            "ts BIGINT",
                                            "sum_val BIGINT",
                                            "cnt_key BIGINT")
                                    .consumedBeforeRestore(MULTIPLE_AGGS_APPEND_MODE_BEFORE_DATA)
                                    .consumedAfterRestore(MULTIPLE_AGGS_APPEND_MODE_AFTER_DATA)
                                    .build())
                    .runSql(
                            "INSERT INTO sink_t SELECT key, val, ts, "
                                    + "SUM(val) OVER ("
                                    + "PARTITION BY key "
                                    + "ORDER BY val "
                                    + "ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) "
                                    + "AS sum_val, "
                                    + "COUNT(key) OVER ("
                                    + "PARTITION BY key "
                                    + "ORDER BY val "
                                    + "ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) "
                                    + "AS cnt_key "
                                    + "FROM source_t")
                    .build();

    static final TableTestProgram OVER_AGGREGATE_NON_TIME_RANGE_UNBOUNDED_SUM_NO_PARTITION_BY =
            TableTestProgram.of(
                            "over-aggregate-non-time-range-unbounded-sum-no-partition-by",
                            "validates restoring a non-time unbounded preceding sum function without partition by")
                    .setupTableSource(APPEND_SOURCE)
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
                                            Row.ofKind(RowKind.UPDATE_BEFORE, "key1", 1L, 100L, 1L),
                                            Row.ofKind(RowKind.UPDATE_AFTER, "key1", 1L, 100L, 2L),
                                            Row.of("key2", 1L, 100L, 2L),
                                            Row.ofKind(RowKind.UPDATE_BEFORE, "key1", 2L, 200L, 3L),
                                            Row.ofKind(RowKind.UPDATE_AFTER, "key1", 2L, 200L, 4L),
                                            Row.ofKind(RowKind.UPDATE_BEFORE, "key1", 5L, 500L, 8L),
                                            Row.ofKind(RowKind.UPDATE_AFTER, "key1", 5L, 500L, 9L),
                                            Row.ofKind(
                                                    RowKind.UPDATE_BEFORE, "key1", 6L, 600L, 14L),
                                            Row.ofKind(RowKind.UPDATE_AFTER, "key1", 6L, 600L, 15L),
                                            Row.ofKind(RowKind.UPDATE_BEFORE, "key1", 2L, 200L, 4L),
                                            Row.ofKind(RowKind.UPDATE_AFTER, "key1", 2L, 200L, 6L),
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

    static final TableTestProgram OVER_AGGREGATE_NON_TIME_ROWS_UNBOUNDED_SUM_NO_PARTITION_BY =
            TableTestProgram.of(
                            "over-aggregate-non-time-rows-unbounded-sum-no-partition-by",
                            "validates restoring a non-time unbounded preceding sum function without partition by")
                    .setupTableSource(APPEND_SOURCE)
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
                                    + "ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) "
                                    + "AS sum_val "
                                    + "FROM source_t")
                    .build();
}
