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

import org.apache.flink.table.api.config.AggregatePhaseStrategy;
import org.apache.flink.table.api.config.OptimizerConfigOptions;
import org.apache.flink.table.test.program.SinkTestStep;
import org.apache.flink.table.test.program.SourceTestStep;
import org.apache.flink.table.test.program.TableTestProgram;
import org.apache.flink.types.Row;

import java.math.BigDecimal;
import java.util.function.Function;

/** {@link TableTestProgram} definitions for testing {@link StreamExecWindowAggregate}. */
public class WindowAggregateTestPrograms {

    enum DistinctAggSplit {
        ENABLED,
        DISABLED
    }

    private static final Row[] BEFORE_DATA = {
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

    private static final Row[] AFTER_DATA = {
        Row.of("2020-10-10 00:00:40", 10, 3d, 3f, new BigDecimal("4.44"), "Comment#4", "a"),
        Row.of("2020-10-10 00:00:42", 11, 4d, 4f, new BigDecimal("5.44"), "Comment#5", "d"),
        Row.of("2020-10-10 00:00:43", 12, 5d, 5f, new BigDecimal("6.44"), "Comment#6", "c"),
        Row.of("2020-10-10 00:00:44", 13, 6d, 6f, new BigDecimal("7.44"), "Comment#7", "d")
    };

    private static final Function<String, SourceTestStep.Builder> SOURCE_BUILDER =
            str ->
                    SourceTestStep.newBuilder(str)
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
                            .producedAfterRestore(AFTER_DATA);
    private static final SourceTestStep SOURCE = SOURCE_BUILDER.apply("window_source_t").build();

    private static final SourceTestStep CDC_SOURCE =
            SOURCE_BUILDER
                    .apply("cdc_window_source_t")
                    .addOption("changelog-mode", "I,UA,UB,D")
                    .build();

    private static final String[] TUMBLE_EVENT_TIME_BEFORE_ROWS = {
        "+I[a, 2020-10-10T00:00, 2020-10-10T00:00:05, 4, 10, 2]",
        "+I[a, 2020-10-10T00:00:05, 2020-10-10T00:00:10, 1, 3, 1]",
        "+I[b, 2020-10-10T00:00:05, 2020-10-10T00:00:10, 2, 9, 2]",
        "+I[b, 2020-10-10T00:00:15, 2020-10-10T00:00:20, 1, 4, 1]"
    };

    private static final String[] TUMBLE_EVENT_TIME_AFTER_ROWS = {
        "+I[b, 2020-10-10T00:00:30, 2020-10-10T00:00:35, 1, 1, 1]",
        "+I[null, 2020-10-10T00:00:30, 2020-10-10T00:00:35, 1, 7, 0]",
        "+I[a, 2020-10-10T00:00:40, 2020-10-10T00:00:45, 1, 10, 1]",
        "+I[c, 2020-10-10T00:00:40, 2020-10-10T00:00:45, 1, 12, 1]",
        "+I[d, 2020-10-10T00:00:40, 2020-10-10T00:00:45, 2, 24, 2]"
    };

    static final TableTestProgram TUMBLE_WINDOW_EVENT_TIME =
            getTableTestProgram(
                    "window-aggregate-tumble-event-time",
                    "validates group by using tumbling window with event time",
                    AggregatePhaseStrategy.ONE_PHASE.toString(),
                    "TUMBLE(TABLE window_source_t, DESCRIPTOR(rowtime), INTERVAL '5' SECOND)",
                    TUMBLE_EVENT_TIME_BEFORE_ROWS,
                    TUMBLE_EVENT_TIME_AFTER_ROWS);

    static final TableTestProgram TUMBLE_WINDOW_EVENT_TIME_TWO_PHASE =
            getTableTestProgram(
                    "window-aggregate-tumble-event-time-two-phase",
                    "validates group by using tumbling window with event time with two phase aggregation",
                    AggregatePhaseStrategy.TWO_PHASE.toString(),
                    "TUMBLE(TABLE window_source_t, DESCRIPTOR(rowtime), INTERVAL '5' SECOND)",
                    TUMBLE_EVENT_TIME_BEFORE_ROWS,
                    TUMBLE_EVENT_TIME_AFTER_ROWS);

    static final TableTestProgram TUMBLE_WINDOW_EVENT_TIME_TWO_PHASE_DISTINCT_SPLIT =
            getTableTestProgram(
                    "window-aggregate-tumble-event-time-two-phase-distinct-split",
                    "validates group by using tumbling window with event time with two phase aggregation",
                    AggregatePhaseStrategy.TWO_PHASE.toString(),
                    "TUMBLE(TABLE window_source_t, DESCRIPTOR(rowtime), INTERVAL '5' SECOND)",
                    TUMBLE_EVENT_TIME_BEFORE_ROWS,
                    TUMBLE_EVENT_TIME_AFTER_ROWS,
                    DistinctAggSplit.ENABLED);

    private static final String[] TUMBLE_EVENT_TIME_WITH_OFFSET_BEFORE_ROWS = {
        "+I[a, 2020-10-10T00:00:01, 2020-10-10T00:00:06, 4, 10, 2]",
        "+I[b, 2020-10-10T00:00:06, 2020-10-10T00:00:11, 2, 9, 2]",
        "+I[a, 2020-10-10T00:00:06, 2020-10-10T00:00:11, 1, 3, 1]",
        "+I[b, 2020-10-10T00:00:16, 2020-10-10T00:00:21, 1, 4, 1]"
    };

    private static final String[] TUMBLE_EVENT_TIME_WITH_OFFSET_AFTER_ROWS = {
        "+I[b, 2020-10-10T00:00:31, 2020-10-10T00:00:36, 1, 1, 1]",
        "+I[null, 2020-10-10T00:00:31, 2020-10-10T00:00:36, 1, 7, 0]",
        "+I[a, 2020-10-10T00:00:36, 2020-10-10T00:00:41, 1, 10, 1]",
        "+I[c, 2020-10-10T00:00:41, 2020-10-10T00:00:46, 1, 12, 1]",
        "+I[d, 2020-10-10T00:00:41, 2020-10-10T00:00:46, 2, 24, 2]"
    };

    static final TableTestProgram TUMBLE_WINDOW_EVENT_TIME_WITH_OFFSET =
            getTableTestProgram(
                    "window-aggregate-tumble-event-time-with-offset",
                    "validates group by using tumbling window with event time with an offset",
                    AggregatePhaseStrategy.ONE_PHASE.toString(),
                    "TUMBLE(TABLE window_source_t, DESCRIPTOR(rowtime), INTERVAL '5' SECOND, INTERVAL '1' SECOND)",
                    TUMBLE_EVENT_TIME_WITH_OFFSET_BEFORE_ROWS,
                    TUMBLE_EVENT_TIME_WITH_OFFSET_AFTER_ROWS);

    static final TableTestProgram TUMBLE_WINDOW_EVENT_TIME_TWO_PHASE_WITH_OFFSET =
            getTableTestProgram(
                    "window-aggregate-tumble-event-time-two-phase-with-offset",
                    "validates group by using tumbling window with event time with two phase aggregation with an offset",
                    AggregatePhaseStrategy.TWO_PHASE.toString(),
                    "TUMBLE(TABLE window_source_t, DESCRIPTOR(rowtime), INTERVAL '5' SECOND, INTERVAL '1' SECOND)",
                    TUMBLE_EVENT_TIME_WITH_OFFSET_BEFORE_ROWS,
                    TUMBLE_EVENT_TIME_WITH_OFFSET_AFTER_ROWS);

    static final TableTestProgram TUMBLE_WINDOW_EVENT_TIME_TWO_PHASE_WITH_OFFSET_DISTINCT_SPLIT =
            getTableTestProgram(
                    "window-aggregate-tumble-event-time-two-phase-with-offset-distinct-split",
                    "validates group by using tumbling window with event time with two phase aggregation with an offset",
                    AggregatePhaseStrategy.TWO_PHASE.toString(),
                    "TUMBLE(TABLE window_source_t, DESCRIPTOR(rowtime), INTERVAL '5' SECOND, INTERVAL '1' SECOND)",
                    TUMBLE_EVENT_TIME_WITH_OFFSET_BEFORE_ROWS,
                    TUMBLE_EVENT_TIME_WITH_OFFSET_AFTER_ROWS,
                    DistinctAggSplit.ENABLED);

    private static final String[] HOP_EVENT_TIME_BEFORE_ROWS = {
        "+I[a, 2020-10-09T23:59:55, 2020-10-10T00:00:05, 4, 10, 2]",
        "+I[b, 2020-10-10T00:00, 2020-10-10T00:00:10, 2, 9, 2]",
        "+I[a, 2020-10-10T00:00, 2020-10-10T00:00:10, 6, 18, 3]",
        "+I[b, 2020-10-10T00:00:05, 2020-10-10T00:00:15, 2, 9, 2]",
        "+I[a, 2020-10-10T00:00:05, 2020-10-10T00:00:15, 1, 3, 1]",
        "+I[b, 2020-10-10T00:00:10, 2020-10-10T00:00:20, 1, 4, 1]",
        "+I[b, 2020-10-10T00:00:15, 2020-10-10T00:00:25, 1, 4, 1]"
    };

    private static final String[] HOP_EVENT_TIME_AFTER_ROWS = {
        "+I[b, 2020-10-10T00:00:25, 2020-10-10T00:00:35, 1, 1, 1]",
        "+I[null, 2020-10-10T00:00:25, 2020-10-10T00:00:35, 1, 7, 0]",
        "+I[b, 2020-10-10T00:00:30, 2020-10-10T00:00:40, 1, 1, 1]",
        "+I[null, 2020-10-10T00:00:30, 2020-10-10T00:00:40, 1, 7, 0]",
        "+I[c, 2020-10-10T00:00:35, 2020-10-10T00:00:45, 1, 12, 1]",
        "+I[d, 2020-10-10T00:00:35, 2020-10-10T00:00:45, 2, 24, 2]",
        "+I[a, 2020-10-10T00:00:35, 2020-10-10T00:00:45, 1, 10, 1]",
        "+I[d, 2020-10-10T00:00:40, 2020-10-10T00:00:50, 2, 24, 2]",
        "+I[a, 2020-10-10T00:00:40, 2020-10-10T00:00:50, 1, 10, 1]",
        "+I[c, 2020-10-10T00:00:40, 2020-10-10T00:00:50, 1, 12, 1]"
    };

    static final TableTestProgram HOP_WINDOW_EVENT_TIME =
            getTableTestProgram(
                    "window-aggregate-hop-event-time",
                    "validates group by using a hop window with event time",
                    AggregatePhaseStrategy.ONE_PHASE.toString(),
                    "HOP(TABLE window_source_t, DESCRIPTOR(rowtime), INTERVAL '5' SECOND, INTERVAL '10' SECOND)",
                    HOP_EVENT_TIME_BEFORE_ROWS,
                    HOP_EVENT_TIME_AFTER_ROWS);

    static final TableTestProgram HOP_WINDOW_EVENT_TIME_TWO_PHASE =
            getTableTestProgram(
                    "window-aggregate-hop-event-time-two-phase",
                    "validates group by using a hop window with event time with two phase aggregation",
                    AggregatePhaseStrategy.TWO_PHASE.toString(),
                    "HOP(TABLE window_source_t, DESCRIPTOR(rowtime), INTERVAL '5' SECOND, INTERVAL '10' SECOND)",
                    HOP_EVENT_TIME_BEFORE_ROWS,
                    HOP_EVENT_TIME_AFTER_ROWS);

    static final TableTestProgram HOP_WINDOW_EVENT_TIME_TWO_PHASE_DISTINCT_SPLIT =
            getTableTestProgram(
                    "window-aggregate-hop-event-time-two-phase-distinct-split",
                    "validates group by using a hop window with event time with two phase aggregation",
                    AggregatePhaseStrategy.TWO_PHASE.toString(),
                    "HOP(TABLE window_source_t, DESCRIPTOR(rowtime), INTERVAL '5' SECOND, INTERVAL '10' SECOND)",
                    HOP_EVENT_TIME_BEFORE_ROWS,
                    HOP_EVENT_TIME_AFTER_ROWS,
                    DistinctAggSplit.ENABLED);

    private static final String[] HOP_EVENT_TIME_WITH_OFFSET_BEFORE_ROWS = {
        "+I[a, 2020-10-09T23:59:56, 2020-10-10T00:00:06, 4, 10, 2]",
        "+I[b, 2020-10-10T00:00:01, 2020-10-10T00:00:11, 2, 9, 2]",
        "+I[a, 2020-10-10T00:00:01, 2020-10-10T00:00:11, 6, 18, 3]",
        "+I[b, 2020-10-10T00:00:06, 2020-10-10T00:00:16, 2, 9, 2]",
        "+I[a, 2020-10-10T00:00:06, 2020-10-10T00:00:16, 1, 3, 1]",
        "+I[b, 2020-10-10T00:00:11, 2020-10-10T00:00:21, 1, 4, 1]",
        "+I[b, 2020-10-10T00:00:16, 2020-10-10T00:00:26, 1, 4, 1]"
    };

    private static final String[] HOP_EVENT_TIME_WITH_OFFSET_AFTER_ROWS = {
        "+I[b, 2020-10-10T00:00:26, 2020-10-10T00:00:36, 1, 1, 1]",
        "+I[null, 2020-10-10T00:00:26, 2020-10-10T00:00:36, 1, 7, 0]",
        "+I[a, 2020-10-10T00:00:31, 2020-10-10T00:00:41, 1, 10, 1]",
        "+I[b, 2020-10-10T00:00:31, 2020-10-10T00:00:41, 1, 1, 1]",
        "+I[null, 2020-10-10T00:00:31, 2020-10-10T00:00:41, 1, 7, 0]",
        "+I[c, 2020-10-10T00:00:36, 2020-10-10T00:00:46, 1, 12, 1]",
        "+I[d, 2020-10-10T00:00:36, 2020-10-10T00:00:46, 2, 24, 2]",
        "+I[a, 2020-10-10T00:00:36, 2020-10-10T00:00:46, 1, 10, 1]",
        "+I[d, 2020-10-10T00:00:41, 2020-10-10T00:00:51, 2, 24, 2]",
        "+I[c, 2020-10-10T00:00:41, 2020-10-10T00:00:51, 1, 12, 1]"
    };

    static final TableTestProgram HOP_WINDOW_EVENT_TIME_WITH_OFFSET =
            getTableTestProgram(
                    "window-aggregate-hop-event-time-with-offset",
                    "validates group by using a hop window with event time with an offset",
                    AggregatePhaseStrategy.ONE_PHASE.toString(),
                    "HOP(TABLE window_source_t, DESCRIPTOR(rowtime), INTERVAL '5' SECOND, INTERVAL '10' SECOND, INTERVAL '1' SECOND)",
                    HOP_EVENT_TIME_WITH_OFFSET_BEFORE_ROWS,
                    HOP_EVENT_TIME_WITH_OFFSET_AFTER_ROWS);

    static final TableTestProgram HOP_WINDOW_EVENT_TIME_TWO_PHASE_WITH_OFFSET =
            getTableTestProgram(
                    "window-aggregate-hop-event-time-two-phase-with-offset",
                    "validates group by using a hop window with event time with two phase aggregation with an offset",
                    AggregatePhaseStrategy.TWO_PHASE.toString(),
                    "HOP(TABLE window_source_t, DESCRIPTOR(rowtime), INTERVAL '5' SECOND, INTERVAL '10' SECOND, INTERVAL '1' SECOND)",
                    HOP_EVENT_TIME_WITH_OFFSET_BEFORE_ROWS,
                    HOP_EVENT_TIME_WITH_OFFSET_AFTER_ROWS);

    static final TableTestProgram HOP_WINDOW_EVENT_TIME_TWO_PHASE_WITH_OFFSET_DISTINCT_SPLIT =
            getTableTestProgram(
                    "window-aggregate-hop-event-time-two-phase-with-offset-distinct-split",
                    "validates group by using a hop window with event time with two phase aggregation with an offset",
                    AggregatePhaseStrategy.TWO_PHASE.toString(),
                    "HOP(TABLE window_source_t, DESCRIPTOR(rowtime), INTERVAL '5' SECOND, INTERVAL '10' SECOND, INTERVAL '1' SECOND)",
                    HOP_EVENT_TIME_WITH_OFFSET_BEFORE_ROWS,
                    HOP_EVENT_TIME_WITH_OFFSET_AFTER_ROWS,
                    DistinctAggSplit.ENABLED);

    private static final String[] CUMULATE_EVENT_TIME_BEFORE_ROWS = {
        "+I[a, 2020-10-10T00:00, 2020-10-10T00:00:05, 4, 10, 2]",
        "+I[b, 2020-10-10T00:00, 2020-10-10T00:00:10, 2, 9, 2]",
        "+I[a, 2020-10-10T00:00, 2020-10-10T00:00:10, 6, 18, 3]",
        "+I[b, 2020-10-10T00:00, 2020-10-10T00:00:15, 2, 9, 2]",
        "+I[a, 2020-10-10T00:00, 2020-10-10T00:00:15, 6, 18, 3]",
        "+I[b, 2020-10-10T00:00:15, 2020-10-10T00:00:20, 1, 4, 1]",
        "+I[b, 2020-10-10T00:00:15, 2020-10-10T00:00:25, 1, 4, 1]",
        "+I[b, 2020-10-10T00:00:15, 2020-10-10T00:00:30, 1, 4, 1]"
    };

    private static final String[] CUMULATE_EVENT_TIME_AFTER_ROWS = {
        "+I[b, 2020-10-10T00:00:30, 2020-10-10T00:00:35, 1, 1, 1]",
        "+I[null, 2020-10-10T00:00:30, 2020-10-10T00:00:35, 1, 7, 0]",
        "+I[b, 2020-10-10T00:00:30, 2020-10-10T00:00:40, 1, 1, 1]",
        "+I[null, 2020-10-10T00:00:30, 2020-10-10T00:00:40, 1, 7, 0]",
        "+I[b, 2020-10-10T00:00:30, 2020-10-10T00:00:45, 1, 1, 1]",
        "+I[c, 2020-10-10T00:00:30, 2020-10-10T00:00:45, 1, 12, 1]",
        "+I[d, 2020-10-10T00:00:30, 2020-10-10T00:00:45, 2, 24, 2]",
        "+I[a, 2020-10-10T00:00:30, 2020-10-10T00:00:45, 1, 10, 1]",
        "+I[null, 2020-10-10T00:00:30, 2020-10-10T00:00:45, 1, 7, 0]"
    };

    static final TableTestProgram CUMULATE_WINDOW_EVENT_TIME =
            getTableTestProgram(
                    "window-aggregate-cumulate-event-time",
                    "validates group by using cumulate window with event time",
                    AggregatePhaseStrategy.ONE_PHASE.toString(),
                    "CUMULATE(TABLE window_source_t, DESCRIPTOR(rowtime), INTERVAL '5' SECOND, INTERVAL '15' SECOND)",
                    CUMULATE_EVENT_TIME_BEFORE_ROWS,
                    CUMULATE_EVENT_TIME_AFTER_ROWS);

    static final TableTestProgram CUMULATE_WINDOW_EVENT_TIME_TWO_PHASE =
            getTableTestProgram(
                    "window-aggregate-cumulate-event-time-two-phase",
                    "validates group by using cumulate window with event time with two phase aggregation",
                    AggregatePhaseStrategy.TWO_PHASE.toString(),
                    "CUMULATE(TABLE window_source_t, DESCRIPTOR(rowtime), INTERVAL '5' SECOND, INTERVAL '15' SECOND)",
                    CUMULATE_EVENT_TIME_BEFORE_ROWS,
                    CUMULATE_EVENT_TIME_AFTER_ROWS);

    static final TableTestProgram CUMULATE_WINDOW_EVENT_TIME_TWO_PHASE_DISTINCT_SPLIT =
            getTableTestProgram(
                    "window-aggregate-cumulate-event-time-two-phase-distinct-split",
                    "validates group by using cumulate window with event time with two phase aggregation with distinct split",
                    AggregatePhaseStrategy.TWO_PHASE.toString(),
                    "CUMULATE(TABLE window_source_t, DESCRIPTOR(rowtime), INTERVAL '5' SECOND, INTERVAL '15' SECOND)",
                    CUMULATE_EVENT_TIME_BEFORE_ROWS,
                    CUMULATE_EVENT_TIME_AFTER_ROWS,
                    DistinctAggSplit.ENABLED);

    private static final String[] CUMULATE_EVENT_TIME_WITH_OFFSET_BEFORE_ROWS = {
        "+I[a, 2020-10-10T00:00:01, 2020-10-10T00:00:06, 4, 10, 2]",
        "+I[b, 2020-10-10T00:00:01, 2020-10-10T00:00:11, 2, 9, 2]",
        "+I[a, 2020-10-10T00:00:01, 2020-10-10T00:00:11, 6, 18, 3]",
        "+I[b, 2020-10-10T00:00:01, 2020-10-10T00:00:16, 2, 9, 2]",
        "+I[a, 2020-10-10T00:00:01, 2020-10-10T00:00:16, 6, 18, 3]",
        "+I[b, 2020-10-10T00:00:16, 2020-10-10T00:00:21, 1, 4, 1]",
        "+I[b, 2020-10-10T00:00:16, 2020-10-10T00:00:26, 1, 4, 1]",
        "+I[b, 2020-10-10T00:00:16, 2020-10-10T00:00:31, 1, 4, 1]"
    };

    private static final String[] CUMULATE_EVENT_TIME_WITH_OFFSET_AFTER_ROWS = {
        "+I[b, 2020-10-10T00:00:31, 2020-10-10T00:00:36, 1, 1, 1]",
        "+I[null, 2020-10-10T00:00:31, 2020-10-10T00:00:36, 1, 7, 0]",
        "+I[a, 2020-10-10T00:00:31, 2020-10-10T00:00:41, 1, 10, 1]",
        "+I[b, 2020-10-10T00:00:31, 2020-10-10T00:00:41, 1, 1, 1]",
        "+I[null, 2020-10-10T00:00:31, 2020-10-10T00:00:41, 1, 7, 0]",
        "+I[b, 2020-10-10T00:00:31, 2020-10-10T00:00:46, 1, 1, 1]",
        "+I[c, 2020-10-10T00:00:31, 2020-10-10T00:00:46, 1, 12, 1]",
        "+I[d, 2020-10-10T00:00:31, 2020-10-10T00:00:46, 2, 24, 2]",
        "+I[a, 2020-10-10T00:00:31, 2020-10-10T00:00:46, 1, 10, 1]",
        "+I[null, 2020-10-10T00:00:31, 2020-10-10T00:00:46, 1, 7, 0]"
    };

    static final TableTestProgram CUMULATE_WINDOW_EVENT_TIME_WITH_OFFSET =
            getTableTestProgram(
                    "window-aggregate-cumulate-event-time-with-offset",
                    "validates group by using cumulate window with event time with an offset",
                    AggregatePhaseStrategy.ONE_PHASE.toString(),
                    "CUMULATE(TABLE window_source_t, DESCRIPTOR(rowtime), INTERVAL '5' SECOND, INTERVAL '15' SECOND, INTERVAL '1' SECOND)",
                    CUMULATE_EVENT_TIME_WITH_OFFSET_BEFORE_ROWS,
                    CUMULATE_EVENT_TIME_WITH_OFFSET_AFTER_ROWS);

    static final TableTestProgram CUMULATE_WINDOW_EVENT_TIME_TWO_PHASE_WITH_OFFSET =
            getTableTestProgram(
                    "window-aggregate-cumulate-event-time-two-phase-with-offset",
                    "validates group by using cumulate window with event time with two phase aggregation with an offset",
                    AggregatePhaseStrategy.TWO_PHASE.toString(),
                    "CUMULATE(TABLE window_source_t, DESCRIPTOR(rowtime), INTERVAL '5' SECOND, INTERVAL '15' SECOND, INTERVAL '1' SECOND)",
                    CUMULATE_EVENT_TIME_WITH_OFFSET_BEFORE_ROWS,
                    CUMULATE_EVENT_TIME_WITH_OFFSET_AFTER_ROWS);

    static final TableTestProgram CUMULATE_WINDOW_EVENT_TIME_WITH_OFFSET_DISTINCT_SPLIT =
            getTableTestProgram(
                    "window-aggregate-cumulate-event-time-with-offset-distinct-split",
                    "validates group by using cumulate window with event time with an offset",
                    AggregatePhaseStrategy.TWO_PHASE.toString(),
                    "CUMULATE(TABLE window_source_t, DESCRIPTOR(rowtime), INTERVAL '5' SECOND, INTERVAL '15' SECOND, INTERVAL '1' SECOND)",
                    CUMULATE_EVENT_TIME_WITH_OFFSET_BEFORE_ROWS,
                    CUMULATE_EVENT_TIME_WITH_OFFSET_AFTER_ROWS,
                    DistinctAggSplit.ENABLED);

    private static final String[] SESSION_EVENT_TIME_BEFORE_ROWS = {
        "+I[a, 2020-10-10T00:00:01, 2020-10-10T00:00:13, 1, 1, 1]",
        "-U[a, 2020-10-10T00:00:01, 2020-10-10T00:00:13, 1, 1, 1]",
        "+U[a, 2020-10-10T00:00:01, 2020-10-10T00:00:13, 2, 3, 2]",
        "-U[a, 2020-10-10T00:00:01, 2020-10-10T00:00:13, 2, 3, 2]",
        "+U[a, 2020-10-10T00:00:01, 2020-10-10T00:00:13, 3, 5, 2]",
        "-U[a, 2020-10-10T00:00:01, 2020-10-10T00:00:13, 3, 5, 2]",
        "+U[a, 2020-10-10T00:00:01, 2020-10-10T00:00:13, 4, 10, 2]",
        "+I[b, 2020-10-10T00:00:01, 2020-10-10T00:00:13, 1, 3, 1]",
        "-U[b, 2020-10-10T00:00:01, 2020-10-10T00:00:13, 1, 3, 1]",
        "+U[b, 2020-10-10T00:00:01, 2020-10-10T00:00:13, 2, 9, 2]",
        "-U[a, 2020-10-10T00:00:01, 2020-10-10T00:00:13, 4, 10, 2]",
        "+U[a, 2020-10-10T00:00:01, 2020-10-10T00:00:13, 5, 13, 3]",
        "-U[a, 2020-10-10T00:00:01, 2020-10-10T00:00:13, 5, 13, 3]",
        "+U[a, 2020-10-10T00:00:01, 2020-10-10T00:00:13, 6, 18, 3]",
        "+I[b, 2020-10-10T00:00:16, 2020-10-10T00:00:21, 1, 4, 1]"
    };

    private static final String[] SESSION_EVENT_TIME_AFTER_ROWS = {
        "+I[null, 2020-10-10T00:00:32, 2020-10-10T00:00:39, 1, 7, 0]",
        "+I[b, 2020-10-10T00:00:32, 2020-10-10T00:00:39, 1, 1, 1]",
        "+I[a, 2020-10-10T00:00:40, 2020-10-10T00:00:49, 1, 10, 1]",
        "+I[d, 2020-10-10T00:00:40, 2020-10-10T00:00:49, 1, 11, 1]",
        "+I[c, 2020-10-10T00:00:40, 2020-10-10T00:00:49, 1, 12, 1]",
        "-U[d, 2020-10-10T00:00:40, 2020-10-10T00:00:49, 1, 11, 1]",
        "+U[d, 2020-10-10T00:00:40, 2020-10-10T00:00:49, 2, 24, 2]"
    };

    static final TableTestProgram SESSION_WINDOW_EVENT_TIME =
            getTableTestProgram(
                    "window-aggregate-session-event-time",
                    "validates group by using session window with event time",
                    AggregatePhaseStrategy.ONE_PHASE.toString(),
                    "SESSION(TABLE window_source_t, DESCRIPTOR(rowtime), INTERVAL '5' SECOND)",
                    SESSION_EVENT_TIME_BEFORE_ROWS,
                    SESSION_EVENT_TIME_AFTER_ROWS);

    static final TableTestProgram SESSION_WINDOW_EVENT_TIME_TWO_PHASE =
            getTableTestProgram(
                    "window-aggregate-session-event-time-two-phase",
                    "validates group by using session window with event time with two phase aggregation",
                    AggregatePhaseStrategy.TWO_PHASE.toString(),
                    "SESSION(TABLE window_source_t, DESCRIPTOR(rowtime), INTERVAL '5' SECOND)",
                    SESSION_EVENT_TIME_BEFORE_ROWS,
                    SESSION_EVENT_TIME_AFTER_ROWS);

    static final TableTestProgram SESSION_WINDOW_EVENT_TIME_TWO_PHASE_DISTINCT_SPLIT =
            getTableTestProgram(
                    "window-aggregate-session-event-time-two-phase-distinct-split",
                    "validates group by using session window with event time with two phase aggregation",
                    AggregatePhaseStrategy.TWO_PHASE.toString(),
                    "SESSION(TABLE window_source_t, DESCRIPTOR(rowtime), INTERVAL '5' SECOND)",
                    SESSION_EVENT_TIME_BEFORE_ROWS,
                    SESSION_EVENT_TIME_AFTER_ROWS,
                    DistinctAggSplit.ENABLED);

    private static final String[] SESSION_EVENT_TIME_PARTITIONED_BEFORE_ROWS = {
        "+I[b, 2020-10-10T00:00:06, 2020-10-10T00:00:12, 2, 9, 2]",
        "+I[a, 2020-10-10T00:00:01, 2020-10-10T00:00:13, 6, 18, 3]",
        "+I[b, 2020-10-10T00:00:16, 2020-10-10T00:00:21, 1, 4, 1]"
    };

    private static final String[] SESSION_EVENT_TIME_PARTITIONED_AFTER_ROWS = {
        "+I[null, 2020-10-10T00:00:32, 2020-10-10T00:00:37, 1, 7, 0]",
        "+I[b, 2020-10-10T00:00:34, 2020-10-10T00:00:39, 1, 1, 1]",
        "+I[a, 2020-10-10T00:00:40, 2020-10-10T00:00:45, 1, 10, 1]",
        "+I[c, 2020-10-10T00:00:43, 2020-10-10T00:00:48, 1, 12, 1]",
        "+I[d, 2020-10-10T00:00:42, 2020-10-10T00:00:49, 2, 24, 2]"
    };

    static final TableTestProgram SESSION_WINDOW_PARTITION_EVENT_TIME =
            getTableTestProgram(
                    "window-aggregate-session-partition-event-time",
                    "validates group by using session window with event time",
                    AggregatePhaseStrategy.ONE_PHASE.toString(),
                    "SESSION(TABLE window_source_t PARTITION BY name, DESCRIPTOR(rowtime), INTERVAL '5' SECOND)",
                    SESSION_EVENT_TIME_PARTITIONED_BEFORE_ROWS,
                    SESSION_EVENT_TIME_PARTITIONED_AFTER_ROWS);

    static final TableTestProgram SESSION_WINDOW_PARTITION_EVENT_TIME_TWO_PHASE =
            getTableTestProgram(
                    "window-aggregate-session-partition-event-time-two-phase",
                    "validates group by using session window with event time with two phase aggregation",
                    AggregatePhaseStrategy.TWO_PHASE.toString(),
                    "SESSION(TABLE window_source_t PARTITION BY name, DESCRIPTOR(rowtime), INTERVAL '5' SECOND)",
                    SESSION_EVENT_TIME_PARTITIONED_BEFORE_ROWS,
                    SESSION_EVENT_TIME_PARTITIONED_AFTER_ROWS);

    static final TableTestProgram SESSION_WINDOW_PARTITION_EVENT_TIME_TWO_PHASE_DISTINCT_SPLIT =
            getTableTestProgram(
                    "window-aggregate-session-partition-event-time-two-phase-distinct-split",
                    "validates group by using session window with event time with two phase aggregation",
                    AggregatePhaseStrategy.TWO_PHASE.toString(),
                    "SESSION(TABLE cdc_window_source_t PARTITION BY name, DESCRIPTOR(rowtime), INTERVAL '5' SECOND)",
                    SESSION_EVENT_TIME_PARTITIONED_BEFORE_ROWS,
                    SESSION_EVENT_TIME_PARTITIONED_AFTER_ROWS,
                    DistinctAggSplit.ENABLED);

    private static TableTestProgram getTableTestProgram(
            final String id,
            final String description,
            final String aggPhaseStrategy,
            final String windowSql,
            final String[] beforeRows,
            final String[] afterRows) {
        return getTableTestProgram(
                id,
                description,
                aggPhaseStrategy,
                windowSql,
                beforeRows,
                afterRows,
                DistinctAggSplit.DISABLED);
    }

    private static TableTestProgram getTableTestProgram(
            final String id,
            final String description,
            final String aggPhaseStrategy,
            final String windowSql,
            final String[] beforeRows,
            final String[] afterRows,
            final DistinctAggSplit enableDistinctAggSplit) {
        final String sql =
                String.format(
                        "INSERT INTO window_sink_t SELECT "
                                + "name, "
                                + "window_start, "
                                + "window_end, "
                                + "COUNT(*), "
                                + "SUM(a_int), "
                                + "COUNT(DISTINCT `comment`) "
                                + "FROM TABLE(%s)\n"
                                + "GROUP BY name, window_start, window_end",
                        windowSql);

        TableTestProgram.Builder builder = TableTestProgram.of(id, description);
        if (enableDistinctAggSplit == DistinctAggSplit.ENABLED) {
            builder.setupConfig(
                    OptimizerConfigOptions.TABLE_OPTIMIZER_DISTINCT_AGG_SPLIT_ENABLED, true);
        }

        return builder.setupConfig(
                        OptimizerConfigOptions.TABLE_OPTIMIZER_AGG_PHASE_STRATEGY,
                        AggregatePhaseStrategy.valueOf(aggPhaseStrategy))
                .setupTableSource(SOURCE)
                .setupTableSource(CDC_SOURCE)
                .setupTableSink(
                        SinkTestStep.newBuilder("window_sink_t")
                                .addSchema(
                                        "name STRING",
                                        "window_start TIMESTAMP(3)",
                                        "window_end TIMESTAMP(3)",
                                        "cnt BIGINT",
                                        "sum_int INT",
                                        "distinct_cnt BIGINT")
                                .consumedBeforeRestore(beforeRows)
                                .consumedAfterRestore(afterRows)
                                .build())
                .runSql(sql)
                .build();
    }
}
