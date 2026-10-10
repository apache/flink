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

/**
 * Semantic {@link TableTestProgram} definitions for a window aggregate placed on top of a {@link
 * StreamExecWindowJoin}.
 */
public class WindowJoinSemanticTestPrograms {

    // window W1 = [00:00, 00:15), two left rows, one right row -> matched on both sides
    // window W2 = [00:15, 00:30), two left rows, no right rows  -> left-only
    // window W3 = [00:30, 00:45), no left rows, two right rows  -> right-only
    private static final Row[] SOURCE_ONE_DATA = {
        Row.of("2020-10-10 00:01:00"),
        Row.of("2020-10-10 00:02:00"),
        Row.of("2020-10-10 00:16:00"),
        Row.of("2020-10-10 00:17:00")
    };

    private static final Row[] SOURCE_TWO_DATA = {
        Row.of("2020-10-10 00:03:00"), Row.of("2020-10-10 00:31:00"), Row.of("2020-10-10 00:32:00")
    };

    private static final String[] SOURCE_SCHEMA = {
        "ts STRING",
        "rowtime AS TO_TIMESTAMP(`ts`)",
        "WATERMARK for `rowtime` AS `rowtime` - INTERVAL '1' SECOND"
    };

    private static final String[] SINK_SCHEMA = {
        "window_start TIMESTAMP(3)", "window_end TIMESTAMP(3)", "cnt BIGINT"
    };

    private static final SourceTestStep SOURCE_ONE =
            SourceTestStep.newBuilder("source_one_t")
                    .addSchema(SOURCE_SCHEMA)
                    .producedValues(SOURCE_ONE_DATA)
                    .build();

    private static final SourceTestStep SOURCE_TWO =
            SourceTestStep.newBuilder("source_two_t")
                    .addSchema(SOURCE_SCHEMA)
                    .producedValues(SOURCE_TWO_DATA)
                    .build();

    private static String windowAggregateOnWindowJoin(String joinType, String windowSide) {
        return String.format(
                "INSERT INTO sink_t\n"
                        + "SELECT %1$s.window_start, %1$s.window_end, COUNT(*) AS cnt\n"
                        + "FROM TUMBLE(TABLE source_one_t, DESCRIPTOR(rowtime), INTERVAL '15' MINUTE) t_left\n"
                        + "%2$s TUMBLE(TABLE source_two_t, DESCRIPTOR(rowtime), INTERVAL '15' MINUTE) t_right\n"
                        + "ON t_left.window_start = t_right.window_start"
                        + " AND t_left.window_end = t_right.window_end\n"
                        + "GROUP BY %1$s.window_start, %1$s.window_end",
                windowSide, joinType);
    }

    public static final TableTestProgram WINDOW_AGGREGATE_ON_INNER_WINDOW_JOIN =
            TableTestProgram.of(
                            "window-aggregate-on-inner-window-join",
                            "validates a window aggregate on top of an inner window join keeps the"
                                    + " window properties, so no row carries null window columns")
                    .setupTableSource(SOURCE_ONE)
                    .setupTableSource(SOURCE_TWO)
                    .setupTableSink(
                            SinkTestStep.newBuilder("sink_t")
                                    .addSchema(SINK_SCHEMA)
                                    .testMaterializedData()
                                    .consumedValues("+I[2020-10-10T00:00, 2020-10-10T00:15, 2]")
                                    .build())
                    .runSql(windowAggregateOnWindowJoin("JOIN", "t_right"))
                    .build();

    public static final TableTestProgram WINDOW_AGGREGATE_ON_LEFT_WINDOW_JOIN =
            TableTestProgram.of(
                            "window-aggregate-on-left-window-join",
                            "validates a window aggregate on top of a left window join groups the"
                                    + " unmatched rows whose (right-side) window columns are null")
                    .setupTableSource(SOURCE_ONE)
                    .setupTableSource(SOURCE_TWO)
                    .setupTableSink(
                            SinkTestStep.newBuilder("sink_t")
                                    .addSchema(SINK_SCHEMA)
                                    .testMaterializedData()
                                    .consumedValues(
                                            "+I[2020-10-10T00:00, 2020-10-10T00:15, 2]",
                                            "+I[null, null, 2]")
                                    .build())
                    .runSql(windowAggregateOnWindowJoin("LEFT JOIN", "t_right"))
                    .build();

    public static final TableTestProgram WINDOW_AGGREGATE_ON_RIGHT_WINDOW_JOIN =
            TableTestProgram.of(
                            "window-aggregate-on-right-window-join",
                            "validates a window aggregate on top of a right window join groups the"
                                    + " unmatched rows whose (left-side) window columns are null")
                    .setupTableSource(SOURCE_ONE)
                    .setupTableSource(SOURCE_TWO)
                    .setupTableSink(
                            SinkTestStep.newBuilder("sink_t")
                                    .addSchema(SINK_SCHEMA)
                                    .testMaterializedData()
                                    .consumedValues(
                                            "+I[2020-10-10T00:00, 2020-10-10T00:15, 2]",
                                            "+I[null, null, 2]")
                                    .build())
                    .runSql(windowAggregateOnWindowJoin("RIGHT JOIN", "t_left"))
                    .build();

    public static final TableTestProgram WINDOW_AGGREGATE_ON_FULL_WINDOW_JOIN =
            TableTestProgram.of(
                            "window-aggregate-on-full-window-join",
                            "validates a window aggregate on top of a full window join groups the"
                                    + " unmatched rows whose window columns are null")
                    .setupTableSource(SOURCE_ONE)
                    .setupTableSource(SOURCE_TWO)
                    .setupTableSink(
                            SinkTestStep.newBuilder("sink_t")
                                    .addSchema(SINK_SCHEMA)
                                    .testMaterializedData()
                                    .consumedValues(
                                            "+I[2020-10-10T00:00, 2020-10-10T00:15, 2]",
                                            "+I[2020-10-10T00:30, 2020-10-10T00:45, 2]",
                                            "+I[null, null, 2]")
                                    .build())
                    .runSql(windowAggregateOnWindowJoin("FULL JOIN", "t_right"))
                    .build();
}
