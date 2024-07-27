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

/** {@link TableTestProgram} definitions for testing {@link StreamExecWindowJoin}. */
public class WindowJoinTestPrograms {

    static final Row[] SRC_ONE_BEFORE_DATA = {
        Row.of("2020-10-10 00:00:01", "L1", 1, "a"),
        Row.of("2020-10-10 00:00:02", "L2", 2, "c"),
        Row.of("2020-10-10 00:00:03", "L3", 2, "x"),
        Row.of("2020-10-10 00:00:04", "L7", 5, "a"),
        Row.of("2020-10-10 00:00:07", "L8", 3, "b"),
        // out of order
        Row.of("2020-10-10 00:00:06", "L6", 6, "b"),
        Row.of("2020-10-10 00:00:08", "L9", 3, "a"),
        // late event
        Row.of("2020-10-10 00:00:04", "L4", 5, "a"),
        Row.of("2020-10-10 00:00:16", "L10", 4, "b"),
        Row.of("2020-10-10 00:00:32", "L11", 7, null),
        Row.of("2020-10-10 00:00:34", "L12", 1, "b")
    };

    static final Row[] SRC_TWO_BEFORE_DATA = {
        Row.of("2020-10-10 00:00:01", "R1", 5, "a"),
        Row.of("2020-10-10 00:00:02", "R2", 7, "b"),
        Row.of("2020-10-10 00:00:03", "R3", 7, "f"),
        Row.of("2020-10-10 00:00:04", "R7", 3, "g"),
        Row.of("2020-10-10 00:00:07", "R8", 8, "b"),
        // out of order
        Row.of("2020-10-10 00:00:06", "R6", 6, "b"),
        Row.of("2020-10-10 00:00:08", "R9", 3, "a"),
        // late event
        Row.of("2020-10-10 00:00:04", "R4", 5, "a"),
        Row.of("2020-10-10 00:00:17", "R17", 4, "k"),
        Row.of("2020-10-10 00:00:31", "R31", 7, null),
        Row.of("2020-10-10 00:00:33", "R33", 1, "b")
    };

    static final Row[] SRC_ONE_AFTER_DATA = {
        Row.of("2020-10-10 00:00:41", "L41", 10, "a"),
        Row.of("2020-10-10 00:00:42", "L42", 11, "d"),
        Row.of("2020-10-10 00:00:43", "L43", 12, "c"),
        Row.of("2020-10-10 00:00:44", "L44", 13, "d")
    };

    static final Row[] SRC_TWO_AFTER_DATA = {
        Row.of("2020-10-10 00:00:41", "R41", 10, "y"),
        Row.of("2020-10-10 00:00:42", "R42", 11, "c"),
        Row.of("2020-10-10 00:00:43", "R43", 12, "x"),
        Row.of("2020-10-10 00:00:44", "R44", 13, "z")
    };

    static final String[] SOURCE_SCHEMA = {
        "ts STRING",
        "id STRING",
        "num INT",
        "name STRING",
        "row_time AS TO_TIMESTAMP(`ts`)",
        "WATERMARK for `row_time` AS `row_time` - INTERVAL '1' SECOND"
    };

    static final String[] SINK_SCHEMA = {
        "window_start TIMESTAMP(3)",
        "window_end TIMESTAMP(3)",
        "name STRING",
        "L_id STRING",
        "L_num INT",
        "R_id STRING",
        "R_num INT"
    };

    static final String TUMBLE_TVF =
            "TABLE(TUMBLE(TABLE %s, DESCRIPTOR(row_time), INTERVAL '5' SECOND))";

    static final String HOP_TVF =
            "TABLE(HOP(TABLE %s, DESCRIPTOR(row_time), INTERVAL '5' SECOND, INTERVAL '10' SECOND))";

    static final String CUMULATIVE_TVF =
            "TABLE(CUMULATE(TABLE %s, DESCRIPTOR(row_time), INTERVAL '5' SECOND, INTERVAL '10' SECOND))";

    static final String QUERY =
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
                    + "    SELECT * FROM %s\n" // WINDOW TVF
                    + ") L\n"
                    + "%s\n" // JOIN TYPE
                    + "(\n"
                    + "    SELECT * FROM %s\n" // WINDOW TVF
                    + ") R\n"
                    + "%s L.name = R.name\n" // JOIN CONDITION
                    + "AND L.window_start = R.window_start\n"
                    + "AND L.window_end = R.window_end";

    static final SourceTestStep SOURCE_ONE =
            SourceTestStep.newBuilder("source_one_t")
                    .addSchema(SOURCE_SCHEMA)
                    .producedBeforeRestore(SRC_ONE_BEFORE_DATA)
                    .producedAfterRestore(SRC_ONE_AFTER_DATA)
                    .build();

    static final SourceTestStep SOURCE_TWO =
            SourceTestStep.newBuilder("source_two_t")
                    .addSchema(SOURCE_SCHEMA)
                    .producedBeforeRestore(SRC_TWO_BEFORE_DATA)
                    .producedAfterRestore(SRC_TWO_AFTER_DATA)
                    .build();

    static final TableTestProgram WINDOW_JOIN_INNER_TUMBLE_EVENT_TIME =
            TableTestProgram.of(
                            "window-join-inner-tumble-event-time",
                            "validates window inner join using tumbling window with event time")
                    .setupTableSource(SOURCE_ONE)
                    .setupTableSource(SOURCE_TWO)
                    .setupTableSink(
                            SinkTestStep.newBuilder("sink_t")
                                    .addSchema(SINK_SCHEMA)
                                    .consumedBeforeRestore(
                                            "+I[2020-10-10T00:00, 2020-10-10T00:00:05, a, L1, 1, R1, 5]",
                                            "+I[2020-10-10T00:00, 2020-10-10T00:00:05, a, L7, 5, R1, 5]",
                                            "+I[2020-10-10T00:00:05, 2020-10-10T00:00:10, b, L8, 3, R8, 8]",
                                            "+I[2020-10-10T00:00:05, 2020-10-10T00:00:10, b, L8, 3, R6, 6]",
                                            "+I[2020-10-10T00:00:05, 2020-10-10T00:00:10, b, L6, 6, R8, 8]",
                                            "+I[2020-10-10T00:00:05, 2020-10-10T00:00:10, b, L6, 6, R6, 6]",
                                            "+I[2020-10-10T00:00:05, 2020-10-10T00:00:10, a, L9, 3, R9, 3]")
                                    .consumedAfterRestore(
                                            "+I[2020-10-10T00:00:30, 2020-10-10T00:00:35, b, L12, 1, R33, 1]",
                                            "+I[2020-10-10T00:00:40, 2020-10-10T00:00:45, c, L43, 12, R42, 11]")
                                    .build())
                    .runSql(
                            String.format(
                                    QUERY,
                                    String.format(TUMBLE_TVF, "source_one_t"),
                                    "JOIN",
                                    String.format(TUMBLE_TVF, "source_two_t"),
                                    "ON"))
                    .build();

    static final TableTestProgram WINDOW_JOIN_LEFT_TUMBLE_EVENT_TIME =
            TableTestProgram.of(
                            "window-join-left-tumble-event-time",
                            "validates window left join using tumbling window with event time")
                    .setupTableSource(SOURCE_ONE)
                    .setupTableSource(SOURCE_TWO)
                    .setupTableSink(
                            SinkTestStep.newBuilder("sink_t")
                                    .addSchema(SINK_SCHEMA)
                                    .consumedBeforeRestore(
                                            "+I[2020-10-10T00:00, 2020-10-10T00:00:05, a, L1, 1, R1, 5]",
                                            "+I[2020-10-10T00:00, 2020-10-10T00:00:05, a, L7, 5, R1, 5]",
                                            "+I[2020-10-10T00:00, 2020-10-10T00:00:05, x, L3, 2, null, null]",
                                            "+I[2020-10-10T00:00, 2020-10-10T00:00:05, c, L2, 2, null, null]",
                                            "+I[2020-10-10T00:00:05, 2020-10-10T00:00:10, b, L8, 3, R8, 8]",
                                            "+I[2020-10-10T00:00:05, 2020-10-10T00:00:10, b, L8, 3, R6, 6]",
                                            "+I[2020-10-10T00:00:05, 2020-10-10T00:00:10, b, L6, 6, R8, 8]",
                                            "+I[2020-10-10T00:00:05, 2020-10-10T00:00:10, b, L6, 6, R6, 6]",
                                            "+I[2020-10-10T00:00:05, 2020-10-10T00:00:10, a, L9, 3, R9, 3]",
                                            "+I[2020-10-10T00:00:15, 2020-10-10T00:00:20, b, L10, 4, null, null]")
                                    .consumedAfterRestore(
                                            "+I[2020-10-10T00:00:30, 2020-10-10T00:00:35, b, L12, 1, R33, 1]",
                                            "+I[2020-10-10T00:00:30, 2020-10-10T00:00:35, null, L11, 7, null, null]",
                                            "+I[2020-10-10T00:00:40, 2020-10-10T00:00:45, a, L41, 10, null, null]",
                                            "+I[2020-10-10T00:00:40, 2020-10-10T00:00:45, d, L42, 11, null, null]",
                                            "+I[2020-10-10T00:00:40, 2020-10-10T00:00:45, d, L44, 13, null, null]",
                                            "+I[2020-10-10T00:00:40, 2020-10-10T00:00:45, c, L43, 12, R42, 11]")
                                    .build())
                    .runSql(
                            String.format(
                                    QUERY,
                                    String.format(TUMBLE_TVF, "source_one_t"),
                                    "LEFT JOIN",
                                    String.format(TUMBLE_TVF, "source_two_t"),
                                    "ON"))
                    .build();

    static final TableTestProgram WINDOW_JOIN_RIGHT_TUMBLE_EVENT_TIME =
            TableTestProgram.of(
                            "window-join-right-tumble-event-time",
                            "validates window right join using tumbling window with event time")
                    .setupTableSource(SOURCE_ONE)
                    .setupTableSource(SOURCE_TWO)
                    .setupTableSink(
                            SinkTestStep.newBuilder("sink_t")
                                    .addSchema(SINK_SCHEMA)
                                    .consumedBeforeRestore(
                                            "+I[2020-10-10T00:00, 2020-10-10T00:00:05, a, L1, 1, R1, 5]",
                                            "+I[2020-10-10T00:00, 2020-10-10T00:00:05, a, L7, 5, R1, 5]",
                                            "+I[null, null, null, null, null, R7, 3]",
                                            "+I[null, null, null, null, null, R2, 7]",
                                            "+I[null, null, null, null, null, R3, 7]",
                                            "+I[2020-10-10T00:00:05, 2020-10-10T00:00:10, b, L8, 3, R8, 8]",
                                            "+I[2020-10-10T00:00:05, 2020-10-10T00:00:10, b, L6, 6, R8, 8]",
                                            "+I[2020-10-10T00:00:05, 2020-10-10T00:00:10, b, L8, 3, R6, 6]",
                                            "+I[2020-10-10T00:00:05, 2020-10-10T00:00:10, b, L6, 6, R6, 6]",
                                            "+I[2020-10-10T00:00:05, 2020-10-10T00:00:10, a, L9, 3, R9, 3]",
                                            "+I[null, null, null, null, null, R17, 4]")
                                    .consumedAfterRestore(
                                            "+I[2020-10-10T00:00:30, 2020-10-10T00:00:35, b, L12, 1, R33, 1]",
                                            "+I[null, null, null, null, null, R31, 7]",
                                            "+I[null, null, null, null, null, R43, 12]",
                                            "+I[null, null, null, null, null, R44, 13]",
                                            "+I[null, null, null, null, null, R41, 10]",
                                            "+I[2020-10-10T00:00:40, 2020-10-10T00:00:45, c, L43, 12, R42, 11]")
                                    .build())
                    .runSql(
                            String.format(
                                    QUERY,
                                    String.format(TUMBLE_TVF, "source_one_t"),
                                    "RIGHT JOIN",
                                    String.format(TUMBLE_TVF, "source_two_t"),
                                    "ON"))
                    .build();

    static final TableTestProgram WINDOW_JOIN_FULL_OUTER_TUMBLE_EVENT_TIME =
            TableTestProgram.of(
                            "window-join-full-outer-tumble-event-time",
                            "validates window full outer join using tumbling window with event time")
                    .setupTableSource(SOURCE_ONE)
                    .setupTableSource(SOURCE_TWO)
                    .setupTableSink(
                            SinkTestStep.newBuilder("sink_t")
                                    .addSchema(SINK_SCHEMA)
                                    .consumedBeforeRestore(
                                            "+I[2020-10-10T00:00, 2020-10-10T00:00:05, a, L1, 1, R1, 5]",
                                            "+I[2020-10-10T00:00, 2020-10-10T00:00:05, a, L7, 5, R1, 5]",
                                            "+I[2020-10-10T00:00, 2020-10-10T00:00:05, x, L3, 2, null, null]",
                                            "+I[null, null, null, null, null, R7, 3]",
                                            "+I[2020-10-10T00:00, 2020-10-10T00:00:05, c, L2, 2, null, null]",
                                            "+I[null, null, null, null, null, R2, 7]",
                                            "+I[null, null, null, null, null, R3, 7]",
                                            "+I[2020-10-10T00:00:05, 2020-10-10T00:00:10, b, L8, 3, R8, 8]",
                                            "+I[2020-10-10T00:00:05, 2020-10-10T00:00:10, b, L8, 3, R6, 6]",
                                            "+I[2020-10-10T00:00:05, 2020-10-10T00:00:10, b, L6, 6, R8, 8]",
                                            "+I[2020-10-10T00:00:05, 2020-10-10T00:00:10, b, L6, 6, R6, 6]",
                                            "+I[2020-10-10T00:00:05, 2020-10-10T00:00:10, a, L9, 3, R9, 3]",
                                            "+I[2020-10-10T00:00:15, 2020-10-10T00:00:20, b, L10, 4, null, null]",
                                            "+I[null, null, null, null, null, R17, 4]")
                                    .consumedAfterRestore(
                                            "+I[2020-10-10T00:00:30, 2020-10-10T00:00:35, b, L12, 1, R33, 1]",
                                            "+I[2020-10-10T00:00:30, 2020-10-10T00:00:35, null, L11, 7, null, null]",
                                            "+I[null, null, null, null, null, R31, 7]",
                                            "+I[2020-10-10T00:00:40, 2020-10-10T00:00:45, a, L41, 10, null, null]",
                                            "+I[2020-10-10T00:00:40, 2020-10-10T00:00:45, d, L42, 11, null, null]",
                                            "+I[2020-10-10T00:00:40, 2020-10-10T00:00:45, d, L44, 13, null, null]",
                                            "+I[null, null, null, null, null, R43, 12]",
                                            "+I[null, null, null, null, null, R44, 13]",
                                            "+I[null, null, null, null, null, R41, 10]",
                                            "+I[2020-10-10T00:00:40, 2020-10-10T00:00:45, c, L43, 12, R42, 11]")
                                    .build())
                    .runSql(
                            String.format(
                                    QUERY,
                                    String.format(TUMBLE_TVF, "source_one_t"),
                                    "FULL OUTER JOIN",
                                    String.format(TUMBLE_TVF, "source_two_t"),
                                    "ON"))
                    .build();

    static final TableTestProgram WINDOW_JOIN_SEMI_TUMBLE_EVENT_TIME =
            TableTestProgram.of(
                            "window-join-semi-tumble-event-time",
                            "validates window semi join using tumbling window with event time")
                    .setupTableSource(SOURCE_ONE)
                    .setupTableSource(SOURCE_TWO)
                    .setupTableSink(
                            SinkTestStep.newBuilder("sink_t")
                                    .addSchema(SINK_SCHEMA)
                                    .consumedBeforeRestore(
                                            "+I[2020-10-10T00:00, 2020-10-10T00:00:05, a, L1, 1, NOT_PRESENT, 0]",
                                            "+I[2020-10-10T00:00, 2020-10-10T00:00:05, a, L7, 5, NOT_PRESENT, 0]",
                                            "+I[2020-10-10T00:00:05, 2020-10-10T00:00:10, b, L8, 3, NOT_PRESENT, 0]",
                                            "+I[2020-10-10T00:00:05, 2020-10-10T00:00:10, b, L6, 6, NOT_PRESENT, 0]",
                                            "+I[2020-10-10T00:00:05, 2020-10-10T00:00:10, a, L9, 3, NOT_PRESENT, 0]")
                                    .consumedAfterRestore(
                                            "+I[2020-10-10T00:00:30, 2020-10-10T00:00:35, b, L12, 1, NOT_PRESENT, 0]",
                                            "+I[2020-10-10T00:00:40, 2020-10-10T00:00:45, c, L43, 12, NOT_PRESENT, 0]")
                                    .build())
                    .runSql(
                            "INSERT INTO sink_t SELECT window_start, window_end, name, id, num, 'NOT_PRESENT', 0\n"
                                    + "           FROM (\n"
                                    + "               SELECT * FROM TABLE(TUMBLE(TABLE source_one_t, DESCRIPTOR(row_time), INTERVAL '5' SECOND))\n"
                                    + "           ) L WHERE EXISTS (\n"
                                    + "             SELECT * FROM (   \n"
                                    + "               SELECT * FROM TABLE(TUMBLE(TABLE source_two_t, DESCRIPTOR(row_time), INTERVAL '5' SECOND))\n"
                                    + "             ) R "
                                    + "             WHERE L.name = R.name AND L.window_start = R.window_start AND L.window_end = R.window_end)")
                    .build();

    static final TableTestProgram WINDOW_JOIN_ANTI_TUMBLE_EVENT_TIME =
            TableTestProgram.of(
                            "window-join-anti-tumble-event-time",
                            "validates window anti join using tumbling window with event time")
                    .setupTableSource(SOURCE_ONE)
                    .setupTableSource(SOURCE_TWO)
                    .setupTableSink(
                            SinkTestStep.newBuilder("sink_t")
                                    .addSchema(SINK_SCHEMA)
                                    .consumedBeforeRestore(
                                            "+I[2020-10-10T00:00, 2020-10-10T00:00:05, x, L3, 2, NOT_PRESENT, 0]",
                                            "+I[2020-10-10T00:00, 2020-10-10T00:00:05, c, L2, 2, NOT_PRESENT, 0]",
                                            "+I[2020-10-10T00:00:15, 2020-10-10T00:00:20, b, L10, 4, NOT_PRESENT, 0]")
                                    .consumedAfterRestore(
                                            "+I[2020-10-10T00:00:30, 2020-10-10T00:00:35, null, L11, 7, NOT_PRESENT, 0]",
                                            "+I[2020-10-10T00:00:40, 2020-10-10T00:00:45, a, L41, 10, NOT_PRESENT, 0]",
                                            "+I[2020-10-10T00:00:40, 2020-10-10T00:00:45, d, L42, 11, NOT_PRESENT, 0]",
                                            "+I[2020-10-10T00:00:40, 2020-10-10T00:00:45, d, L44, 13, NOT_PRESENT, 0]")
                                    .build())
                    .runSql(
                            "INSERT INTO sink_t SELECT window_start, window_end, name, id, num, 'NOT_PRESENT', 0\n"
                                    + "           FROM (\n"
                                    + "               SELECT * FROM TABLE(TUMBLE(TABLE source_one_t, DESCRIPTOR(row_time), INTERVAL '5' SECOND))\n"
                                    + "           ) L WHERE NOT EXISTS (\n"
                                    + "             SELECT * FROM (   \n"
                                    + "               SELECT * FROM TABLE(TUMBLE(TABLE source_two_t, DESCRIPTOR(row_time), INTERVAL '5' SECOND))\n"
                                    + "             ) R "
                                    + "             WHERE L.name = R.name AND L.window_start = R.window_start AND L.window_end = R.window_end)")
                    .build();

    static final TableTestProgram WINDOW_JOIN_HOP_EVENT_TIME =
            TableTestProgram.of(
                            "window-join-hop-event-time",
                            "validates window join using hopping window with event time")
                    .setupTableSource(SOURCE_ONE)
                    .setupTableSource(SOURCE_TWO)
                    .setupTableSink(
                            SinkTestStep.newBuilder("sink_t")
                                    .addSchema(SINK_SCHEMA)
                                    .consumedBeforeRestore(
                                            "+I[2020-10-09T23:59:55, 2020-10-10T00:00:05, a, L1, 1, R1, 5]",
                                            "+I[2020-10-09T23:59:55, 2020-10-10T00:00:05, a, L7, 5, R1, 5]",
                                            "+I[2020-10-10T00:00, 2020-10-10T00:00:10, a, L1, 1, R1, 5]",
                                            "+I[2020-10-10T00:00, 2020-10-10T00:00:10, a, L1, 1, R9, 3]",
                                            "+I[2020-10-10T00:00, 2020-10-10T00:00:10, a, L1, 1, R4, 5]",
                                            "+I[2020-10-10T00:00, 2020-10-10T00:00:10, a, L7, 5, R1, 5]",
                                            "+I[2020-10-10T00:00, 2020-10-10T00:00:10, a, L7, 5, R9, 3]",
                                            "+I[2020-10-10T00:00, 2020-10-10T00:00:10, a, L7, 5, R4, 5]",
                                            "+I[2020-10-10T00:00, 2020-10-10T00:00:10, a, L9, 3, R1, 5]",
                                            "+I[2020-10-10T00:00, 2020-10-10T00:00:10, a, L9, 3, R9, 3]",
                                            "+I[2020-10-10T00:00, 2020-10-10T00:00:10, a, L9, 3, R4, 5]",
                                            "+I[2020-10-10T00:00, 2020-10-10T00:00:10, a, L4, 5, R1, 5]",
                                            "+I[2020-10-10T00:00, 2020-10-10T00:00:10, a, L4, 5, R9, 3]",
                                            "+I[2020-10-10T00:00, 2020-10-10T00:00:10, a, L4, 5, R4, 5]",
                                            "+I[2020-10-10T00:00, 2020-10-10T00:00:10, b, L8, 3, R2, 7]",
                                            "+I[2020-10-10T00:00, 2020-10-10T00:00:10, b, L8, 3, R8, 8]",
                                            "+I[2020-10-10T00:00, 2020-10-10T00:00:10, b, L8, 3, R6, 6]",
                                            "+I[2020-10-10T00:00, 2020-10-10T00:00:10, b, L6, 6, R2, 7]",
                                            "+I[2020-10-10T00:00, 2020-10-10T00:00:10, b, L6, 6, R8, 8]",
                                            "+I[2020-10-10T00:00, 2020-10-10T00:00:10, b, L6, 6, R6, 6]",
                                            "+I[2020-10-10T00:00:05, 2020-10-10T00:00:15, a, L9, 3, R9, 3]",
                                            "+I[2020-10-10T00:00:05, 2020-10-10T00:00:15, b, L8, 3, R8, 8]",
                                            "+I[2020-10-10T00:00:05, 2020-10-10T00:00:15, b, L8, 3, R6, 6]",
                                            "+I[2020-10-10T00:00:05, 2020-10-10T00:00:15, b, L6, 6, R8, 8]",
                                            "+I[2020-10-10T00:00:05, 2020-10-10T00:00:15, b, L6, 6, R6, 6]")
                                    .consumedAfterRestore(
                                            "+I[2020-10-10T00:00:25, 2020-10-10T00:00:35, b, L12, 1, R33, 1]",
                                            "+I[2020-10-10T00:00:30, 2020-10-10T00:00:40, b, L12, 1, R33, 1]",
                                            "+I[2020-10-10T00:00:35, 2020-10-10T00:00:45, c, L43, 12, R42, 11]",
                                            "+I[2020-10-10T00:00:40, 2020-10-10T00:00:50, c, L43, 12, R42, 11]")
                                    .build())
                    .runSql(
                            String.format(
                                    QUERY,
                                    String.format(HOP_TVF, "source_one_t"),
                                    "JOIN",
                                    String.format(HOP_TVF, "source_two_t"),
                                    "ON"))
                    .build();

    static final TableTestProgram WINDOW_JOIN_CUMULATE_EVENT_TIME =
            TableTestProgram.of(
                            "window-join-cumulate-event-time",
                            "validates window join using cumulate window with event time")
                    .setupTableSource(SOURCE_ONE)
                    .setupTableSource(SOURCE_TWO)
                    .setupTableSink(
                            SinkTestStep.newBuilder("sink_t")
                                    .addSchema(SINK_SCHEMA)
                                    .consumedBeforeRestore(
                                            "+I[2020-10-10T00:00, 2020-10-10T00:00:05, a, L1, 1, R1, 5]",
                                            "+I[2020-10-10T00:00, 2020-10-10T00:00:05, a, L7, 5, R1, 5]",
                                            "+I[2020-10-10T00:00, 2020-10-10T00:00:10, a, L1, 1, R1, 5]",
                                            "+I[2020-10-10T00:00, 2020-10-10T00:00:10, a, L1, 1, R9, 3]",
                                            "+I[2020-10-10T00:00, 2020-10-10T00:00:10, a, L1, 1, R4, 5]",
                                            "+I[2020-10-10T00:00, 2020-10-10T00:00:10, a, L7, 5, R1, 5]",
                                            "+I[2020-10-10T00:00, 2020-10-10T00:00:10, a, L7, 5, R9, 3]",
                                            "+I[2020-10-10T00:00, 2020-10-10T00:00:10, a, L7, 5, R4, 5]",
                                            "+I[2020-10-10T00:00, 2020-10-10T00:00:10, a, L9, 3, R1, 5]",
                                            "+I[2020-10-10T00:00, 2020-10-10T00:00:10, a, L9, 3, R9, 3]",
                                            "+I[2020-10-10T00:00, 2020-10-10T00:00:10, a, L9, 3, R4, 5]",
                                            "+I[2020-10-10T00:00, 2020-10-10T00:00:10, a, L4, 5, R1, 5]",
                                            "+I[2020-10-10T00:00, 2020-10-10T00:00:10, a, L4, 5, R9, 3]",
                                            "+I[2020-10-10T00:00, 2020-10-10T00:00:10, a, L4, 5, R4, 5]",
                                            "+I[2020-10-10T00:00, 2020-10-10T00:00:10, b, L8, 3, R2, 7]",
                                            "+I[2020-10-10T00:00, 2020-10-10T00:00:10, b, L8, 3, R8, 8]",
                                            "+I[2020-10-10T00:00, 2020-10-10T00:00:10, b, L8, 3, R6, 6]",
                                            "+I[2020-10-10T00:00, 2020-10-10T00:00:10, b, L6, 6, R2, 7]",
                                            "+I[2020-10-10T00:00, 2020-10-10T00:00:10, b, L6, 6, R8, 8]",
                                            "+I[2020-10-10T00:00, 2020-10-10T00:00:10, b, L6, 6, R6, 6]")
                                    .consumedAfterRestore(
                                            "+I[2020-10-10T00:00:30, 2020-10-10T00:00:35, b, L12, 1, R33, 1]",
                                            "+I[2020-10-10T00:00:30, 2020-10-10T00:00:40, b, L12, 1, R33, 1]",
                                            "+I[2020-10-10T00:00:40, 2020-10-10T00:00:45, c, L43, 12, R42, 11]",
                                            "+I[2020-10-10T00:00:40, 2020-10-10T00:00:50, c, L43, 12, R42, 11]")
                                    .build())
                    .runSql(
                            String.format(
                                    QUERY,
                                    String.format(CUMULATIVE_TVF, "source_one_t"),
                                    "JOIN",
                                    String.format(CUMULATIVE_TVF, "source_two_t"),
                                    "ON"))
                    .build();
}
