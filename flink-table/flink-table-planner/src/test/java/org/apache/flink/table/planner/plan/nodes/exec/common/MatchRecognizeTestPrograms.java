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

package org.apache.flink.table.planner.plan.nodes.exec.common;

import org.apache.flink.table.planner.plan.nodes.exec.stream.StreamExecMatch;
import org.apache.flink.table.test.program.SinkTestStep;
import org.apache.flink.table.test.program.SourceTestStep;
import org.apache.flink.table.test.program.TableTestProgram;
import org.apache.flink.types.Row;

/** {@link TableTestProgram} definitions for testing {@link StreamExecMatch}. */
public class MatchRecognizeTestPrograms {
    private static final Row[] SIMPLE_DATA = {
        Row.of(1L, "a"),
        Row.of(2L, "z"),
        Row.of(3L, "b"),
        Row.of(4L, "c"),
        Row.of(5L, "d"),
        Row.of(6L, "a"),
        Row.of(7L, "b"),
        Row.of(8L, "c"),
        Row.of(9L, "a"),
        Row.of(10L, "b")
    };

    private static final Row[] SIMPLE_DATA2 = {Row.of(11L, "c")};

    private static final Row[] COMPLEX_DATA = {
        Row.of("ACME", 1L, 19, 1),
        Row.of("BETA", 2L, 18, 1),
        Row.of("ACME", 3L, 17, 2),
        Row.of("ACME", 4L, 13, 3),
        Row.of("BETA", 5L, 16, 2),
        Row.of("ACME", 6L, 20, 4)
    };

    private static final Row[] COMPLEX_DATA2 = {Row.of("BETA", 7L, 22, 4)};

    public static final TableTestProgram MATCH_SIMPLE =
            TableTestProgram.of("match-simple", "simple match recognize test")
                    .setupTableSource(
                            SourceTestStep.newBuilder("MyTable")
                                    .addSchema(
                                            "id bigint", "name varchar", "proctime as PROCTIME()")
                                    .producedBeforeRestore(SIMPLE_DATA)
                                    .producedAfterRestore(SIMPLE_DATA2)
                                    .build())
                    .setupTableSink(
                            SinkTestStep.newBuilder("MySink")
                                    .addSchema("a bigint", "b bigint", "c bigint")
                                    .consumedBeforeRestore(Row.of(6L, 7L, 8L))
                                    .consumedAfterRestore(Row.of(9L, 10L, 11L))
                                    .build())
                    .runSql(
                            "insert into MySink"
                                    + " SELECT T.aid, T.bid, T.cid\n"
                                    + "     FROM MyTable MATCH_RECOGNIZE (\n"
                                    + "             ORDER BY proctime\n"
                                    + "             MEASURES\n"
                                    + "             `A\"`.id AS aid,\n"
                                    + "             \u006C.id AS bid,\n"
                                    + "             C.id AS cid\n"
                                    + "             PATTERN (`A\"` \u006C C)\n"
                                    + "             DEFINE\n"
                                    + "                 `A\"` AS name = 'a',\n"
                                    + "                 \u006C AS name = 'b',\n"
                                    + "                 C AS name = 'c'\n"
                                    + "     ) AS T")
                    .build();

    public static final TableTestProgram MATCH_COMPLEX =
            TableTestProgram.of("match-complex", "complex match recognize test")
                    .setupTableSource(
                            SourceTestStep.newBuilder("MyTable")
                                    .addSchema(
                                            "symbol string",
                                            "tstamp bigint",
                                            "price int",
                                            "tax int",
                                            "proctime as PROCTIME()")
                                    .producedBeforeRestore(COMPLEX_DATA)
                                    .producedAfterRestore(COMPLEX_DATA2)
                                    .build())
                    .setupTableSink(
                            SinkTestStep.newBuilder("MySink")
                                    .addSchema(
                                            "somestring string", "a bigint", "b bigint", "c bigint")
                                    .consumedBeforeRestore(Row.of("ACME", 19L, 13L, null))
                                    .consumedAfterRestore(Row.of("BETA", 18L, 16L, null))
                                    .build())
                    .runSql(
                            "insert into MySink SELECT * FROM MyTable MATCH_RECOGNIZE (\n"
                                    + "  PARTITION BY symbol\n"
                                    + "  ORDER BY proctime\n"
                                    + "  MEASURES\n"
                                    + "    FIRST(DOWN.price) as first,\n"
                                    + "    LAST(DOWN.price) as last,\n"
                                    + "    FIRST(DOWN.price, 5) as nullPrice\n"
                                    + "  ONE ROW PER MATCH\n"
                                    + "  AFTER MATCH SKIP PAST LAST ROW\n"
                                    + "  PATTERN (DOWN{2,} UP)\n"
                                    + "  DEFINE\n"
                                    + "    DOWN AS price < LAST(DOWN.price, 1) OR LAST(DOWN.price, 1) IS NULL,\n"
                                    + "    UP AS price > LAST(DOWN.price)\n"
                                    + ") AS T")
                    .build();

    private static final Row[] BEFORE_DATA = {
        Row.of("2020-10-10 00:00:01", 9, 1),
        Row.of("2020-10-10 00:00:01", 8, 2),
        Row.of("2020-10-10 00:00:01", 10, 3),
        Row.of("2020-10-10 00:00:04", 7, 4),
        Row.of("2020-10-10 00:00:06", 5, 6),
        Row.of("2020-10-10 00:00:07", 8, 5),
        Row.of("2020-10-10 00:00:12", 3, 7),
        Row.of("2020-10-10 00:00:16", 4, 9),
        Row.of("2020-10-10 00:00:32", 7, 10),
        Row.of("2020-10-10 00:00:33", 9, 12),
        Row.of("2020-10-10 00:00:34", 5, 11)
    };

    private static final Row[] AFTER_DATA = {
        Row.of("2020-10-10 00:00:41", 3, 13),
        Row.of("2020-10-10 00:00:42", 11, 16),
        Row.of("2020-10-10 00:00:43", 12, 15),
        Row.of("2020-10-10 00:00:44", 13, 14)
    };

    private static final Row[] BEFORE_DATA_WITH_OUT_OF_ORDER_DATA = {
        Row.of("2020-10-10 00:00:01", 10, 3),
        Row.of("2020-10-10 00:00:01", 8, 2),
        Row.of("2020-10-10 00:00:01", 9, 1),
        Row.of("2020-10-10 00:00:04", 7, 4),
        Row.of("2020-10-10 00:00:07", 8, 5),
        // out of order - should be processed with a 2-second watermark in use.
        Row.of("2020-10-10 00:00:06", 5, 6),
        Row.of("2020-10-10 00:00:12", 3, 7),
        // late event - should be ignored with a 2-second watermark in use.
        Row.of("2020-10-10 00:00:08", 4, 8),
        Row.of("2020-10-10 00:00:16", 4, 9),
        Row.of("2020-10-10 00:00:32", 7, 10),
        Row.of("2020-10-10 00:00:34", 5, 11)
    };

    private static final Row[] AFTER_DATA_WITH_OUT_OF_ORDER_DATA = {
        Row.of("2020-10-10 00:00:33", 9, 12),
        Row.of("2020-10-10 00:00:41", 3, 13),
        Row.of("2020-10-10 00:00:42", 11, 16),
        Row.of("2020-10-10 00:00:43", 12, 15),
        Row.of("2020-10-10 00:00:44", 13, 14)
    };

    private static final SourceTestStep SOURCE =
            SourceTestStep.newBuilder("MyEventTimeTable")
                    .addSchema(
                            "ts STRING",
                            "price INT",
                            "sequence_num INT",
                            "`rowtime` AS TO_TIMESTAMP(`ts`)",
                            "`proctime` AS PROCTIME()",
                            "WATERMARK for `rowtime` AS `rowtime` - INTERVAL '2' SECOND")
                    .producedBeforeRestore(BEFORE_DATA)
                    .producedAfterRestore(AFTER_DATA)
                    .build();

    public static final TableTestProgram MATCH_ORDER_BY_EVENT_TIME =
            TableTestProgram.of("match-order-by-event-time", "complex match recognize test")
                    .setupTableSource(SOURCE)
                    .setupTableSink(
                            SinkTestStep.newBuilder("MySink")
                                    .addSchema("first bigint", "last bigint", "up bigint")
                                    .consumedBeforeRestore(Row.of(9L, 8L, 10L), Row.of(7L, 5L, 8L))
                                    .consumedAfterRestore(Row.of(9L, 3L, 11L))
                                    .build())
                    .runSql(getEventTimeSql("ORDER BY rowtime"))
                    .build();

    public static final TableTestProgram MATCH_ORDER_BY_INT_COLUMN =
            TableTestProgram.of("match-order-by-int-column", "complex match recognize test")
                    .setupTableSource(SOURCE)
                    .setupTableSink(
                            SinkTestStep.newBuilder("MySink")
                                    .addSchema("first bigint", "last bigint", "up bigint")
                                    .consumedBeforeRestore(Row.of(9L, 8L, 10L), Row.of(7L, 5L, 8L))
                                    .consumedAfterRestore(Row.of(9L, 3L, 11L))
                                    .build())
                    .runSql(getEventTimeSql("ORDER BY rowtime, sequence_num"))
                    .build();

    private static final SourceTestStep SOURCE_WITH_OUT_OF_ORDER_DATA =
            SourceTestStep.newBuilder("MyEventTimeTable")
                    .addSchema(
                            "ts STRING",
                            "price INT",
                            "sequence_num INT",
                            "`rowtime` AS TO_TIMESTAMP(`ts`)",
                            "`proctime` AS PROCTIME()",
                            "WATERMARK for `rowtime` AS `rowtime` - INTERVAL '2' SECOND")
                    .producedBeforeRestore(BEFORE_DATA_WITH_OUT_OF_ORDER_DATA)
                    .producedAfterRestore(AFTER_DATA_WITH_OUT_OF_ORDER_DATA)
                    .build();

    public static final TableTestProgram MATCH_ORDER_BY_EVENT_TIME_WITH_OUT_OF_ORDER_DATA =
            TableTestProgram.of(
                            "match-order-by-event-time-with-out-of-order-data",
                            "complex match recognize test")
                    .setupTableSource(SOURCE_WITH_OUT_OF_ORDER_DATA)
                    .setupTableSink(
                            SinkTestStep.newBuilder("MySink")
                                    .addSchema("first bigint", "last bigint", "up bigint")
                                    .consumedBeforeRestore(Row.of(10L, 8L, 9L), Row.of(7L, 5L, 8L))
                                    .consumedAfterRestore(Row.of(9L, 3L, 11L))
                                    .build())
                    .runSql(getEventTimeSql("ORDER BY rowtime"))
                    .build();

    public static final TableTestProgram MATCH_ORDER_BY_INT_COLUMN_WITH_OUT_OF_ORDER_DATA =
            TableTestProgram.of(
                            "match-order-by-int-column-with-out-of-order-data",
                            "complex match recognize test")
                    .setupTableSource(SOURCE_WITH_OUT_OF_ORDER_DATA)
                    .setupTableSink(
                            SinkTestStep.newBuilder("MySink")
                                    .addSchema("first bigint", "last bigint", "up bigint")
                                    .consumedBeforeRestore(Row.of(9L, 8L, 10L), Row.of(7L, 5L, 8L))
                                    .consumedAfterRestore(Row.of(9L, 3L, 11L))
                                    .build())
                    .runSql(getEventTimeSql("ORDER BY rowtime, sequence_num"))
                    .build();

    private static String getEventTimeSql(final String orderByClause) {
        final String sql =
                "insert into MySink SELECT * FROM MyEventTimeTable MATCH_RECOGNIZE (\n"
                        + "   %s\n"
                        + "  MEASURES\n"
                        + "    FIRST(DOWN.price) as first,\n"
                        + "    LAST(DOWN.price) as last,\n"
                        + "    UP.price as up\n"
                        + "  ONE ROW PER MATCH\n"
                        + "  AFTER MATCH SKIP PAST LAST ROW\n"
                        + "  PATTERN (DOWN{2,} UP)\n"
                        + "  DEFINE\n"
                        + "    DOWN AS price < LAST(DOWN.price, 1) OR LAST(DOWN.price, 1) IS NULL,\n"
                        + "    UP AS price > LAST(DOWN.price)\n"
                        + ") AS T";
        return String.format(sql, orderByClause);
    }

    public static final TableTestProgram MATCH_SKIP_TO_FIRST =
            getSkipTestProgram(
                    "match-skip-to-first",
                    "skip to first match recognize test",
                    "AFTER MATCH SKIP TO FIRST B",
                    new Row[] {Row.of(1L, 100, 106), Row.of(1L, 105, 107), Row.of(1L, 101, 101)},
                    new Row[] {Row.of(1L, 100, 111)});

    public static final TableTestProgram MATCH_SKIP_TO_LAST =
            getSkipTestProgram(
                    "match-skip-to-last",
                    "skip to last match recognize test",
                    "AFTER MATCH SKIP TO LAST B",
                    new Row[] {Row.of(1L, 100, 106), Row.of(1L, 105, 107), Row.of(1L, 101, 101)},
                    new Row[] {Row.of(1L, 100, 111)});

    public static final TableTestProgram MATCH_SKIP_TO_NEXT_ROW =
            getSkipTestProgram(
                    "match-skip-to-next-row",
                    "skip to next row match recognize test",
                    "AFTER MATCH SKIP TO NEXT ROW",
                    new Row[] {
                        Row.of(1L, 100, 106),
                        Row.of(1L, 102, 106),
                        Row.of(1L, 104, 106),
                        Row.of(1L, 106, 106),
                        Row.of(1L, 105, 107),
                        Row.of(1L, 107, 107),
                        Row.of(1L, 101, 101)
                    },
                    new Row[] {Row.of(1L, 100, 111), Row.of(1L, 110, 111), Row.of(1L, 111, 111)});

    public static final TableTestProgram MATCH_SKIP_PAST_LAST_ROW =
            getSkipTestProgram(
                    "match-skip-past-last-row",
                    "skip past last row match recognize test",
                    "AFTER MATCH SKIP PAST LAST ROW",
                    new Row[] {Row.of(1L, 100, 106), Row.of(1L, 107, 107)},
                    new Row[] {Row.of(1L, 100, 111)});

    private static TableTestProgram getSkipTestProgram(
            final String name,
            final String description,
            final String skipClause,
            final Row[] beforeRows,
            final Row[] afterRows) {
        return TableTestProgram.of(name, description)
                .setupTableSource(
                        SourceTestStep.newBuilder("MyTable")
                                .addSchema(
                                        "  vehicle_id bigint,\n"
                                                + "  engine_temperature int,\n"
                                                + "  proctime as PROCTIME()")
                                .producedBeforeRestore(
                                        Row.of(1L, 100),
                                        Row.of(1L, 102),
                                        Row.of(1L, 104),
                                        Row.of(1L, 106),
                                        Row.of(1L, 105),
                                        Row.of(1L, 107),
                                        Row.of(1L, 101),
                                        Row.of(1L, 100))
                                .producedAfterRestore(
                                        Row.of(1L, 110), Row.of(1L, 111), Row.of(1L, 99))
                                .build())
                .setupTableSink(
                        SinkTestStep.newBuilder("MySink")
                                .addSchema(
                                        "  vehicle_id bigint,\n"
                                                + "  Initial_Temp int,\n"
                                                + "  Final_Temp int\n")
                                .consumedBeforeRestore(beforeRows)
                                .consumedAfterRestore(afterRows)
                                .build())
                .runSql(getSql(skipClause))
                .build();
    }

    private static String getSql(final String afterClause) {
        final String sql =
                "insert into MySink"
                        + " SELECT * FROM\n"
                        + " MyTable\n"
                        + "   MATCH_RECOGNIZE(\n"
                        + "   PARTITION BY vehicle_id\n"
                        + "   ORDER BY `proctime`\n"
                        + "   MEASURES \n"
                        + "       FIRST(A.engine_temperature) as Initial_Temp,\n"
                        + "       LAST(A.engine_temperature) as Final_Temp\n"
                        + "   ONE ROW PER MATCH\n"
                        + "   %s\n"
                        + "   PATTERN (A+ B)\n"
                        + "   DEFINE\n"
                        + "       A as LAST(A.engine_temperature,1) is NULL OR A.engine_temperature > LAST(A.engine_temperature,1),\n"
                        + "       B as B.engine_temperature < LAST(A.engine_temperature)\n"
                        + "   )MR;";
        return String.format(sql, afterClause);
    }
}
