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

package org.apache.flink.table.planner.runtime.stream.jsonplan;

import org.apache.flink.table.planner.factories.TestValuesTableFactory;
import org.apache.flink.table.planner.utils.JsonPlanTestBase;
import org.apache.flink.types.Row;

import org.junit.Test;

import java.util.Arrays;
import java.util.Collections;
import java.util.List;

/** Test json deserialization for match recognize. */
public class MatchRecognizeJsonPlanITCase extends JsonPlanTestBase {
    @Test
    public void testSimpleMatch() throws Exception {
        List<Row> data =
                Arrays.asList(
                        Row.of(1L, "a"),
                        Row.of(2L, "z"),
                        Row.of(3L, "b"),
                        Row.of(4L, "c"),
                        Row.of(5L, "d"),
                        Row.of(6L, "a"),
                        Row.of(7L, "b"),
                        Row.of(8L, "c"),
                        Row.of(9L, "h"));

        createTestValuesSourceTable(
                "MyTable", data, "id bigint", "name varchar", "proctime as PROCTIME()");
        createTestValuesSinkTable("MySink", "a bigint", "b bigint", "c bigint");

        String sql =
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
                        + "     ) AS T";
        executeSqlWithJsonPlanVerified(sql).await();

        List<String> expected = Collections.singletonList("+I[6, 7, 8]");
        assertResult(expected, TestValuesTableFactory.getResults("MySink"));
    }

    @Test
    public void testComplexMatch() throws Exception {
        List<Row> data =
                Arrays.asList(
                        Row.of("ACME", 1L, 19, 1),
                        Row.of("ACME", 2L, 17, 2),
                        Row.of("ACME", 3L, 13, 3),
                        Row.of("ACME", 4L, 20, 4));
        createTestValuesSourceTable(
                "MyTable",
                data,
                "symbol string",
                "tstamp bigint",
                "price int",
                "tax int",
                "proctime as PROCTIME()");
        createTestValuesSinkTable("MySink", "a bigint", "b bigint", "c bigint");

        String sql =
                "insert into MySink SELECT * FROM MyTable MATCH_RECOGNIZE (\n"
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
                        + ") AS T";
        executeSqlWithJsonPlanVerified(sql).await();

        List<String> expected = Collections.singletonList("+I[19, 13, null]");
        assertResult(expected, TestValuesTableFactory.getResults("MySink"));
    }
}
