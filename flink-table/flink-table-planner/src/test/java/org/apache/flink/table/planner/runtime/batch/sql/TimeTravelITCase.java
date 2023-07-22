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

package org.apache.flink.table.planner.runtime.batch.sql;

import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.table.api.TableResult;
import org.apache.flink.table.planner.factories.TestTimeTravelCatalog;
import org.apache.flink.table.planner.runtime.utils.BatchTestBase;
import org.apache.flink.table.planner.runtime.utils.TimeTravelTestUtil;
import org.apache.flink.table.planner.utils.DateTimeTestUtil;
import org.apache.flink.types.Row;
import org.apache.flink.util.CollectionUtil;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.time.ZoneId;
import java.util.Arrays;
import java.util.List;
import java.util.stream.Collectors;

import static org.junit.jupiter.api.Assertions.assertEquals;

/** Test for time travel. */
public class TimeTravelITCase extends BatchTestBase {
    private static final List<Tuple2<String, String>> EXPECTED_TIME_TRAVEL_RESULT =
            Arrays.asList(
                    Tuple2.of("2023-01-01 01:00:00", "[+I[1]]"),
                    Tuple2.of("2023-01-01 02:00:00", "[+I[1, 2]]"),
                    Tuple2.of("2023-01-01 03:00:00", "[+I[1, 2, 3]]"));

    @BeforeEach
    @Override
    public void before() {
        String catalogName = "TimeTravelCatalog";
        TestTimeTravelCatalog catalog =
                TimeTravelTestUtil.getTestingCatalogWithVersionedTable(catalogName, "t1");

        tEnv().registerCatalog(catalogName, catalog);
        tEnv().useCatalog(catalogName);
        tEnv().getConfig().setLocalTimeZone(ZoneId.of("UTC"));
    }

    @Test
    void testTimeTravel() {
        for (Tuple2<String, String> res : EXPECTED_TIME_TRAVEL_RESULT) {
            TableResult tableResult =
                    tEnv().executeSql(
                                    String.format(
                                            "SELECT * FROM t1 FOR SYSTEM_TIME AS OF TIMESTAMP '%s'",
                                            res.f0));
            List<String> sortedResult = toSortedResults(tableResult);
            assertEquals(res.f1, sortedResult.toString());
        }
    }

    @Test
    void testTimeTravelWithAsExpression() {

        for (Tuple2<String, String> res : EXPECTED_TIME_TRAVEL_RESULT) {
            TableResult tableResult =
                    tEnv().executeSql(
                                    String.format(
                                            "SELECT\n"
                                                    + "    *\n"
                                                    + "FROM\n"
                                                    + "    t1 FOR SYSTEM_TIME AS OF TIMESTAMP '%s' AS t2",
                                            res.f0));
            List<String> sortedResult = toSortedResults(tableResult);
            assertEquals(res.f1, sortedResult.toString());
        }
    }

    @Test
    void testTimeTravelWithSimpleExpression() {
        TableResult tableResult =
                tEnv().executeSql(
                                "SELECT\n"
                                        + "    *\n"
                                        + "FROM\n"
                                        + "    t1 FOR SYSTEM_TIME AS OF TIMESTAMP '2023-01-01 00:00:00'+INTERVAL '60' DAY");
        List<String> sortedResult = toSortedResults(tableResult);

        assertEquals("[+I[1, 2, 3]]", sortedResult.toString());
    }

    @Test
    void testTimeTravelWithDifferentTimezone() {
        tEnv().getConfig().setLocalTimeZone(ZoneId.of("Asia/Shanghai"));
        for (Tuple2<String, String> res : EXPECTED_TIME_TRAVEL_RESULT) {
            TableResult tableResult =
                    tEnv().executeSql(
                                    String.format(
                                            "SELECT\n"
                                                    + "    *\n"
                                                    + "FROM\n"
                                                    + "    t1 FOR SYSTEM_TIME AS OF TIMESTAMP '%s' AS t2",
                                            DateTimeTestUtil.timezoneConvert(
                                                    res.f0,
                                                    "yyyy-MM-dd HH:mm:ss",
                                                    ZoneId.of("UTC"),
                                                    ZoneId.of("Asia/Shanghai"))));
            List<String> sortedResult = toSortedResults(tableResult);
            assertEquals(res.f1, sortedResult.toString());
        }
    }

    @Test
    void testTimeTravelOneTableMultiTimes() {
        // test union all same table with different snapshot.
        TableResult tableResult =
                tEnv().executeSql(
                                "SELECT\n"
                                        + "    f1\n"
                                        + "FROM\n"
                                        + "    t1 FOR SYSTEM_TIME AS OF TIMESTAMP '2023-01-01 01:00:00'\n"
                                        + "UNION ALL\n"
                                        + "SELECT\n"
                                        + "    f2\n"
                                        + "FROM\n"
                                        + "    t1 FOR SYSTEM_TIME AS OF TIMESTAMP '2023-01-01 02:00:00'");

        List<String> sortedResult = toSortedResults(tableResult);
        assertEquals("[+I[1], +I[2]]", sortedResult.toString());

        // test join same table with different snapshot
        tableResult =
                tEnv().executeSql(
                                "SELECT\n"
                                        + "    l.f1,\n"
                                        + "    r.f2\n"
                                        + "FROM\n"
                                        + "    t1 FOR SYSTEM_TIME AS OF TIMESTAMP '2023-01-01 01:00:00' l\n"
                                        + "    LEFT JOIN t1 FOR SYSTEM_TIME AS OF TIMESTAMP '2023-01-01 02:00:00' r ON l.f1=r.f1");

        sortedResult = toSortedResults(tableResult);
        assertEquals("[+I[1, 2]]", sortedResult.toString());
    }

    @Test
    void testTimeTravelWithLookupJoin() {
        // We must make sure time travel will not affect the lookup join and temporal join
        TableResult tableResult =
                tEnv().executeSql(
                                "SELECT\n"
                                        + "    l.f2,\n"
                                        + "    r.f3\n"
                                        + "FROM\n"
                                        + "    (\n"
                                        + "        SELECT\n"
                                        + "            *,\n"
                                        + "            proctime () as p\n"
                                        + "        FROM\n"
                                        + "            t1 FOR SYSTEM_TIME AS OF TIMESTAMP '2023-01-01 02:00:00'\n"
                                        + "    ) l\n"
                                        + "    LEFT JOIN t1 FOR SYSTEM_TIME AS OF l.p r ON l.f1=r.f1");

        List<String> sortedResult = toSortedResults(tableResult);
        assertEquals("[+I[2, 3]]", sortedResult.toString());
    }

    @Test
    void testTimeTravelWithHints() {
        TableResult tableResult =
                tEnv().executeSql(
                                "SELECT * FROM t1 /*+ options('bounded'='true') */ FOR SYSTEM_TIME AS OF TIMESTAMP '2023-01-01 01:00:00'");

        List<String> sortedResult = toSortedResults(tableResult);
        assertEquals("[+I[1]]", sortedResult.toString());

        tableResult =
                tEnv().executeSql(
                                "SELECT * FROM t1 /*+ options('bounded'='true') */ FOR SYSTEM_TIME AS OF TIMESTAMP '2023-01-01 02:00:00' AS t2");

        sortedResult = toSortedResults(tableResult);
        assertEquals("[+I[1, 2]]", sortedResult.toString());
    }

    private List<String> toSortedResults(TableResult result) {
        return CollectionUtil.iteratorToList(result.collect()).stream()
                .map(Row::toString)
                .sorted()
                .collect(Collectors.toList());
    }
}
