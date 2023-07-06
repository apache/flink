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
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.table.api.DataTypes;
import org.apache.flink.table.api.Schema;
import org.apache.flink.table.api.TableException;
import org.apache.flink.table.api.TableResult;
import org.apache.flink.table.api.ValidationException;
import org.apache.flink.table.planner.factories.TestTimeTravelCatalog;
import org.apache.flink.table.planner.factories.TestValuesTableFactory;
import org.apache.flink.table.planner.runtime.utils.BatchTestBase;
import org.apache.flink.types.Row;
import org.apache.flink.util.CollectionUtil;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.time.LocalDateTime;
import java.time.ZoneId;
import java.time.format.DateTimeFormatter;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.junit.jupiter.api.Assertions.assertEquals;

/** Test for time travel. */
public class TimeTravelITCase extends BatchTestBase {
    private static final List<Tuple3<String, Schema, List<Row>>> TEST_TIME_TRAVEL_DATE =
            Arrays.asList(
                    Tuple3.of(
                            "2023-01-01 01:00:00",
                            Schema.newBuilder().column("f1", DataTypes.INT()).build(),
                            Collections.singletonList(Row.of(1))),
                    Tuple3.of(
                            "2023-01-01 02:00:00",
                            Schema.newBuilder()
                                    .column("f1", DataTypes.INT())
                                    .column("f2", DataTypes.INT())
                                    .build(),
                            Collections.singletonList(Row.of(1, 2))),
                    Tuple3.of(
                            "2023-01-01 03:00:00",
                            Schema.newBuilder()
                                    .column("f1", DataTypes.INT())
                                    .column("f2", DataTypes.INT())
                                    .column("f3", DataTypes.INT())
                                    .build(),
                            Collections.singletonList(Row.of(1, 2, 3))));

    private static final List<Tuple2<String, String>> EXPECTED_TIME_TRAVEL_RESULT =
            Arrays.asList(
                    Tuple2.of("2023-01-01 01:00:00", "[+I[1]]"),
                    Tuple2.of("2023-01-01 02:00:00", "[+I[1, 2]]"),
                    Tuple2.of("2023-01-01 03:00:00", "[+I[1, 2, 3]]"));

    @BeforeEach
    public void before() {
        TestTimeTravelCatalog catalog = new TestTimeTravelCatalog("TimeTravelCatalog");

        TEST_TIME_TRAVEL_DATE.forEach(
                t -> {
                    String dataId = TestValuesTableFactory.registerData(t.f2);
                    Map<String, String> options = new HashMap<>();
                    options.put("connector", "values");
                    options.put("bounded", "true");
                    options.put("data-id", dataId);
                    try {
                        catalog.registerTable(
                                "t1", t.f1, options, convertStringToLong(t.f0, ZoneId.of("UTC")));
                    } catch (Exception e) {
                        throw new RuntimeException(e);
                    }
                });
        tEnv().registerCatalog("TimeTravelCatalog", catalog);
        tEnv().useCatalog("TimeTravelCatalog");
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
                                            timezoneConvert(
                                                    res.f0,
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
                                        + "    f1\n"
                                        + "FROM\n"
                                        + "    t1 FOR SYSTEM_TIME AS OF TIMESTAMP '2023-01-01 02:00:00'");

        List<String> sortedResult = toSortedResults(tableResult);
        assertEquals("[+I[1], +I[1]]", sortedResult.toString());

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
                                        + "    l.f1,\n"
                                        + "    r.f2\n"
                                        + "FROM\n"
                                        + "    (\n"
                                        + "        SELECT\n"
                                        + "            *,\n"
                                        + "            proctime () as p\n"
                                        + "        FROM\n"
                                        + "            t1 FOR SYSTEM_TIME AS OF TIMESTAMP '2023-01-01 01:00:00'\n"
                                        + "    ) l\n"
                                        + "    LEFT JOIN t1 FOR SYSTEM_TIME AS OF l.p r ON l.f1=r.f1");

        List<String> sortedResult = toSortedResults(tableResult);
        assertEquals("[+I[1, 2]]", sortedResult.toString());
    }

    @Test
    void testTimeTravelWithUnsupportedExpression() {
        assertThatThrownBy(
                        () ->
                                tEnv().executeSql(
                                                "SELECT\n"
                                                        + "    *\n"
                                                        + "FROM\n"
                                                        + "    t1 FOR SYSTEM_TIME AS OF TO_TIMESTAMP_LTZ (0, 3)"))
                .isInstanceOf(ValidationException.class)
                .hasMessageContaining("Unsupported time travel period: TO_TIMESTAMP_LTZ(0, 3)");

        assertThatThrownBy(
                        () ->
                                tEnv().executeSql(
                                                "SELECT\n"
                                                        + "    *\n"
                                                        + "FROM\n"
                                                        + "    t1 FOR SYSTEM_TIME AS OF PROCTIME()"))
                .isInstanceOf(ValidationException.class)
                .hasMessageContaining("Unsupported time travel period: PROCTIME()");
    }

    @Test
    void testTimeTravelWithIdentifierSnapshot() {

        tEnv().executeSql(
                        "CREATE TABLE\n"
                                + "    t2 (f1 VARCHAR, f2 TIMESTAMP(3))\n"
                                + "WITH\n"
                                + "    ('connector'='values', 'bounded'='true')");

        // select snapshot with identifier only support in lookup join or temporal join.
        // The following query can't generate a validate execution plan.

        assertThatThrownBy(
                        () ->
                                tEnv().executeSql(
                                                "SELECT\n"
                                                        + "    *\n"
                                                        + "FROM\n"
                                                        + "    t2 FOR SYSTEM_TIME AS OF f2"))
                .isInstanceOf(TableException.class)
                .hasMessageContaining("Cannot generate a valid execution plan for the given query");
    }

    @Test
    void testTimeTravelWithView() {
        tEnv().executeSql("CREATE VIEW tb_view AS SELECT * FROM t1");

        assertThatThrownBy(
                        () ->
                                tEnv().executeSql(
                                                "SELECT\n"
                                                        + "    *\n"
                                                        + "FROM\n"
                                                        + "    tb_view FOR SYSTEM_TIME AS OF TIMESTAMP '2013-01-01 01:00:00'"))
                .isInstanceOf(ValidationException.class)
                .hasMessageContaining(
                        "Table view: TimeTravelCatalog.default.tb_view does not support time travel");
    }

    private static Long convertStringToLong(String timestamp, ZoneId zoneId) {
        return LocalDateTime.parse(timestamp, DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss"))
                .atZone(zoneId)
                .toInstant()
                .toEpochMilli();
    }

    private static String timezoneConvert(
            String timestamp, ZoneId originZoneId, ZoneId convrtedZoneId) {
        return LocalDateTime.parse(timestamp, DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss"))
                .atZone(originZoneId)
                .toInstant()
                .atZone(convrtedZoneId)
                .toLocalDateTime()
                .format(DateTimeFormatter.ofPattern("yyyy-MM-dd HH:MM:ss"));
    }

    private List<String> toSortedResults(TableResult result) {
        return CollectionUtil.iteratorToList(result.collect()).stream()
                .map(Row::toString)
                .sorted()
                .collect(Collectors.toList());
    }
}
