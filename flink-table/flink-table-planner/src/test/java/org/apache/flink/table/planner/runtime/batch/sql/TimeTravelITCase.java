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
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.DataTypes;
import org.apache.flink.table.api.EnvironmentSettings;
import org.apache.flink.table.api.Schema;
import org.apache.flink.table.api.TableResult;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.apache.flink.table.planner.factories.TestTimeTravelCatalog;
import org.apache.flink.table.planner.factories.TestValuesTableFactory;
import org.apache.flink.types.Row;
import org.apache.flink.util.CollectionUtil;

import org.junit.Before;
import org.junit.Test;

import java.time.LocalDateTime;
import java.time.ZoneId;
import java.time.format.DateTimeFormatter;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

import static org.junit.jupiter.api.Assertions.assertEquals;

public class TimeTravelITCase {
    private StreamExecutionEnvironment env;
    private StreamTableEnvironment tEnv;

    public static List<Tuple3<String, Schema, List<Row>>> TEST_TIME_TRAVEL_DATE =
            Arrays.asList(
                    Tuple3.of(
                            "2023-01-01 01:00:00",
                            Schema.newBuilder().column("a", DataTypes.INT()).build(),
                            Arrays.asList(Row.of(1))),
                    Tuple3.of(
                            "2023-01-01 02:00:00",
                            Schema.newBuilder()
                                    .column("a", DataTypes.INT())
                                    .column("b", DataTypes.INT())
                                    .build(),
                            Arrays.asList(Row.of(1, 2))),
                    Tuple3.of(
                            "2023-01-01 03:00:00",
                            Schema.newBuilder()
                                    .column("a", DataTypes.INT())
                                    .column("b", DataTypes.INT())
                                    .column("c", DataTypes.INT())
                                    .build(),
                            Arrays.asList(Row.of(1, 2, 3))));

    public static List<Tuple2<String, String>> EXPECTED_TIME_TRAVEL_RESULT =
            Arrays.asList(
                    Tuple2.of("2023-01-01 01:00:00", "[+I[1]]"),
                    Tuple2.of("2023-01-01 02:00:00", "[+I[1, 2]]"),
                    Tuple2.of("2023-01-01 03:00:00", "[+I[1, 2, 3]]"));

    @Before
    public void setup() {
        env = StreamExecutionEnvironment.getExecutionEnvironment();
        tEnv = StreamTableEnvironment.create(env, EnvironmentSettings.inBatchMode());
        TestTimeTravelCatalog catalog = new TestTimeTravelCatalog("timetravel");

        TEST_TIME_TRAVEL_DATE.stream()
                .forEach(
                        t -> {
                            String dataId = TestValuesTableFactory.registerData(t.f2);
                            Map<String, String> options = new HashMap<>();
                            options.put("connector", "values");
                            options.put("bounded", "true");
                            options.put("data-id", dataId);
                            catalog.registerTable(
                                    "t1",
                                    t.f1,
                                    options,
                                    convertStringToLong(t.f0, ZoneId.of("Asia/Shanghai")));
                        });

        tEnv.getConfig().setLocalTimeZone(ZoneId.of("Asia/Shanghai"));
        tEnv.registerCatalog("timetravel", catalog);
        tEnv.executeSql("use catalog timetravel");
    }

    @Test
    public void testTimeTravel() {

        for (Tuple2<String, String> res : EXPECTED_TIME_TRAVEL_RESULT) {
            TableResult tableResult =
                    tEnv.executeSql(
                            String.format(
                                    "SELECT * FROM t1 FOR SYSTEM_TIME AS OF TIMESTAMP '%s'",
                                    res.f0));
            List<String> sortedResult = toSortedResults(tableResult);
            assertEquals(res.f1, sortedResult.toString());
        }
    }

    @Test
    public void testTimeTravelWithExpression() {
        TableResult tableResult =
                tEnv.executeSql(
                        String.format(
                                "SELECT * FROM t1 FOR SYSTEM_TIME AS OF TIMESTAMP '%s' + INTERVAL '60' DAY  ",
                                "2023-01-01 00:00:00"));
        List<String> sortedResult = toSortedResults(tableResult);

        assertEquals("[+I[1, 2, 3]]", sortedResult.toString());
    }

    private static Long convertStringToLong(String timestamp, ZoneId zoneId) {
        return LocalDateTime.parse(timestamp, DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss"))
                        .atZone(zoneId)
                        .toEpochSecond()
                * 1000;
    }

    private List<String> toSortedResults(TableResult result) {
        return CollectionUtil.iteratorToList(result.collect()).stream()
                .map(Row::toString)
                .sorted()
                .collect(Collectors.toList());
    }
}
