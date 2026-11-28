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

import org.apache.flink.table.api.TableException;
import org.apache.flink.table.data.GenericRowData;
import org.apache.flink.table.data.StringData;
import org.apache.flink.table.data.TimestampData;
import org.apache.flink.table.planner.factories.TestValuesTableFactory;
import org.apache.flink.table.planner.utils.JsonPlanTestBase;
import org.apache.flink.table.planner.utils.JsonTestUtils;

import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.JsonNode;

import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.function.Function;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Tests for configuring operator-level state TTL via {@link
 * org.apache.flink.table.api.CompiledPlan}.
 */
class ConfigureOperatorLevelStateTtlJsonITCase extends JsonPlanTestBase {

    @Test
    void testDifferentStateTtlThroughCompiledPlanForDifferentOneInputStreamOperator()
            throws Exception {
        innerTestDeduplicateAndGroupAggregate(
                "INSERT INTO OrdersStats \n"
                        + "SELECT buyer, COUNT(1) AS ord_cnt, SUM(quantity) AS quantity_cnt, SUM(amount) AS total_amount FROM (\n"
                        + "SELECT *, ROW_NUMBER() OVER(PARTITION BY order_id, buyer, quantity, amount ORDER BY proctime() ASC) AS rk FROM Orders) tmp\n"
                        + "WHERE rk = 1\n"
                        + "GROUP BY buyer",
                json -> {
                    try {
                        JsonNode target = JsonTestUtils.readFromString(json);
                        JsonTestUtils.setExecNodeStateMetadata(
                                target, "stream-exec-deduplicate", 0, 6000L);
                        JsonTestUtils.setExecNodeStateMetadata(
                                target, "stream-exec-group-aggregate", 0, 9000L);
                        return JsonTestUtils.writeToString(target);
                    } catch (IOException e) {
                        throw new TableException("Cannot modify compiled json plan.", e);
                    }
                });
    }

    @Test
    void testDifferentStateTtlThroughSqlHintForDifferentOneInputStreamOperator() throws Exception {
        tableEnv.getConfig().set("table.exec.mini-batch.enabled", "true");
        tableEnv.getConfig().set("table.exec.mini-batch.size", "2");
        tableEnv.getConfig().set("table.exec.mini-batch.allow-latency", "1");
        innerTestDeduplicateAndGroupAggregate(
                "INSERT INTO OrdersStats \n"
                        + "SELECT /*+STATE_TTL('tmp' = '9s')*/ buyer, COUNT(1) AS ord_cnt, SUM(quantity) AS quantity_cnt, SUM(amount) AS total_amount \n"
                        + "FROM (\n"
                        + "    SELECT *, ROW_NUMBER() OVER(PARTITION BY order_id, buyer, quantity, amount ORDER BY proctime() ASC) AS rk FROM Orders\n"
                        + ") tmp\n"
                        + "WHERE rk = 1\n"
                        + "GROUP BY buyer",
                json -> {
                    try {
                        JsonNode target = JsonTestUtils.readFromString(json);
                        JsonTestUtils.setExecNodeStateMetadata(
                                target, "stream-exec-deduplicate", 0, 6000L);
                        return JsonTestUtils.writeToString(target);
                    } catch (IOException e) {
                        throw new TableException("Cannot modify compiled json plan.", e);
                    }
                });
    }

    @Test
    void testDifferentStateTtlThroughCompiledPlanForSameTwoInputStreamOperator() throws Exception {
        innerTestRegularJoin(
                "INSERT INTO OrdersShipInfo \n"
                        + "SELECT a.order_id, a.line_order_id, b.ship_mode FROM Orders a JOIN LineOrders b ON a.line_order_id = b.line_order_id",
                json -> {
                    try {
                        JsonNode target = JsonTestUtils.readFromString(json);
                        JsonTestUtils.setExecNodeStateMetadata(
                                target, "stream-exec-join", 0, 3000L);
                        JsonTestUtils.setExecNodeStateMetadata(
                                target, "stream-exec-join", 1, 9000L);
                        return JsonTestUtils.writeToString(target);
                    } catch (IOException e) {
                        throw new TableException("Cannot modify compiled json plan.", e);
                    }
                });
    }

    @Test
    void testDifferentStateTtlThroughSqlHintForSameTwoInputStreamOperator() throws Exception {
        innerTestRegularJoin(
                "INSERT INTO OrdersShipInfo \n"
                        + "SELECT /*+ STATE_TTL('a' = '3s', 'b' = '9s') */\n"
                        + " a.order_id, a.line_order_id, b.ship_mode "
                        + "FROM Orders a JOIN LineOrders b ON a.line_order_id = b.line_order_id",
                json -> json);
    }

    @Test
    void testStateTtlThroughCompiledPlanForOverAggregate() throws Exception {
        String dataId =
                TestValuesTableFactory.registerRowData(
                        Arrays.asList(
                                GenericRowData.of(1, 1L),
                                GenericRowData.of(1, 2L),
                                GenericRowData.of(1, 3L)));
        createTestSourceTable(
                "Orders",
                new String[] {"`buyer` INT", "`amount` BIGINT", "`proctime` AS PROCTIME()"},
                null,
                getProperties(dataId, 2, "4s"));
        createTestValuesSinkTable("RunningTotals", "`buyer` INT", "`total` BIGINT");
        compileSqlAndExecutePlan(
                        "INSERT INTO RunningTotals SELECT buyer, SUM(amount) "
                                + "OVER (PARTITION BY buyer ORDER BY proctime) FROM Orders",
                        withStateTtl("stream-exec-over-aggregate", 2000L))
                .await();

        // the third record arrives 4s after the second one, so with a state TTL of 2s the
        // running total starts again
        List<String> expected = Arrays.asList("+I[1, 1]", "+I[1, 3]", "+I[1, 3]");
        assertResult(expected, TestValuesTableFactory.getResultsAsStrings("RunningTotals"));
    }

    @Test
    void testStateTtlThroughCompiledPlanForTemporalJoin() throws Exception {
        String ordersDataId =
                TestValuesTableFactory.registerRowData(
                        Arrays.asList(
                                GenericRowData.of(
                                        StringData.fromString("EUR"),
                                        10L,
                                        TimestampData.fromEpochMillis(2000L)),
                                GenericRowData.of(
                                        StringData.fromString("EUR"),
                                        20L,
                                        TimestampData.fromEpochMillis(3000L))));
        createTestSourceTable(
                "Orders",
                new String[] {
                    "`currency` STRING",
                    "`amount` BIGINT",
                    "`order_time` TIMESTAMP(3)",
                    "WATERMARK FOR `order_time` AS `order_time`"
                },
                null,
                getProperties(ordersDataId, 1, "4s"));
        String ratesDataId =
                TestValuesTableFactory.registerRowData(
                        Collections.singletonList(
                                GenericRowData.of(
                                        StringData.fromString("EUR"),
                                        2L,
                                        TimestampData.fromEpochMillis(1000L))));
        createTestSourceTable(
                "Rates",
                new String[] {
                    "`currency` STRING",
                    "`rate` BIGINT",
                    "`rate_time` TIMESTAMP(3)",
                    "WATERMARK FOR `rate_time` AS `rate_time`",
                    "PRIMARY KEY (`currency`) NOT ENFORCED"
                },
                null,
                getProperties(ratesDataId, 1, "0s"));
        createTestValuesSinkTable("Amounts", "`amount` BIGINT");
        compileSqlAndExecutePlan(
                        "INSERT INTO Amounts SELECT o.amount * r.rate FROM Orders AS o "
                                + "JOIN Rates FOR SYSTEM_TIME AS OF o.order_time AS r "
                                + "ON o.currency = r.currency",
                        withStateTtl("stream-exec-temporal-join", 1000L))
                .await();

        // the second order arrives 4s after the first one, so with a state TTL of 1s the rate
        // has been cleaned up and the second order finds no match
        List<String> expected = Arrays.asList("+I[20]");
        assertResult(expected, TestValuesTableFactory.getResultsAsStrings("Amounts"));
    }

    @Test
    void testNoStateInCompiledPlanForBoundedRangeOverAggregate() throws Exception {
        createTestSourceTable(
                "Orders",
                new String[] {"`buyer` INT", "`amount` BIGINT", "`proctime` AS PROCTIME()"},
                null,
                getProperties(
                        TestValuesTableFactory.registerRowData(Collections.emptyList()), 1, "0s"));
        createTestValuesSinkTable("RunningTotals", "`buyer` INT", "`total` BIGINT");
        JsonNode plan =
                JsonTestUtils.readFromString(
                        tableEnv.compilePlanSql(
                                        "INSERT INTO RunningTotals SELECT buyer, SUM(amount) OVER ("
                                                + "PARTITION BY buyer ORDER BY proctime RANGE "
                                                + "BETWEEN INTERVAL '1' MINUTE PRECEDING AND "
                                                + "CURRENT ROW) FROM Orders")
                                .asJsonString());

        // a bounded RANGE window has no state retention time, so there is no TTL to configure
        assertThat(plan.get("nodes"))
                .filteredOn(n -> n.get("type").asText().startsWith("stream-exec-over-aggregate"))
                .singleElement()
                .satisfies(n -> assertThat(n.has("state")).isFalse());
    }

    private static Function<String, String> withStateTtl(String execNodeType, long stateTtl) {
        return json -> {
            try {
                JsonNode target = JsonTestUtils.readFromString(json);
                JsonTestUtils.setExecNodeStateMetadata(target, execNodeType, 0, stateTtl);
                return JsonTestUtils.writeToString(target);
            } catch (IOException e) {
                throw new TableException("Cannot modify compiled json plan.", e);
            }
        };
    }

    private void innerTestDeduplicateAndGroupAggregate(
            String sql, Function<String, String> jsonPlanTransformer) throws Exception {
        String dataId =
                TestValuesTableFactory.registerRowData(
                        Arrays.asList(
                                GenericRowData.of(1, StringData.fromString("Tom"), 1, 199.9d),
                                GenericRowData.of(2, StringData.fromString("Jerry"), 2, 99.9d),
                                GenericRowData.of(1, StringData.fromString("Tom"), 1, 199.9d),
                                GenericRowData.of(3, StringData.fromString("Tom"), 1, 29.9d),
                                GenericRowData.of(4, StringData.fromString("Olivia"), 1, 100d),
                                GenericRowData.of(4, StringData.fromString("Olivia"), 1, 100d),
                                GenericRowData.of(2, StringData.fromString("Jerry"), 2, 99.9d),
                                GenericRowData.of(5, StringData.fromString("Michael"), 3, 599.9d),
                                GenericRowData.of(6, StringData.fromString("Olivia"), 3, 1000d)));
        createTestSourceTable(
                "Orders",
                new String[] {
                    "`order_id` INT", "`buyer` STRING", "`quantity` INT", "`amount` DOUBLE"
                },
                null,
                getProperties(dataId, 1, "2s"));

        createTestNonInsertOnlyValuesSinkTable(
                "OrdersStats",
                "`buyer` STRING",
                "`ord_cnt` BIGINT",
                "`quantity_cnt` BIGINT",
                "`total_amount` DOUBLE");
        compileSqlAndExecutePlan(sql, jsonPlanTransformer).await();

        // with deduplicate state's TTL as 6s, record (+I,2,Jerry,2,99.9) will duplicate itself
        // +-------------------+--------------------------------------+------------------+
        // |        data       | diff(last_arriving, first_arriving) | within_time_range |
        // +-------------------+-------------------------------------+-------------------+
        // | 1,Tom,1,199.9     |                 4s                  |         Y         |
        // +-------------------+-------------------------------------+-------------------+
        // | 2,Jerry,2,99.9    |                 10s                  |        N         |
        // +-------------------+-------------------------------------+-------------------+
        // | 3,Tom,1,29.9      |                 0s                  |         Y         |
        // +-------------------+-------------------------------------+-------------------+
        // | 4,Olivia,1,100    |                 2s                  |         Y         |
        // +-------------------+-------------------------------------+-------------------+
        // | 5,Michael,3,599.9 |                 0s                  |         Y         |
        // +-------------------+-------------------------------------+-------------------+
        // | 6,Olivia,3,1000   |                 0s                  |         Y         |
        // +-------------------+-------------------------------------+-------------------+

        // with group-aggregate state's TTL as 9s, record (+I,2,Jerry,2,99.9) will be counted twice
        List<String> expected =
                Arrays.asList(
                        "+I[Tom, 2, 2, 229.8]",
                        "+I[Jerry, 1, 2, 99.9]",
                        "+I[Jerry, 1, 2, 99.9]",
                        "+I[Olivia, 2, 4, 1100.0]",
                        "+I[Michael, 1, 3, 599.9]");
        assertResult(expected, TestValuesTableFactory.getResultsAsStrings("OrdersStats"));
    }

    private void innerTestRegularJoin(String sql, Function<String, String> jsonPlanTransformer)
            throws Exception {
        String leftTableDataId =
                TestValuesTableFactory.registerRowData(
                        Arrays.asList(
                                GenericRowData.of(1, 1000001),
                                GenericRowData.of(1, 1000002),
                                GenericRowData.of(1, 1000003),
                                GenericRowData.of(1, 1000004),
                                GenericRowData.of(1, 1000005),
                                GenericRowData.of(2, 2000001)));
        createTestSourceTable(
                "Orders",
                new String[] {"`order_id` INT", "`line_order_id` INT"},
                null,
                getProperties(leftTableDataId, 1, "2s"));

        String rightTableDataId =
                TestValuesTableFactory.registerRowData(
                        Arrays.asList(
                                GenericRowData.of(2000001, StringData.fromString("TRUCK")),
                                GenericRowData.of(1000005, StringData.fromString("AIR")),
                                GenericRowData.of(1000001, StringData.fromString("SHIP")),
                                GenericRowData.of(1000002, StringData.fromString("TRUCK")),
                                GenericRowData.of(1000003, StringData.fromString("RAIL")),
                                GenericRowData.of(1000004, StringData.fromString("RAIL"))));
        createTestSourceTable(
                "LineOrders",
                new String[] {"`line_order_id` INT", "`ship_mode` STRING"},
                null,
                getProperties(rightTableDataId, 2, "4s"));

        createTestValuesSinkTable(
                "OrdersShipInfo", "`order_id` INT", "`line_order_id` INT", "`ship_mode` STRING");
        compileSqlAndExecutePlan(sql, jsonPlanTransformer).await();

        // with left-state TTL as 3s and right-state TTL as 9s
        // +--------------+--------------+-------------------------------------+-------------------+
        // |  left_data   |  right_data  | diff(left_arriving, right_arriving) | within_time_range |
        // +--------------+--------------+-------------------------------------+-------------------+
        // |  1,1000001   | 1000001,SHIP |                 4s                  |         N         |
        // +--------------+--------------+-------------------------------------+-------------------+
        // |  1,1000002   | 1000002,TRUCK|                 2s                  |         Y         |
        // +--------------+--------------+-------------------------------------+-------------------+
        // |  1,1000003   | 1000003,RAIL |                 4s                  |         N         |
        // +--------------+--------------+-------------------------------------+-------------------+
        // |  1,1000004   | 1000004,RAIL |                 2s                  |         Y         |
        // +--------------+--------------+-------------------------------------+-------------------+
        // |  1,1000005   | 1000005,AIR  |                -8s                  |         Y         |
        // +--------------+--------------+-------------------------------------+-------------------+
        // |  2,2000001   | 2000001,TRUCK|               -10s                  |         N         |
        // +--------------+--------------+-------------------------------------+-------------------+

        List<String> expected =
                Arrays.asList(
                        "+I[1, 1000002, TRUCK]", "+I[1, 1000004, RAIL]", "+I[1, 1000005, AIR]");
        assertResult(expected, TestValuesTableFactory.getResultsAsStrings("OrdersShipInfo"));
    }

    private static Map<String, String> getProperties(
            String dataId, int sleepAfterElements, String sleepTime) {
        return new HashMap<String, String>() {
            {
                put("connector", "values");
                put("bounded", "false");
                put("register-internal-data", "true");
                put("source.sleep-after-elements", String.valueOf(sleepAfterElements));
                put("source.sleep-time", sleepTime);
                put("data-id", dataId);
            }
        };
    }
}
