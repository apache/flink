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

package org.apache.flink.table.planner.plan.stream.sql;

import org.apache.flink.table.api.ExplainDetail;
import org.apache.flink.table.api.StatementSet;
import org.apache.flink.table.api.TableConfig;
import org.apache.flink.table.api.config.OptimizerConfigOptions;
import org.apache.flink.table.planner.utils.PlanKind;
import org.apache.flink.table.planner.utils.StreamTableTestUtil;
import org.apache.flink.table.planner.utils.TableTestBase;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;

import scala.Enumeration;

import static scala.runtime.BoxedUnit.UNIT;

/** Tests for multi-join plans. */
public class MultiJoinTest extends TableTestBase {

    private StreamTableTestUtil util;

    @BeforeEach
    void setup() {
        util =
                streamTestUtil(
                        TableConfig.getDefault()
                                .set(
                                        OptimizerConfigOptions.TABLE_OPTIMIZER_MULTI_JOIN_ENABLED,
                                        true));

        util.tableEnv()
                .executeSql(
                        "CREATE TABLE Users ("
                                + "  user_id STRING PRIMARY KEY NOT ENFORCED,"
                                + "  name STRING,"
                                + "  cash INT"
                                + ") WITH ('connector' = 'values', 'changelog-mode' = 'I,UA,D')");

        util.tableEnv()
                .executeSql(
                        "CREATE TABLE Orders ("
                                + "  order_id STRING PRIMARY KEY NOT ENFORCED,"
                                + "  user_id STRING,"
                                + "  product STRING"
                                + ") WITH ('connector' = 'values', 'changelog-mode' = 'I,D')");

        util.tableEnv()
                .executeSql(
                        "CREATE TABLE Payments ("
                                + "  payment_id STRING PRIMARY KEY NOT ENFORCED,"
                                + "  price INT,"
                                + "  user_id STRING"
                                + ") WITH ('connector' = 'values', 'changelog-mode' = 'I')");

        util.tableEnv()
                .executeSql(
                        "CREATE TABLE Shipments ("
                                + "  location STRING,"
                                + "  user_id STRING"
                                + ") WITH ('connector' = 'values', 'changelog-mode' = 'I,UA,UB,D')");

        util.tableEnv()
                .executeSql(
                        "CREATE TABLE Detail ("
                                + "  detail_id STRING PRIMARY KEY NOT ENFORCED,"
                                + "  description STRING,"
                                + "  user_id STRING,"
                                + "  data STRING,"
                                + "  `timestamp` BIGINT"
                                + ") WITH ('connector' = 'values', 'changelog-mode' = 'I')");

        // Tables for testing temporal join exclusion
        util.tableEnv()
                .executeSql(
                        "CREATE TABLE LookupTable ("
                                + "  id STRING PRIMARY KEY NOT ENFORCED,"
                                + "  name STRING,"
                                + "  age INT"
                                + ") WITH ('connector' = 'values', 'changelog-mode' = 'I')");

        util.tableEnv()
                .executeSql(
                        "CREATE TABLE StreamTable ("
                                + "  user_id STRING,"
                                + "  amount INT,"
                                + "  proctime AS PROCTIME()"
                                + ") WITH ('connector' = 'values', 'changelog-mode' = 'I')");

        // Tables for testing interval join exclusion
        util.tableEnv()
                .executeSql(
                        "CREATE TABLE EventTable1 ("
                                + "  id STRING,"
                                + "  val INT,"
                                + "  `$rowtime` TIMESTAMP(3),"
                                + "  WATERMARK FOR `$rowtime` AS `$rowtime` - INTERVAL '5' SECOND"
                                + ") WITH ('connector' = 'values', 'changelog-mode' = 'I')");

        util.tableEnv()
                .executeSql(
                        "CREATE TABLE EventTable2 ("
                                + "  id STRING,"
                                + "  price DOUBLE,"
                                + "  `$rowtime` TIMESTAMP(3),"
                                + "  WATERMARK FOR `$rowtime` AS `$rowtime` - INTERVAL '5' SECOND"
                                + ") WITH ('connector' = 'values', 'changelog-mode' = 'I')");

        // Tables for testing time attribute materialization in multi-join
        util.tableEnv()
                .executeSql(
                        "CREATE TABLE UsersWithProctime ("
                                + "  user_id STRING PRIMARY KEY NOT ENFORCED,"
                                + "  name STRING,"
                                + "  proctime AS PROCTIME()"
                                + ") WITH ('connector' = 'values', 'changelog-mode' = 'I')");

        util.tableEnv()
                .executeSql(
                        "CREATE TABLE OrdersWithRowtime ("
                                + "  order_id STRING PRIMARY KEY NOT ENFORCED,"
                                + "  user_id STRING,"
                                + "  `$rowtime` TIMESTAMP(3),"
                                + "  WATERMARK FOR `$rowtime` AS `$rowtime`"
                                + ") WITH ('connector' = 'values', 'changelog-mode' = 'I')");
        // Tables for testing upsert key preservation
        util.tableEnv()
                .executeSql(
                        "CREATE TABLE UsersPK ("
                                + "  user_id STRING PRIMARY KEY NOT ENFORCED,"
                                + "  name STRING,"
                                + "  region_id INT,"
                                + "  description STRING"
                                + ") WITH ('connector' = 'values', 'changelog-mode' = 'I,UA,D')");

        util.tableEnv()
                .executeSql(
                        "CREATE TABLE OrdersPK ("
                                + "  order_id STRING NOT NULL,"
                                + "  user_id STRING NOT NULL,"
                                + "  product STRING,"
                                + "  CONSTRAINT `PRIMARY` PRIMARY KEY (order_id, user_id) NOT ENFORCED"
                                + ") WITH ('connector' = 'values', 'changelog-mode' = 'I,UA,D')");

        util.tableEnv()
                .executeSql(
                        "CREATE TABLE PaymentsPK ("
                                + "  payment_id STRING NOT NULL,"
                                + "  user_id STRING NOT NULL,"
                                + "  price INT,"
                                + "  CONSTRAINT `PRIMARY` PRIMARY KEY (payment_id, user_id) NOT ENFORCED"
                                + ") WITH ('connector' = 'values', 'changelog-mode' = 'I,UA,D')");

        util.tableEnv()
                .executeSql(
                        "CREATE TABLE AddressPK ("
                                + "  user_id STRING PRIMARY KEY NOT ENFORCED,"
                                + "  location STRING"
                                + ") WITH ('connector' = 'values', 'changelog-mode' = 'I,UA,D')");
    }

    @Test
    void testThreeWayInnerJoinRelPlan() {
        util.verifyRelPlan(
                "\nSELECT\n"
                        + "    u.user_id,\n"
                        + "    u.name,\n"
                        + "    o.order_id,\n"
                        + "    p.payment_id\n"
                        + "FROM Users u\n"
                        + "INNER JOIN Orders o\n"
                        + "    ON u.user_id = o.user_id\n"
                        + "INNER JOIN Payments p\n"
                        + "    ON u.user_id = p.user_id");
    }

    @Test
    @Tag("no-common-join-key")
    void testThreeWayInnerJoinRelPlanNoCommonJoinKey() {
        util.verifyRelPlan(
                "\nSELECT\n"
                        + "    u.user_id,\n"
                        + "    u.name,\n"
                        + "    o.order_id,\n"
                        + "    p.payment_id\n"
                        + "FROM Users u\n"
                        + "INNER JOIN Orders o\n"
                        + "    ON u.user_id = o.user_id\n"
                        + "INNER JOIN Payments p\n"
                        + "    ON u.cash = p.price");
    }

    @Test
    void testThreeWayInnerJoinExecPlan() {
        util.verifyExecPlan(
                "\nSELECT\n"
                        + "    u.user_id,\n"
                        + "    u.name,\n"
                        + "    o.order_id,\n"
                        + "    p.payment_id\n"
                        + "FROM Users u\n"
                        + "INNER JOIN Orders o\n"
                        + "    ON u.user_id = o.user_id\n"
                        + "INNER JOIN Payments p\n"
                        + "    ON u.user_id = p.user_id");
    }

    @Test
    void testThreeWayLeftOuterJoinRelPlan() {
        util.verifyRelPlan(
                "\nSELECT\n"
                        + "    u.user_id,\n"
                        + "    u.name,\n"
                        + "    o.order_id,\n"
                        + "    p.payment_id\n"
                        + "FROM Users u\n"
                        + "LEFT JOIN Orders o\n"
                        + "    ON u.user_id = o.user_id\n"
                        + "LEFT JOIN Payments p\n"
                        + "    ON u.user_id = p.user_id");
    }

    @Test
    void testThreeWayInnerJoinWithTttlHints() {
        util.verifyRelPlan(
                "\nSELECT\n"
                        + "    /*+ STATE_TTL(u='1d', o='2d', p='1h') */\n"
                        + "    u.user_id,\n"
                        + "    u.name,\n"
                        + "    o.order_id,\n"
                        + "    p.payment_id\n"
                        + "FROM Users u\n"
                        + "INNER JOIN Orders o\n"
                        + "    ON u.user_id = o.user_id\n"
                        + "INNER JOIN Payments p\n"
                        + "    ON u.user_id = p.user_id");
    }

    @Test
    void testThreeWayInnerJoinWithSingleTttlHint() {
        util.verifyRelPlan(
                "\nSELECT\n"
                        + "    /*+ STaTE_tTL(o='2d') */\n"
                        + "    u.user_id,\n"
                        + "    u.name,\n"
                        + "    o.order_id,\n"
                        + "    p.payment_id\n"
                        + "FROM Users u\n"
                        + "INNER JOIN Orders o\n"
                        + "    ON u.user_id = o.user_id\n"
                        + "INNER JOIN Payments p\n"
                        + "    ON u.user_id = p.user_id");
    }

    @Test
    void testThreeWayJoinWithMultiJoinHint() {
        // Disable config so the MultiJoin is enabled by hints
        util.getTableEnv()
                .getConfig()
                .set(OptimizerConfigOptions.TABLE_OPTIMIZER_MULTI_JOIN_ENABLED, false);

        util.verifyRelPlan(
                "SELECT /*+ MULTI_JOIN(u, o, p) */u.user_id, u.name, o.order_id, p.payment_id "
                        + "FROM Users u "
                        + "INNER JOIN Orders o ON u.user_id = o.user_id "
                        + "LEFT JOIN Payments p ON u.user_id = p.user_id");
    }

    @Test
    void testMultiJoinHintCombinedWithStateTtlHint() {
        util.getTableEnv()
                .getConfig()
                .set(OptimizerConfigOptions.TABLE_OPTIMIZER_MULTI_JOIN_ENABLED, false);

        util.verifyRelPlan(
                "SELECT /*+ MULTI_JOIN(u, o, p), STATE_TTL(u='1d', o='2d', p='1h') */u.user_id, u.name, o.order_id, p.payment_id "
                        + "FROM Users u "
                        + "INNER JOIN Orders o ON u.user_id = o.user_id "
                        + "INNER JOIN Payments p ON u.user_id = p.user_id");
    }

    @Test
    void testMultiJoinPartialHintCombinedWithStateTtlHint() {
        util.getTableEnv()
                .getConfig()
                .set(OptimizerConfigOptions.TABLE_OPTIMIZER_MULTI_JOIN_ENABLED, false);

        util.verifyRelPlan(
                "SELECT /*+ MULTI_JOIN(u, o), STATE_TTL(u='1d', o='2d', p='1h') */u.user_id, u.name, o.order_id, p.payment_id "
                        + "FROM Users u "
                        + "INNER JOIN Orders o ON u.user_id = o.user_id "
                        + "INNER JOIN Payments p ON u.user_id = p.user_id");
    }

    @Test
    void testMultiJoinHintWithTableNames() {
        util.getTableEnv()
                .getConfig()
                .set(OptimizerConfigOptions.TABLE_OPTIMIZER_MULTI_JOIN_ENABLED, false);

        util.verifyRelPlan(
                "SELECT /*+ MULTI_JOIN(Users, Orders, Payments) */Users.user_id, Users.name, Orders.order_id, Payments.payment_id "
                        + "FROM Users "
                        + "INNER JOIN Orders ON Users.user_id = Orders.user_id "
                        + "INNER JOIN Payments ON Users.user_id = Payments.user_id");
    }

    @Test
    void testMultiJoinHintPartialMatch() {
        // First join (u, o) should become MultiJoin with hint
        // When the result joins with s, that's not in the hint so regular join should be used
        util.getTableEnv()
                .getConfig()
                .set(OptimizerConfigOptions.TABLE_OPTIMIZER_MULTI_JOIN_ENABLED, false);

        util.verifyRelPlan(
                "SELECT /*+ MULTI_JOIN(u, o) */u.user_id, u.name, o.order_id, s.location "
                        + "FROM Users u "
                        + "INNER JOIN Orders o ON u.user_id = o.user_id "
                        + "INNER JOIN Shipments s ON u.user_id = s.user_id");
    }

    @Test
    void testMultiJoinHintWithMixedNamesAndAliases() {
        // Hint uses table name for Users, but aliases for others - matching should work
        util.getTableEnv()
                .getConfig()
                .set(OptimizerConfigOptions.TABLE_OPTIMIZER_MULTI_JOIN_ENABLED, false);

        util.verifyRelPlan(
                "SELECT /*+ MULTI_JOIN(Users, o, p) */ Users.user_id, Users.name, o.order_id, p.payment_id "
                        + "FROM Users "
                        + "INNER JOIN Orders o ON Users.user_id = o.user_id "
                        + "INNER JOIN Payments p ON Users.user_id = p.user_id");
    }

    @Test
    void testChainedMultiJoinHints() {
        // Tests two separate MULTI_JOIN hints in the same query
        // Inner subquery has MULTI_JOIN(o, p, s) and outer query joins with Users
        // This verifies that multiple MULTI_JOIN hints can be used at different query levels
        util.getTableEnv()
                .getConfig()
                .set(OptimizerConfigOptions.TABLE_OPTIMIZER_MULTI_JOIN_ENABLED, false);

        util.verifyRelPlan(
                "SELECT /*+ MULTI_JOIN(u, subq) */ u.user_id, u.name, subq.order_id, subq.payment_id, subq.location "
                        + "FROM Users u "
                        + "INNER JOIN ("
                        + "  SELECT /*+ MULTI_JOIN(o, p, s) */ o.order_id, o.user_id u1, p.payment_id, p.user_id u2, s.location, s.user_id u3"
                        + "  FROM Orders o "
                        + "  INNER JOIN Payments p ON o.user_id = p.user_id "
                        + "  INNER JOIN Shipments s ON p.user_id = s.user_id"
                        + ") subq ON u.user_id = subq.u1");
    }

    @Test
    void testMultipleMultiJoinHintsInDifferentBranches() {
        // Tests multiple MULTI_JOIN hints where two separate multi-joins are joined together
        // Left side: MULTI_JOIN(u, o)
        // Right side: MULTI_JOIN(p, s)
        // Then these two multi-joins are joined together
        util.getTableEnv()
                .getConfig()
                .set(OptimizerConfigOptions.TABLE_OPTIMIZER_MULTI_JOIN_ENABLED, false);

        util.verifyRelPlan(
                "SELECT left_side.user_id, left_side.order_id, right_side.payment_id, right_side.location "
                        + "FROM ("
                        + "  SELECT /*+ MULTI_JOIN(u, o) */ u.user_id, o.order_id, o.user_id ouid "
                        + "  FROM Users u "
                        + "  INNER JOIN Orders o ON u.user_id = o.user_id"
                        + ") left_side "
                        + "INNER JOIN ("
                        + "  SELECT /*+ MULTI_JOIN(p, s) */ p.payment_id, p.user_id, s.location, s.user_id suid "
                        + "  FROM Payments p "
                        + "  INNER JOIN Shipments s ON p.user_id = s.user_id"
                        + ") right_side "
                        + "ON left_side.user_id = right_side.user_id");
    }

    @Test
    void testThreeWayLeftOuterJoinExecPlan() {
        util.verifyExecPlan(
                "\nSELECT\n"
                        + "    u.user_id,\n"
                        + "    u.name,\n"
                        + "    o.order_id,\n"
                        + "    p.payment_id\n"
                        + "FROM Users u\n"
                        + "LEFT JOIN Orders o\n"
                        + "    ON u.user_id = o.user_id\n"
                        + "LEFT JOIN Payments p\n"
                        + "    ON u.user_id = p.user_id");
    }

    @Test
    void testTwoWayJoinWithUnion() {
        util.tableEnv()
                .executeSql(
                        "CREATE TABLE Orders2 ("
                                + "  order_id STRING PRIMARY KEY NOT ENFORCED,"
                                + "  user_id STRING,"
                                + "  product STRING"
                                + ") WITH ('connector' = 'values', 'changelog-mode' = 'I,D')");

        util.verifyRelPlan(
                "\nWITH OrdersUnion as (\n"
                        + "SELECT * FROM Orders\n"
                        + "UNION ALL\n"
                        + "SELECT * FROM Orders2\n"
                        + ")\n"
                        + "SELECT * FROM OrdersUnion o\n"
                        + "LEFT JOIN Users u\n"
                        + "    ON o.user_id = u.user_id");
    }

    @Test
    void testTwoWayJoinWithRank() {
        util.verifyRelPlan(
                "\nWITH JoinedEvents as (\n"
                        + "SELECT\n"
                        + "    e1.id as id,\n"
                        + "    e1.val,\n"
                        + "    e1.`$rowtime` as `$rowtime`,\n"
                        + "    e2.price\n"
                        + "FROM EventTable1 e1\n"
                        + "JOIN EventTable2 e2\n"
                        + "    ON e1.id = e2.id)\n"
                        + "SELECT\n"
                        + "    id,\n"
                        + "    val,\n"
                        + "    `$rowtime`\n"
                        + "FROM (\n"
                        + "    SELECT\n"
                        + "        *,\n"
                        + "        ROW_NUMBER() OVER (PARTITION BY id ORDER BY `$rowtime` DESC) as ts\n"
                        + "    FROM JoinedEvents)\n"
                        + "WHERE ts = 1");
    }

    @Test
    void testFourWayComplexJoinRelPlan() {
        util.verifyRelPlan(
                "\nSELECT\n"
                        + "    u.user_id,\n"
                        + "    u.name,\n"
                        + "    o.order_id,\n"
                        + "    p.payment_id,\n"
                        + "    s.location\n"
                        + "FROM Users u\n"
                        + "LEFT JOIN Orders o\n"
                        + "    ON u.user_id = o.user_id\n"
                        + "INNER JOIN Payments p\n"
                        + "    ON u.user_id = p.user_id\n"
                        + "    AND (u.cash >= p.price OR p.price < 0)\n"
                        + "LEFT JOIN Shipments s\n"
                        + "    ON p.user_id = s.user_id");
    }

    @Test
    @Tag("no-common-join-key")
    void testThreeWayJoinExecPlanNoCommonJoinKey() {
        util.verifyExecPlan(
                "\nSELECT\n"
                        + "    u.user_id,\n"
                        + "    u.name,\n"
                        + "    o.order_id,\n"
                        + "    p.payment_id\n"
                        + "FROM Users u\n"
                        + "LEFT JOIN Orders o ON TRUE\n"
                        + "INNER JOIN Payments p ON TRUE");
    }

    @Test
    @Tag("no-common-join-key")
    void testFourWayJoinRelPlanNoCommonJoinKey() {
        util.verifyRelPlan(
                "\nSELECT\n"
                        + "    u.user_id,\n"
                        + "    u.name,\n"
                        + "    o.order_id,\n"
                        + "    p.payment_id,\n"
                        + "    s.location\n"
                        + "FROM Users u\n"
                        + "LEFT JOIN Orders o\n"
                        + "    ON u.user_id = o.user_id\n"
                        + "LEFT JOIN LookupTable\n"
                        + "    ON u.name = LookupTable.name\n"
                        + "LEFT JOIN Payments p\n"
                        + "    ON u.user_id = p.user_id\n"
                        + "LEFT JOIN Shipments s\n"
                        + "    ON o.user_id = s.user_id");
    }

    @Test
    void testFourWayComplexJoinExecPlan() {
        util.verifyExecPlan(
                "\nSELECT\n"
                        + "    u.user_id,\n"
                        + "    u.name,\n"
                        + "    o.order_id,\n"
                        + "    p.payment_id,\n"
                        + "    s.location\n"
                        + "FROM Users u\n"
                        + "LEFT JOIN Orders o\n"
                        + "    ON u.user_id = o.user_id\n"
                        + "INNER JOIN Payments p\n"
                        + "    ON u.user_id = p.user_id\n"
                        + "    AND (u.cash >= p.price OR p.price < 0)\n"
                        + "LEFT JOIN Shipments s\n"
                        + "    ON p.user_id = s.user_id");
    }

    @Test
    void testThreeWayInnerJoinExplain() {
        util.verifyExplain(
                "\nSELECT\n"
                        + "    u.user_id,\n"
                        + "    u.name,\n"
                        + "    o.order_id,\n"
                        + "    p.payment_id\n"
                        + "FROM Users u\n"
                        + "INNER JOIN Orders o\n"
                        + "    ON u.user_id = o.user_id\n"
                        + "INNER JOIN Payments p\n"
                        + "    ON u.user_id = p.user_id");
    }

    @Test
    void testThreeWayLeftOuterJoinExplain() {
        util.verifyExplain(
                "\nSELECT\n"
                        + "    u.user_id,\n"
                        + "    u.name,\n"
                        + "    o.order_id,\n"
                        + "    p.payment_id\n"
                        + "FROM Users u\n"
                        + "LEFT JOIN Orders o\n"
                        + "    ON u.user_id = o.user_id\n"
                        + "LEFT JOIN Payments p\n"
                        + "    ON u.user_id = p.user_id");
    }

    @Test
    void testFourWayComplexJoinExplain() {
        util.verifyExplain(
                "\nSELECT\n"
                        + "    u.user_id,\n"
                        + "    u.name,\n"
                        + "    o.order_id,\n"
                        + "    p.payment_id,\n"
                        + "    s.location\n"
                        + "FROM Users u\n"
                        + "LEFT JOIN Orders o\n"
                        + "    ON u.user_id = o.user_id\n"
                        + "INNER JOIN Payments p\n"
                        + "    ON u.user_id = p.user_id\n"
                        + "    AND (u.cash >= p.price OR p.price < 0)\n"
                        + "LEFT JOIN Shipments s\n"
                        + "    ON p.user_id = s.user_id");
    }

    @Test
    void testTemporalJoinExcludedFromMultiJoin() {
        // Temporal joins should remain as lookup joins, not be merged into MultiJoin
        util.verifyRelPlan(
                "\nSELECT\n"
                        + "    s.user_id,\n"
                        + "    s.amount,\n"
                        + "    l.name,\n"
                        + "    l.age\n"
                        + "FROM StreamTable s\n"
                        + "JOIN LookupTable FOR SYSTEM_TIME AS OF s.proctime AS l\n"
                        + "    ON s.user_id = l.id");
    }

    @Test
    void testIntervalJoinExcludedFromMultiJoin() {
        // Interval joins (event-time and processing-time) should remain as interval joins
        util.verifyRelPlan(
                "\nSELECT\n"
                        + "    e1.id,\n"
                        + "    e1.val,\n"
                        + "    e2.price\n"
                        + "FROM EventTable1 e1\n"
                        + "JOIN EventTable2 e2\n"
                        + "    ON e1.id = e2.id\n"
                        + "    AND e1.`$rowtime` BETWEEN e2.`$rowtime` - INTERVAL '1' MINUTE\n"
                        + "    AND e2.`$rowtime` + INTERVAL '1' MINUTE");
    }

    @Test
    void testThreeWayLeftOuterJoinWithWhereClauseRelPlan() {
        util.verifyRelPlan(
                "\nSELECT\n"
                        + "    u.user_id,\n"
                        + "    u.name,\n"
                        + "    o.order_id,\n"
                        + "    p.payment_id\n"
                        + "FROM Users u\n"
                        + "LEFT JOIN Orders o\n"
                        + "    ON u.user_id = o.user_id\n"
                        + "LEFT JOIN Payments p\n"
                        + "    ON u.user_id = p.user_id\n"
                        + "WHERE u.name = 'Gus' AND p.price > 10");
    }

    @Test
    void testThreeWayLeftOuterJoinWithWhereClauseExecPlan() {
        util.verifyExecPlan(
                "\nSELECT\n"
                        + "    u.user_id,\n"
                        + "    u.name,\n"
                        + "    o.order_id,\n"
                        + "    p.payment_id\n"
                        + "FROM Users u\n"
                        + "LEFT JOIN Orders o\n"
                        + "    ON u.user_id = o.user_id\n"
                        + "LEFT JOIN Payments p\n"
                        + "    ON u.user_id = p.user_id\n"
                        + "WHERE u.name = 'Gus' AND p.price > 10");
    }

    @Test
    void testThreeWayLeftOuterJoinWithWhereClauseExplain() {
        util.verifyExplain(
                "\nSELECT\n"
                        + "    u.user_id,\n"
                        + "    u.name,\n"
                        + "    o.order_id,\n"
                        + "    p.payment_id\n"
                        + "FROM Users u\n"
                        + "LEFT JOIN Orders o\n"
                        + "    ON u.user_id = o.user_id\n"
                        + "LEFT JOIN Payments p\n"
                        + "    ON u.user_id = p.user_id\n"
                        + "WHERE u.name = 'Gus' AND p.price > 10");
    }

    @Test
    void testRegularJoinsAreMergedApartFromTemporalJoin() {
        // Regular joins should still be eligible for MultiJoin but not mixed with temporal joins
        util.verifyRelPlan(
                "\nSELECT\n"
                        + "    u.user_id,\n"
                        + "    u.name,\n"
                        + "    o.order_id,\n"
                        + "    temporal.age "
                        + "FROM Users u\n"
                        + "INNER JOIN Orders o\n"
                        + "    ON u.user_id = o.user_id\n"
                        + "INNER JOIN (\n"
                        + "    SELECT s.user_id, l.age\n"
                        + "    FROM StreamTable s\n"
                        + "    JOIN LookupTable FOR SYSTEM_TIME AS OF s.proctime AS l\n"
                        + "        ON s.user_id = l.id\n"
                        + ") temporal ON u.user_id = temporal.user_id");
    }

    @Test
    void testFourWayJoinTransitiveCommonJoinKeyRelPlan() {
        util.verifyRelPlan(
                "\nSELECT\n"
                        + "    u.user_id,\n"
                        + "    u.name,\n"
                        + "    o.order_id,\n"
                        + "    p.payment_id,\n"
                        + "    s.location\n"
                        + "FROM Users u\n"
                        + "LEFT JOIN Orders o\n"
                        + "    ON u.user_id = o.user_id\n"
                        + "LEFT JOIN Payments p\n"
                        + "    ON o.user_id = p.user_id\n"
                        + "LEFT JOIN Shipments s\n"
                        + "    ON p.user_id = s.user_id");
    }

    /* Update this to supported with FLINK-37973 https://issues.apache.org/jira/browse/FLINK-37973 */
    @Test
    void testRightJoinNotSupported() {
        util.verifyRelPlan(
                "\nSELECT\n"
                        + "    u.user_id,\n"
                        + "    u.name,\n"
                        + "    o.order_id,\n"
                        + "    p.payment_id\n"
                        + "FROM Users u\n"
                        + "RIGHT JOIN Orders o\n"
                        + "    ON u.user_id = o.user_id\n"
                        + "RIGHT JOIN Payments p\n"
                        + "    ON o.user_id = p.user_id");
    }

    @Test
    void testFullOuterNotSupported() {
        util.verifyRelPlan(
                "\nSELECT\n"
                        + "    u.user_id,\n"
                        + "    u.name,\n"
                        + "    o.order_id,\n"
                        + "    p.payment_id\n"
                        + "FROM Users u\n"
                        + "FULL OUTER JOIN Orders o\n"
                        + "    ON u.user_id = o.user_id\n"
                        + "FULL OUTER JOIN Payments p\n"
                        + "    ON o.user_id = p.user_id");
    }

    @Test
    void testThreeWayJoinWithTimeAttributesMaterialization() {
        util.verifyRelPlan(
                "\nSELECT\n"
                        + "    u.name,\n"
                        + "    u.proctime,\n"
                        + "    o.`$rowtime`,\n"
                        + "    p.price\n"
                        + "FROM UsersWithProctime u\n"
                        + "JOIN OrdersWithRowtime o\n"
                        + "    ON u.user_id = o.user_id\n"
                        + "JOIN Payments p\n"
                        + "    ON u.user_id = p.user_id");
    }

    @Test
    void testPreservesUpsertKeyTwoWayLeftJoinOrders() {
        util.tableEnv()
                .executeSql(
                        "CREATE TABLE sink_two_way ("
                                + "  `user_id` STRING NOT NULL,"
                                + "  `order_id` STRING NOT NULL,"
                                + "  product STRING,"
                                + "  user_region_id INT,"
                                + "  CONSTRAINT `PRIMARY` PRIMARY KEY (`user_id`, `order_id`) NOT ENFORCED"
                                + ") WITH ("
                                + "  'connector' = 'values',"
                                + "  'sink-insert-only' = 'false'"
                                + ")");

        util.verifyRelPlanInsert(
                "\nINSERT INTO sink_two_way\n"
                        + "SELECT\n"
                        + "    o.user_id,\n"
                        + "    o.order_id,\n"
                        + "    o.product,\n"
                        + "    u.region_id\n"
                        + "FROM OrdersPK o\n"
                        + "LEFT JOIN UsersPK u\n"
                        + "    ON u.user_id = o.user_id");
    }

    @Test
    void testPreservesUpsertKeyTwoWayInnerJoinOrders() {
        util.tableEnv()
                .executeSql(
                        "CREATE TABLE sink_two_way ("
                                + "  `user_id` STRING NOT NULL,"
                                + "  `order_id` STRING NOT NULL,"
                                + "  product STRING,"
                                + "  user_region_id INT,"
                                + "  CONSTRAINT `PRIMARY` PRIMARY KEY (`user_id`, `order_id`) NOT ENFORCED"
                                + ") WITH ("
                                + "  'connector' = 'values',"
                                + "  'sink-insert-only' = 'false'"
                                + ")");

        util.verifyRelPlanInsert(
                "\nINSERT INTO sink_two_way\n"
                        + "SELECT\n"
                        + "    o.user_id,\n"
                        + "    o.order_id,\n"
                        + "    o.product,\n"
                        + "    u.region_id\n"
                        + "FROM UsersPK u\n"
                        + "INNER JOIN OrdersPK o\n"
                        + "    ON u.user_id = o.user_id");
    }

    @Test
    void testPreservesUpsertKeyTwoWayInnerJoinOrdersDoesNot() {
        util.tableEnv()
                .executeSql(
                        "CREATE TABLE OrdersSimplePK ("
                                + "  order_id STRING NOT NULL,"
                                + "  user_id STRING NOT NULL,"
                                + "  product STRING,"
                                + "  CONSTRAINT `PRIMARY` PRIMARY KEY (order_id) NOT ENFORCED"
                                + ") WITH ('connector' = 'values', 'changelog-mode' = 'I,UA,D')");

        util.tableEnv()
                .executeSql(
                        "CREATE TABLE sink_two_way ("
                                + "  `user_id` STRING NOT NULL,"
                                + "  `order_id` STRING NOT NULL,"
                                + "  product STRING,"
                                + "  user_region_id INT,"
                                + "  CONSTRAINT `PRIMARY` PRIMARY KEY (`order_id`) NOT ENFORCED"
                                + ") WITH ("
                                + "  'connector' = 'values',"
                                + "  'sink-insert-only' = 'false'"
                                + ")");

        util.verifyRelPlanInsert(
                "\nINSERT INTO sink_two_way\n"
                        + "SELECT\n"
                        + "    o.user_id,\n"
                        + "    o.order_id,\n"
                        + "    o.product,\n"
                        + "    u.region_id\n"
                        + "FROM UsersPK u\n"
                        + "INNER JOIN OrdersSimplePK o\n"
                        + "    ON u.user_id = o.user_id");
    }

    @Test
    void testPreservesUpsertKeyThreeWayJoin() {
        util.tableEnv()
                .executeSql(
                        "CREATE TABLE sink_three_way ("
                                + "  `user_id` STRING NOT NULL,"
                                + "  `order_id` STRING NOT NULL,"
                                + "  `user_id2` STRING NOT NULL,"
                                + "  `payment_id` STRING NOT NULL,"
                                + "  `user_id3` STRING NOT NULL,"
                                + "  `description` STRING,"
                                + "  CONSTRAINT `PRIMARY` PRIMARY KEY (`user_id`, `order_id`, `user_id2`, `payment_id`, `user_id3`) NOT ENFORCED"
                                + ") WITH ("
                                + "  'connector' = 'values',"
                                + "  'sink-insert-only' = 'false'"
                                + ")");

        util.verifyRelPlanInsert(
                "\nINSERT INTO sink_three_way\n"
                        + "SELECT\n"
                        + "    o.user_id,\n"
                        + "    o.order_id,\n"
                        + "    p.user_id,\n"
                        + "    p.payment_id,\n"
                        + "    u.user_id,\n"
                        + "    u.description\n"
                        + "FROM UsersPK u\n"
                        + "JOIN OrdersPK o\n"
                        + "    ON o.user_id = u.user_id\n"
                        + "JOIN PaymentsPK p\n"
                        + "    ON o.user_id = p.user_id");
    }

    @Test
    void testPreservesUpsertKeyFourWayComplex() {
        util.tableEnv()
                .executeSql(
                        "CREATE TABLE sink_four_way ("
                                + "  user_id STRING NOT NULL,"
                                + "  order_id STRING NOT NULL,"
                                + "  user_id1 STRING NOT NULL,"
                                + "  payment_id STRING NOT NULL,"
                                + "  user_id2 STRING NOT NULL,"
                                + "  name STRING,"
                                + "  location STRING,"
                                + "  CONSTRAINT `PRIMARY` PRIMARY KEY (`user_id`, `order_id`, `user_id1`, `payment_id`, `user_id2`) NOT ENFORCED"
                                + ") WITH ("
                                + "  'connector' = 'values',"
                                + "  'sink-insert-only' = 'false'"
                                + ")");

        util.verifyRelPlanInsert(
                "\nINSERT INTO sink_four_way\n"
                        + "SELECT\n"
                        + "    u.user_id,\n"
                        + "    o.order_id,\n"
                        + "    o.user_id,\n"
                        + "    p.payment_id,\n"
                        + "    p.user_id,\n"
                        + "    u.name,\n"
                        + "    a.location\n"
                        + "FROM UsersPK u\n"
                        + "JOIN OrdersPK o\n"
                        + "    ON u.user_id = o.user_id\n"
                        + "    AND o.product IS NOT NULL\n"
                        + "JOIN PaymentsPK p\n"
                        + "    ON u.user_id = p.user_id\n"
                        + "    AND p.price >= 0\n"
                        + "JOIN AddressPK a\n"
                        + "    ON u.user_id = a.user_id\n"
                        + "    AND a.location IS NOT NULL");
    }

    @Test
    void testMultiSinkOnMultiJoinedView() {
        util.tableEnv()
                .executeSql(
                        "create temporary table src1 (\n"
                                + "  a int,\n"
                                + "  b bigint,\n"
                                + "  c string,\n"
                                + "  d int,\n"
                                + "  primary key(a, c) not enforced\n"
                                + ") with (\n"
                                + " 'connector' = 'values',\n"
                                + " 'changelog-mode' = 'I,UA,UB,D'\n"
                                + ")");
        util.tableEnv()
                .executeSql(
                        "create temporary table src2 (\n"
                                + "  a int,\n"
                                + "  b bigint,\n"
                                + "  c string,\n"
                                + "  d int,\n"
                                + "  primary key(a, c) not enforced\n"
                                + ") with (\n"
                                + " 'connector' = 'values',\n"
                                + " 'changelog-mode' = 'I,UA,UB,D'\n"
                                + ")");
        util.tableEnv()
                .executeSql(
                        "create temporary table sink1 (\n"
                                + "  a int,\n"
                                + "  b string,\n"
                                + "  c bigint,\n"
                                + "  d bigint\n"
                                + ") with (\n"
                                + " 'connector' = 'values',\n"
                                + " 'sink-insert-only' = 'false'\n"
                                + ")");
        util.tableEnv()
                .executeSql(
                        "create temporary table sink2 (\n"
                                + "  a int,\n"
                                + "  b string,\n"
                                + "  c bigint,\n"
                                + "  d string\n"
                                + ") with (\n"
                                + " 'connector' = 'values',\n"
                                + " 'sink-insert-only' = 'false'\n"
                                + ")");
        util.tableEnv()
                .executeSql(
                        "create temporary view v1 as\n"
                                + "select\n"
                                + "  t1.a as a, t1.`day` as `day`, t2.b as b, t2.c as c\n"
                                + "from (\n"
                                + "  select a, b, DATE_FORMAT(CURRENT_TIMESTAMP, 'yyMMdd') as `day`\n"
                                + "  from src1\n"
                                + " ) t1\n"
                                + "join (\n"
                                + "  select b, CONCAT(c, DATE_FORMAT(CURRENT_TIMESTAMP, 'yyMMdd')) as `day`, c, d\n"
                                + "  from src2\n"
                                + ") t2\n"
                                + " on t1.a = t2.d");

        StatementSet stmtSet = util.tableEnv().createStatementSet();
        stmtSet.addInsertSql(
                "insert into sink1\n"
                        + "  select a, `day`, sum(b), count(distinct c)\n"
                        + "  from v1\n"
                        + "  group by a, `day`");
        stmtSet.addInsertSql(
                "insert into sink2\n"
                        + "  select a, `day`, b, c\n"
                        + "  from v1\n"
                        + "  where b > 100");

        util.doVerifyPlan(
                stmtSet,
                new ExplainDetail[] {ExplainDetail.PLAN_ADVICE},
                false,
                new Enumeration.Value[] {PlanKind.OPT_REL_WITH_ADVICE()},
                () -> UNIT,
                false,
                false);
    }

    /*
     * Calcite adds a LogicalProject to compute expressions such as UPPER and FLOOR
     * on the necessary fields. As a result, the planner cannot fuse all joins into
     * a single MultiJoin node initially.
     */
    @Test
    @Tag("multijoin-chain-expected")
    void testFourWayJoinWithFunctionInConditionMultiJoinChainExpected() {
        util.verifyRelPlan(
                "\nSELECT\n"
                        + "    u.user_id,\n"
                        + "    u.name,\n"
                        + "    o.order_id,\n"
                        + "    p.payment_id,\n"
                        + "    s.location\n"
                        + "FROM Users u\n"
                        + "LEFT JOIN Orders o\n"
                        + "    ON o.user_id = u.user_id\n"
                        + "LEFT JOIN Payments p\n"
                        + "    ON u.user_id = p.user_id\n"
                        + "    AND UPPER(u.name) = UPPER(p.payment_id)\n"
                        + "    AND (FLOOR(u.cash) >= FLOOR(p.price) OR p.price < 0)\n"
                        + "LEFT JOIN Shipments s\n"
                        + "    ON p.payment_id = s.location");
    }

    /*
     * We expect the join inputs to **not** merge into a single MultiJoin node in this case,
     * because `documents.common_id` is different from `other_documents.common_id`.
     */
    @Test
    @Tag("no-common-join-key")
    void testComplexCommonJoinKeyMissingProjectionNoCommonJoinKey() {
        util.tableEnv()
                .executeSql(
                        "CREATE TABLE Assignments ("
                                + "  assignment_id STRING PRIMARY KEY NOT ENFORCED,"
                                + "  user_id STRING,"
                                + "  detail_id STRING,"
                                + "  common_id STRING"
                                + ") WITH ('connector' = 'values', 'changelog-mode' = 'I,UA,D')");

        util.tableEnv()
                .executeSql(
                        "CREATE TABLE Documents ("
                                + "  detail_id STRING PRIMARY KEY NOT ENFORCED,"
                                + "  creator_nm STRING,"
                                + "  common_id STRING"
                                + ") WITH ('connector' = 'values', 'changelog-mode' = 'I,UA,D')");

        util.verifyRelPlan(
                "\nSELECT *\n"
                        + "FROM Assignments assignments\n"
                        + "LEFT JOIN Documents AS documents\n"
                        + "    ON assignments.detail_id = documents.detail_id\n"
                        + "    AND assignments.common_id = documents.common_id\n"
                        + "LEFT JOIN Documents AS other_documents\n"
                        + "    ON assignments.user_id = other_documents.common_id");
    }

    @Test
    void testComplexCommonJoinKey() {
        util.tableEnv()
                .executeSql(
                        "CREATE TABLE Assignments ("
                                + "  assignment_id STRING PRIMARY KEY NOT ENFORCED,"
                                + "  user_id STRING,"
                                + "  detail_id STRING,"
                                + "  common_id STRING"
                                + ") WITH ('connector' = 'values', 'changelog-mode' = 'I,UA,D')");

        util.tableEnv()
                .executeSql(
                        "CREATE TABLE Customers ("
                                + "  user_id STRING PRIMARY KEY NOT ENFORCED,"
                                + "  name STRING,"
                                + "  depart_num STRING,"
                                + "  common_id STRING"
                                + ") WITH ('connector' = 'values', 'changelog-mode' = 'I,UA,D')");

        util.tableEnv()
                .executeSql(
                        "CREATE TABLE Documents ("
                                + "  detail_id STRING PRIMARY KEY NOT ENFORCED,"
                                + "  creator_nm STRING,"
                                + "  common_id STRING"
                                + ") WITH ('connector' = 'values', 'changelog-mode' = 'I,UA,D')");

        util.tableEnv()
                .executeSql(
                        "CREATE TABLE PhaseDetails ("
                                + "  phase_id STRING PRIMARY KEY NOT ENFORCED,"
                                + "  common_id STRING"
                                + ") WITH ('connector' = 'values', 'changelog-mode' = 'I,UA,D')");

        util.tableEnv()
                .executeSql(
                        "CREATE TABLE Organizations ("
                                + "  org_id STRING PRIMARY KEY NOT ENFORCED,"
                                + "  org_name STRING,"
                                + "  common_id STRING"
                                + ") WITH ('connector' = 'values', 'changelog-mode' = 'I,UA,D')");

        util.verifyExecPlan(
                "\nSELECT *\n"
                        + "FROM Assignments assignments\n"
                        + "LEFT JOIN Customers AS customer\n"
                        + "    ON assignments.user_id = customer.user_id\n"
                        + "    AND assignments.common_id = customer.common_id\n"
                        + "LEFT JOIN Documents AS documents\n"
                        + "    ON assignments.detail_id = documents.detail_id\n"
                        + "    AND assignments.common_id = documents.common_id\n"
                        + "LEFT JOIN PhaseDetails AS phase_details\n"
                        + "    ON documents.common_id = phase_details.common_id\n"
                        + "LEFT JOIN Organizations AS organizations\n"
                        + "    ON customer.depart_num = organizations.org_id\n"
                        + "    AND customer.common_id = organizations.common_id\n"
                        + "LEFT JOIN Customers AS creators\n"
                        + "    ON documents.creator_nm = creators.depart_num\n"
                        + "    AND documents.common_id = creators.common_id");
    }

    @Test
    @Tag("no-common-join-key")
    void testComplexConditionalLogicWithMultiJoinNoCommonJoinKey() {
        util.tableEnv()
                .executeSql(
                        "CREATE TABLE ProductCategories ("
                                + "  category_id STRING PRIMARY KEY NOT ENFORCED,"
                                + "  category_name STRING,"
                                + "  is_premium BOOLEAN,"
                                + "  discount_rate DOUBLE"
                                + ") WITH ('connector' = 'values', 'changelog-mode' = 'I,UA,D')");

        util.tableEnv()
                .executeSql(
                        "CREATE TABLE ProductReviews ("
                                + "  review_id STRING PRIMARY KEY NOT ENFORCED,"
                                + "  product_id STRING,"
                                + "  rating INT,"
                                + "  is_verified BOOLEAN"
                                + ") WITH ('connector' = 'values', 'changelog-mode' = 'I,UA,D')");

        util.verifyRelPlan(
                "\nSELECT\n"
                        + "    u.user_id,\n"
                        + "    o.order_id,\n"
                        + "    p.payment_id,\n"
                        + "    pc.category_name,\n"
                        + "    CASE\n"
                        + "        WHEN pc.is_premium = true AND p.price > 1000 THEN 'High-Value Premium'\n"
                        + "        WHEN pc.is_premium = true THEN 'Premium'\n"
                        + "        WHEN p.price > 500 THEN 'Standard High-Value'\n"
                        + "        ELSE 'Standard'\n"
                        + "    END AS product_tier,\n"
                        + "    CASE\n"
                        + "        WHEN pr.rating >= 4 AND pr.is_verified = true THEN 'Highly Recommended'\n"
                        + "        WHEN pr.rating >= 3 THEN 'Recommended'\n"
                        + "        WHEN pr.rating >= 2 THEN 'Average'\n"
                        + "        ELSE 'Not Recommended'\n"
                        + "    END AS recommendation_status,\n"
                        + "    CASE\n"
                        + "        WHEN pc.discount_rate > 0.2 THEN p.price * (1 - pc.discount_rate)\n"
                        + "        ELSE p.price\n"
                        + "    END AS final_price\n"
                        + "FROM Users u\n"
                        + "LEFT JOIN Orders o\n"
                        + "    ON u.user_id = o.user_id\n"
                        + "LEFT JOIN Payments p\n"
                        + "    ON u.user_id = p.user_id\n"
                        + "LEFT JOIN ProductCategories pc\n"
                        + "    ON o.product = pc.category_id\n"
                        + "LEFT JOIN ProductReviews pr\n"
                        + "    ON o.product = pr.product_id");
    }

    @Test
    @Tag("no-common-join-key")
    void testComplexCTEWithMultiJoinNoCommonJoinKey() {
        util.tableEnv()
                .executeSql(
                        "CREATE TABLE OrderStatus ("
                                + "  status_id STRING PRIMARY KEY NOT ENFORCED,"
                                + "  status_name STRING,"
                                + "  is_final BOOLEAN"
                                + ") WITH ('connector' = 'values', 'changelog-mode' = 'I,UA,D')");

        util.tableEnv()
                .executeSql(
                        "CREATE TABLE PaymentMethods ("
                                + "  method_id STRING PRIMARY KEY NOT ENFORCED,"
                                + "  method_name STRING,"
                                + "  processing_fee DOUBLE"
                                + ") WITH ('connector' = 'values', 'changelog-mode' = 'I,UA,D')");

        util.verifyRelPlan(
                "\nWITH user_orders AS (\n"
                        + "    SELECT u.user_id, u.name, o.order_id, o.product, p.payment_id, p.price\n"
                        + "    FROM Users u\n"
                        + "    LEFT JOIN Orders o ON\n"
                        + "        u.user_id = o.user_id\n"
                        + "    LEFT JOIN Payments p\n"
                        + "        ON u.user_id = p.user_id\n"
                        + "),\n"
                        + "order_details AS (\n"
                        + "    SELECT uo.*, os.status_name, os.is_final, pm.method_name, pm.processing_fee\n"
                        + "    FROM user_orders uo\n"
                        + "    LEFT JOIN OrderStatus os\n"
                        + "        ON uo.order_id = os.status_id\n"
                        + "    LEFT JOIN PaymentMethods pm\n"
                        + "        ON uo.payment_id = pm.method_id\n"
                        + "),\n"
                        + "final_summary AS (\n"
                        + "    SELECT\n"
                        + "        user_id,\n"
                        + "        name,\n"
                        + "        COUNT(order_id) as total_orders,\n"
                        + "        SUM(price) as total_spent,\n"
                        + "        AVG(price) as avg_order_value,\n"
                        + "        COUNT(CASE WHEN is_final = true THEN 1 END) as completed_orders\n"
                        + "    FROM order_details\n"
                        + "    GROUP BY user_id, name\n"
                        + ")\n"
                        + "SELECT * FROM final_summary");
    }

    @Test
    @Tag("no-common-join-key")
    void testAggregationAndGroupingWithMultiJoinNoCommonJoinKey() {
        util.tableEnv()
                .executeSql(
                        "CREATE TABLE OrderItems ("
                                + "  item_id STRING PRIMARY KEY NOT ENFORCED,"
                                + "  order_id STRING,"
                                + "  product_name STRING,"
                                + "  quantity INT,"
                                + "  unit_price DOUBLE"
                                + ") WITH ('connector' = 'values', 'changelog-mode' = 'I,UA,D')");

        util.tableEnv()
                .executeSql(
                        "CREATE TABLE ProductCategories ("
                                + "  category_id STRING PRIMARY KEY NOT ENFORCED,"
                                + "  category_name STRING,"
                                + "  parent_category STRING"
                                + ") WITH ('connector' = 'values', 'changelog-mode' = 'I,UA,D')");

        util.verifyRelPlan(
                "\nSELECT\n"
                        + "    u.user_id,\n"
                        + "    u.name,\n"
                        + "    pc.category_name,\n"
                        + "    COUNT(DISTINCT o.order_id) as order_count,\n"
                        + "    SUM(oi.quantity) as total_items,\n"
                        + "    SUM(oi.quantity * oi.unit_price) as total_value,\n"
                        + "    AVG(oi.unit_price) as avg_item_price,\n"
                        + "    MAX(p.price) as max_payment,\n"
                        + "    MIN(p.price) as min_payment,\n"
                        + "    COUNT(CASE WHEN oi.quantity > 5 THEN 1 END) as bulk_orders\n"
                        + "FROM Users u\n"
                        + "LEFT JOIN Orders o\n"
                        + "    ON u.user_id = o.user_id\n"
                        + "LEFT JOIN OrderItems oi\n"
                        + "    ON o.order_id = oi.order_id\n"
                        + "LEFT JOIN ProductCategories pc\n"
                        + "    ON oi.product_name = pc.category_id\n"
                        + "LEFT JOIN Payments p\n"
                        + "    ON u.user_id = p.user_id\n"
                        + "GROUP BY u.user_id, u.name, pc.category_name\n"
                        + "HAVING COUNT(DISTINCT o.order_id) > 0");
    }

    @Test
    @Tag("no-common-join-key")
    void testFunctionAndExpressionWithMultiJoinNoCommonJoinKey() {
        util.tableEnv()
                .executeSql(
                        "CREATE TABLE ProductDetails ("
                                + "  product_id STRING PRIMARY KEY NOT ENFORCED,"
                                + "  product_name STRING,"
                                + "  description STRING,"
                                + "  created_date BIGINT,"
                                + "  tags STRING"
                                + ") WITH ('connector' = 'values', 'changelog-mode' = 'I,UA,D')");

        util.tableEnv()
                .executeSql(
                        "CREATE TABLE UserPreferences ("
                                + "  user_id STRING PRIMARY KEY NOT ENFORCED,"
                                + "  preferred_category STRING,"
                                + "  notification_level STRING"
                                + ") WITH ('connector' = 'values', 'changelog-mode' = 'I,UA,D')");

        util.verifyRelPlan(
                "\nSELECT\n"
                        + "    u.user_id,\n"
                        + "    UPPER(u.name) as user_name_upper,\n"
                        + "    LOWER(o.product) as product_lower,\n"
                        + "    CONCAT(u.name, ' - ', o.product) as user_product,\n"
                        + "    SUBSTRING(pd.description, 1, 50) as description_preview,\n"
                        + "    CHAR_LENGTH(pd.description) as description_length,\n"
                        + "    FLOOR(p.price / 100.0) * 100 as price_rounded,\n"
                        + "    CASE\n"
                        + "        WHEN p.price > 1000 THEN 'High'\n"
                        + "        WHEN p.price > 500 THEN 'Medium'\n"
                        + "        ELSE 'Low'\n"
                        + "    END as price_tier,\n"
                        + "    REGEXP_REPLACE(pd.tags, ',', ' | ') as formatted_tags,\n"
                        + "    TO_TIMESTAMP_LTZ(pd.created_date, 3) as product_created,\n"
                        + "    COALESCE(up.preferred_category, 'None') as user_preference,\n"
                        + "    CASE\n"
                        + "        WHEN up.notification_level = 'HIGH' THEN 'Frequent Updates'\n"
                        + "        WHEN up.notification_level = 'MEDIUM' THEN 'Daily Updates'\n"
                        + "        ELSE 'Weekly Updates'\n"
                        + "    END as notification_frequency\n"
                        + "FROM Users u\n"
                        + "LEFT JOIN Orders o\n"
                        + "    ON u.user_id = o.user_id\n"
                        + "LEFT JOIN Payments p\n"
                        + "    ON u.user_id = p.user_id\n"
                        + "LEFT JOIN ProductDetails pd\n"
                        + "    ON o.product = pd.product_id\n"
                        + "LEFT JOIN UserPreferences up\n"
                        + "    ON u.user_id = up.user_id");
    }

    /*
     * Calcite automatically generates LogicalProject nodes for nested field access.
     * As a result, each join input in this test is wrapped in a projection, which prevents
     * the planner from fusing all joins into a single MultiJoin node initially.
     * Therefore, in this test, each Join is still converted to a MultiJoin individually.
     */
    @Test
    @Tag("multijoin-chain-expected")
    void testJoinConditionHasNestedFieldsMultiJoinChainExpected() {
        util.tableEnv()
                .executeSql(
                        "CREATE TABLE Developers ("
                                + "  developer_id STRING PRIMARY KEY NOT ENFORCED,"
                                + "  person ROW<info ROW<id STRING, name STRING, region STRING>>,"
                                + "  experience_years INT"
                                + ") WITH ('connector' = 'values', 'changelog-mode' = 'I,UA,D')");

        util.tableEnv()
                .executeSql(
                        "CREATE TABLE SupportTickets ("
                                + "  ticket_id STRING PRIMARY KEY NOT ENFORCED,"
                                + "  reporter ROW<info ROW<id STRING, priority STRING>>,"
                                + "  issue STRING"
                                + ") WITH ('connector' = 'values', 'changelog-mode' = 'I,UA,D')");

        util.tableEnv()
                .executeSql(
                        "CREATE TABLE Feedback ("
                                + "  feedback_id STRING PRIMARY KEY NOT ENFORCED,"
                                + "  author ROW<info ROW<id STRING, rating INT>>,"
                                + "  message STRING"
                                + ") WITH ('connector' = 'values', 'changelog-mode' = 'I,UA,D')");

        util.tableEnv()
                .executeSql(
                        "CREATE TABLE Subscriptions ("
                                + "  sub_id STRING PRIMARY KEY NOT ENFORCED,"
                                + "  subscriber ROW<info ROW<id STRING, plan STRING>>,"
                                + "  active BOOLEAN"
                                + ") WITH ('connector' = 'values', 'changelog-mode' = 'I,UA,D')");

        util.verifyRelPlan(
                "\nSELECT\n"
                        + "    d.developer_id,\n"
                        + "    d.person.info.name AS developer_name,\n"
                        + "    s.ticket_id,\n"
                        + "    s.reporter.info.priority AS ticket_priority,\n"
                        + "    f.feedback_id,\n"
                        + "    f.author.info.rating AS feedback_rating,\n"
                        + "    sub.sub_id,\n"
                        + "    sub.subscriber.info.plan AS subscription_plan\n"
                        + "FROM Developers AS d\n"
                        + "LEFT JOIN SupportTickets AS s\n"
                        + "    ON d.person.info.id = s.reporter.info.id\n"
                        + "LEFT JOIN Feedback AS f\n"
                        + "    ON d.person.info.id = f.author.info.id\n"
                        + "LEFT JOIN Subscriptions AS sub\n"
                        + "    ON d.person.info.id = sub.subscriber.info.id");
    }

    @Test
    @Tag("multijoin-chain-expected")
    void testComplexNestedCTEWithAggregationAndFunctionsMultiJoinChainExpected() {
        util.tableEnv()
                .executeSql(
                        "CREATE TABLE OrderMetrics ("
                                + "  metric_id STRING PRIMARY KEY NOT ENFORCED,"
                                + "  order_id STRING,"
                                + "  metric_type STRING,"
                                + "  metric_value DOUBLE"
                                + ") WITH ('connector' = 'values', 'changelog-mode' = 'I,UA,D')");

        util.verifyRelPlan(
                "\nWITH base_orders AS (\n"
                        + "    SELECT u.user_id, u.name, o.order_id, p.payment_id, p.price\n"
                        + "    FROM Users u\n"
                        + "    INNER JOIN Orders o\n"
                        + "        ON u.user_id = o.user_id\n"
                        + "    INNER JOIN Payments p\n"
                        + "        ON u.user_id = p.user_id\n"
                        + "),\n"
                        + "enriched_orders AS (\n"
                        + "    SELECT\n"
                        + "        bo.*,\n"
                        + "        om.metric_type,\n"
                        + "        om.metric_value,\n"
                        + "        CASE\n"
                        + "            WHEN bo.price > 1000 THEN 'Premium'\n"
                        + "            WHEN bo.price > 500 THEN 'Standard'\n"
                        + "            ELSE 'Basic'\n"
                        + "        END as order_tier\n"
                        + "    FROM base_orders bo\n"
                        + "    LEFT JOIN OrderMetrics om\n"
                        + "        ON bo.order_id = om.order_id\n"
                        + "),\n"
                        + "aggregated_metrics AS (\n"
                        + "    SELECT\n"
                        + "        user_id,\n"
                        + "        name,\n"
                        + "        COUNT(DISTINCT order_id) as total_orders,\n"
                        + "        SUM(price) as total_spent,\n"
                        + "        AVG(price) as avg_order_value,\n"
                        + "        MAX(metric_value) as max_metric,\n"
                        + "        MIN(metric_value) as min_metric,\n"
                        + "        COUNT(CASE WHEN order_tier = 'Premium' THEN 1 END) as premium_orders\n"
                        + "    FROM enriched_orders\n"
                        + "    GROUP BY user_id, name\n"
                        + ")\n"
                        + "SELECT\n"
                        + "    user_id,\n"
                        + "    UPPER(name) as user_name,\n"
                        + "    total_orders,\n"
                        + "    ROUND(total_spent, 2) as total_spent_rounded,\n"
                        + "    ROUND(avg_order_value, 2) as avg_order_value_rounded,\n"
                        + "    CONCAT('User: ', name, ' has ', CAST(total_orders AS STRING), ' orders') as summary,\n"
                        + "    CASE\n"
                        + "        WHEN total_orders > 10 THEN 'Frequent Customer'\n"
                        + "        WHEN total_orders > 5 THEN 'Regular Customer'\n"
                        + "        ELSE 'Occasional Customer'\n"
                        + "    END as customer_type\n"
                        + "FROM aggregated_metrics\n"
                        + "WHERE total_spent > 0");
    }

    @Test
    void testJoinOfProjections() {
        util.verifyRelPlan(
                "\nSELECT u.user_id, o.order_id, o.product, p.price, s.location\n"
                        + "FROM (SELECT user_id, name, cash FROM Users WHERE cash > 100) AS u\n"
                        + "JOIN (SELECT user_id, order_id, product FROM Orders WHERE product IS NOT NULL) AS o\n"
                        + "    ON u.user_id = o.user_id\n"
                        + "LEFT JOIN (SELECT user_id, price FROM Payments WHERE price > 50) AS p\n"
                        + "    ON u.user_id = p.user_id\n"
                        + "LEFT JOIN (SELECT user_id, location FROM Shipments WHERE location IS NOT NULL) AS s\n"
                        + "    ON u.user_id = s.user_id");
    }

    @Test
    @Tag("multijoin-chain-expected")
    void testJoinWithNestedSubqueryMultiJoinChainExpected() {
        util.verifyRelPlan(
                "\nSELECT *\n"
                        + "FROM Users u\n"
                        + "JOIN (\n"
                        + "    SELECT o.user_id, o.order_id, p.payment_id, p.price\n"
                        + "    FROM Orders o\n"
                        + "    JOIN (\n"
                        + "        SELECT payment_id, user_id, price\n"
                        + "        FROM Payments\n"
                        + "        WHERE price > 100\n"
                        + "    ) AS p\n"
                        + "    ON o.user_id = p.user_id\n"
                        + ") AS op\n"
                        + "ON u.user_id = op.user_id");
    }

    @Test
    void testCTEWithMultiJoinV2() {
        util.tableEnv()
                .executeSql(
                        "CREATE TABLE Departments ("
                                + "  dept_id STRING PRIMARY KEY NOT ENFORCED,"
                                + "  dept_name STRING,"
                                + "  budget DOUBLE"
                                + ") WITH ('connector' = 'values', 'changelog-mode' = 'I,UA,D')");

        util.tableEnv()
                .executeSql(
                        "CREATE TABLE Projects ("
                                + "  project_id STRING PRIMARY KEY NOT ENFORCED,"
                                + "  project_name STRING,"
                                + "  dept_id STRING,"
                                + "  status STRING"
                                + ") WITH ('connector' = 'values', 'changelog-mode' = 'I,UA,D')");

        util.verifyRelPlan(
                "\nWITH high_budget_depts AS (\n"
                        + "    SELECT dept_id, dept_name, budget\n"
                        + "    FROM Departments\n"
                        + "    WHERE budget > 600000\n"
                        + "),\n"
                        + "active_projects AS (\n"
                        + "    SELECT project_id, project_name, dept_id\n"
                        + "    FROM Projects\n"
                        + "    WHERE status = 'ACTIVE'\n"
                        + ")\n"
                        + "SELECT\n"
                        + "    u.user_id,\n"
                        + "    u.name,\n"
                        + "    o.order_id,\n"
                        + "    hbd.dept_name,\n"
                        + "    ap.project_name,\n"
                        + "    hbd.budget\n"
                        + "FROM Users u\n"
                        + "LEFT JOIN Orders o\n"
                        + "    ON u.user_id = o.user_id\n"
                        + "LEFT JOIN high_budget_depts hbd\n"
                        + "    ON o.user_id = hbd.dept_id\n"
                        + "LEFT JOIN active_projects ap\n"
                        + "    ON hbd.dept_id = ap.dept_id");
    }

    @Test
    void testWithOrInJoinCondition() {
        util.verifyRelPlan(
                "\nSELECT\n"
                        + "    u.user_id,\n"
                        + "    u.name,\n"
                        + "    o.order_id,\n"
                        + "    p.payment_id,\n"
                        + "    s.location\n"
                        + "FROM Users u\n"
                        + "LEFT JOIN Orders o\n"
                        + "    ON o.user_id = u.user_id\n"
                        + "LEFT JOIN Payments p\n"
                        + "    ON u.user_id = p.user_id OR u.name = p.payment_id\n"
                        + "LEFT JOIN Shipments s\n"
                        + "    ON p.user_id = s.user_id");
    }

    @Test
    @Tag("multijoin-chain-expected")
    void testWithCastCommonJoinKeyToIntegerMultiJoinChainExpected() {
        util.verifyRelPlan(
                "\nSELECT\n"
                        + "    u.user_id,\n"
                        + "    u.name,\n"
                        + "    o.order_id,\n"
                        + "    p.payment_id,\n"
                        + "    s.location\n"
                        + "FROM Users u\n"
                        + "LEFT JOIN Orders o\n"
                        + "    ON o.user_id = u.user_id\n"
                        + "LEFT JOIN Payments p\n"
                        + "    ON CAST(u.user_id as INTEGER) = CAST(p.user_id as INTEGER)\n"
                        + "LEFT JOIN Shipments s\n"
                        + "    ON u.user_id = s.user_id");
    }

    @Test
    void testWithCastCommonJoinKeyToVarchar() {
        util.verifyRelPlan(
                "\nSELECT\n"
                        + "    u.user_id,\n"
                        + "    u.name,\n"
                        + "    o.order_id,\n"
                        + "    p.payment_id,\n"
                        + "    s.location\n"
                        + "FROM Users u\n"
                        + "LEFT JOIN Orders o\n"
                        + "    ON o.user_id = u.user_id\n"
                        + "LEFT JOIN Payments p\n"
                        + "    ON CAST(u.user_id as VARCHAR) = CAST(p.user_id as VARCHAR)\n"
                        + "LEFT JOIN Shipments s\n"
                        + "    ON u.user_id = s.user_id");
    }

    @Test
    void testAggregationAndGroupingWithMultiJoinV2() {
        util.tableEnv()
                .executeSql(
                        "CREATE TABLE Categories ("
                                + "  category_id STRING PRIMARY KEY NOT ENFORCED,"
                                + "  category_name STRING,"
                                + "  parent_category STRING,"
                                + "  user_id STRING"
                                + ") WITH ('connector' = 'values', 'changelog-mode' = 'I,UA,D')");

        util.tableEnv()
                .executeSql(
                        "CREATE TABLE Sales ("
                                + "  sale_id STRING PRIMARY KEY NOT ENFORCED,"
                                + "  user_id STRING,"
                                + "  product_id STRING,"
                                + "  amount DOUBLE,"
                                + "  sale_date DATE"
                                + ") WITH ('connector' = 'values', 'changelog-mode' = 'I,UA,D')");

        util.verifyRelPlan(
                "\nSELECT\n"
                        + "    c.category_name,\n"
                        + "    COUNT(DISTINCT u.user_id) AS unique_users,\n"
                        + "    COUNT(s.sale_id) AS total_sales,\n"
                        + "    SUM(s.amount) AS total_revenue,\n"
                        + "    AVG(s.amount) AS avg_sale_amount,\n"
                        + "    MAX(s.amount) AS max_sale_amount\n"
                        + "FROM Users u\n"
                        + "LEFT JOIN Orders o\n"
                        + "    ON u.user_id = o.user_id\n"
                        + "LEFT JOIN Categories c\n"
                        + "    ON u.user_id = c.user_id AND o.product = c.category_id\n"
                        + "LEFT JOIN Sales s\n"
                        + "    ON u.user_id = s.user_id\n"
                        + "GROUP BY c.category_name\n"
                        + "HAVING COUNT(s.sale_id) > 0");
    }

    @Test
    void testSameTableMultipleAliases() {
        util.verifyRelPlan(
                "\nSELECT *\n"
                        + "FROM Users u\n"
                        + "LEFT JOIN Users u1\n"
                        + "    ON u.user_id = u1.user_id\n"
                        + "LEFT JOIN Users u2\n"
                        + "    ON u1.user_id = u2.user_id\n"
                        + "LEFT JOIN Users u3\n"
                        + "    ON u2.user_id = u3.user_id");
    }

    @Test
    @Tag("multijoin-chain-expected")
    void testWithExpressionInJoinConditionMultiJoinChainExpected() {
        util.tableEnv()
                .executeSql(
                        "CREATE TABLE Products ("
                                + "  product_id STRING PRIMARY KEY NOT ENFORCED,"
                                + "  price DOUBLE,"
                                + "  discount DOUBLE"
                                + ") WITH ('connector' = 'values', 'changelog-mode' = 'I,UA,D')");

        util.tableEnv()
                .executeSql(
                        "CREATE TABLE Sales ("
                                + "  sale_id STRING PRIMARY KEY NOT ENFORCED,"
                                + "  product_key DOUBLE,"
                                + "  quantity INT"
                                + ") WITH ('connector' = 'values', 'changelog-mode' = 'I,UA,D')");

        util.tableEnv()
                .executeSql(
                        "CREATE TABLE Promotions ("
                                + "  promo_id STRING PRIMARY KEY NOT ENFORCED,"
                                + "  product_key DOUBLE,"
                                + "  promo_text STRING"
                                + ") WITH ('connector' = 'values', 'changelog-mode' = 'I,UA,D')");

        util.verifyRelPlan(
                "\nSELECT\n"
                        + "    p.product_id,\n"
                        + "    (p.price - p.discount) AS net_price,\n"
                        + "    s.quantity,\n"
                        + "    pr.promo_text\n"
                        + "FROM Products AS p\n"
                        + "LEFT JOIN Sales AS s\n"
                        + "    ON (p.price - p.discount) = s.product_key\n"
                        + "LEFT JOIN Promotions AS pr\n"
                        + "    ON (p.price - p.discount) = pr.product_key\n"
                        + "WHERE (p.price - p.discount) > 100");
    }

    @Test
    @Tag("no-common-join-key")
    void testFunctionAndExpressionWithMultiJoinNoCommonJoinKeyV2() {
        util.tableEnv()
                .executeSql(
                        "CREATE TABLE ProductDetails ("
                                + "  product_id STRING PRIMARY KEY NOT ENFORCED,"
                                + "  product_name STRING,"
                                + "  price DOUBLE,"
                                + "  weight DOUBLE,"
                                + "  created_date DATE"
                                + ") WITH ('connector' = 'values', 'changelog-mode' = 'I,UA,D')");

        util.tableEnv()
                .executeSql(
                        "CREATE TABLE Reviews ("
                                + "  review_id STRING PRIMARY KEY NOT ENFORCED,"
                                + "  product_id STRING,"
                                + "  rating INT,"
                                + "  review_text STRING,"
                                + "  review_date DATE"
                                + ") WITH ('connector' = 'values', 'changelog-mode' = 'I,UA,D')");

        util.verifyRelPlan(
                "\nSELECT\n"
                        + "    u.user_id,\n"
                        + "    u.name,\n"
                        + "    o.order_id,\n"
                        + "    pd.product_name,\n"
                        + "    pd.price,\n"
                        + "    ROUND(pd.price * 1.1, 2) AS price_with_tax,\n"
                        + "    CONCAT('Product: ', pd.product_name) AS product_description,\n"
                        + "    CHAR_LENGTH(r.review_text) AS review_length,\n"
                        + "    UPPER(SUBSTRING(r.review_text, 1, 10)) AS review_preview,\n"
                        + "    CASE\n"
                        + "        WHEN r.rating >= 4 THEN 'High Rating'\n"
                        + "        WHEN r.rating >= 3 THEN 'Medium Rating'\n"
                        + "        ELSE 'Low Rating'\n"
                        + "    END AS rating_category,\n"
                        + "    TIMESTAMPDIFF(DAY, pd.created_date, CURRENT_DATE) AS days_since_created\n"
                        + "FROM Users u\n"
                        + "LEFT JOIN Orders o\n"
                        + "    ON u.user_id = o.user_id\n"
                        + "LEFT JOIN ProductDetails pd\n"
                        + "    ON o.product = pd.product_id\n"
                        + "LEFT JOIN Reviews r\n"
                        + "    ON pd.product_id = r.product_id");
    }

    @Test
    void testCrossJoinUnnestWithMultiJoinInsert() {
        util.tableEnv()
                .executeSql(
                        "CREATE TABLE UnnestSink ("
                                + "  detail_id STRING,"
                                + "  element_data STRING,"
                                + "  data_value_id INT,"
                                + "  user_id STRING,"
                                + "  order_id STRING"
                                + ") WITH ('connector' = 'values', 'sink-insert-only' = 'false')");

        util.verifyRelPlanInsert(
                "\nINSERT INTO UnnestSink\n"
                        + "(\n"
                        + "    detail_id,\n"
                        + "    element_data,\n"
                        + "    data_value_id,\n"
                        + "    user_id,\n"
                        + "    order_id\n"
                        + ")\n"
                        + "SELECT\n"
                        + "    d.detail_id,\n"
                        + "    TRIM(REGEXP_REPLACE(edata, '[\\[\\]\\\"]', '')) AS element_data,\n"
                        + "    ARRAY_POSITION(split(REGEXP_REPLACE(d.data, '^\\[\"|\"\\]$', '') , '\", \"'), edata) as data_value_id,\n"
                        + "    d.user_id,\n"
                        + "    o.order_id\n"
                        + "FROM Detail d\n"
                        + "INNER JOIN Orders o\n"
                        + "    ON o.user_id = d.user_id\n"
                        + "INNER JOIN Payments p\n"
                        + "    ON p.user_id = d.user_id\n"
                        + "LEFT JOIN Shipments s\n"
                        + "    ON s.user_id = d.user_id\n"
                        + "CROSS JOIN UNNEST(split(REGEXP_REPLACE(d.data, '^\\[\"|\"\\]$', '') , '\", \"')) AS T(edata)\n"
                        + "WHERE NOT (s.location IS NOT NULL)");
    }
}
