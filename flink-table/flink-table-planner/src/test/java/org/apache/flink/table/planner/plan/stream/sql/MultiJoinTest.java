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

import org.apache.flink.table.api.TableConfig;
import org.apache.flink.table.api.config.OptimizerConfigOptions;
import org.apache.flink.table.planner.utils.TableTestBase;
import org.apache.flink.table.planner.utils.TableTestUtil;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

/** Tests for multi-join plans. */
public class MultiJoinTest extends TableTestBase {

    private TableTestUtil util;

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
                                + "  user_id_0 STRING PRIMARY KEY NOT ENFORCED,"
                                + "  name STRING,"
                                + "  cash INT"
                                + ") WITH ('connector' = 'values', 'changelog-mode' = 'I,UA,D')");

        util.tableEnv()
                .executeSql(
                        "CREATE TABLE Orders ("
                                + "  order_id STRING PRIMARY KEY NOT ENFORCED,"
                                + "  user_id_1 STRING,"
                                + "  product STRING"
                                + ") WITH ('connector' = 'values', 'changelog-mode' = 'I,D')");

        util.tableEnv()
                .executeSql(
                        "CREATE TABLE Payments ("
                                + "  payment_id STRING PRIMARY KEY NOT ENFORCED,"
                                + "  price INT,"
                                + "  user_id_2 STRING"
                                + ") WITH ('connector' = 'values', 'changelog-mode' = 'I')");

        util.tableEnv()
                .executeSql(
                        "CREATE TABLE Shipments ("
                                + "  location STRING,"
                                + "  user_id_3 STRING"
                                + ") WITH ('connector' = 'values', 'changelog-mode' = 'I,UA,UB,D')");

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
                                + "  rowtime TIMESTAMP(3),"
                                + "  WATERMARK FOR rowtime AS rowtime - INTERVAL '5' SECOND"
                                + ") WITH ('connector' = 'values', 'changelog-mode' = 'I')");

        util.tableEnv()
                .executeSql(
                        "CREATE TABLE EventTable2 ("
                                + "  id STRING,"
                                + "  price DOUBLE,"
                                + "  rowtime TIMESTAMP(3),"
                                + "  WATERMARK FOR rowtime AS rowtime - INTERVAL '5' SECOND"
                                + ") WITH ('connector' = 'values', 'changelog-mode' = 'I')");

        // Tables for testing time attribute materialization in multi-join
        util.tableEnv()
                .executeSql(
                        "CREATE TABLE UsersWithProctime ("
                                + "  user_id_0 STRING PRIMARY KEY NOT ENFORCED,"
                                + "  name STRING,"
                                + "  proctime AS PROCTIME()"
                                + ") WITH ('connector' = 'values', 'changelog-mode' = 'I')");

        util.tableEnv()
                .executeSql(
                        "CREATE TABLE OrdersWithRowtime ("
                                + "  order_id STRING PRIMARY KEY NOT ENFORCED,"
                                + "  user_id_1 STRING,"
                                + "  rowtime TIMESTAMP(3),"
                                + "  WATERMARK FOR rowtime AS rowtime"
                                + ") WITH ('connector' = 'values', 'changelog-mode' = 'I')");
    }

    @Test
    void testThreeWayInnerJoinRelPlan() {
        util.verifyRelPlan(
                "SELECT u.user_id_0, u.name, o.order_id, p.payment_id "
                        + "FROM Users u "
                        + "INNER JOIN Orders o ON u.user_id_0 = o.user_id_1 "
                        + "INNER JOIN Payments p ON u.user_id_0 = p.user_id_2");
    }

    @Test
    void testThreeWayInnerJoinNoCommonJoinKeyRelPlan() {
        util.verifyRelPlan(
                "SELECT u.user_id_0, u.name, o.order_id, p.payment_id "
                        + "FROM Users u "
                        + "INNER JOIN Orders o ON u.user_id_0 = o.user_id_1 "
                        + "INNER JOIN Payments p ON u.cash = p.price");
    }

    @Test
    void testThreeWayInnerJoinExecPlan() {
        util.verifyExecPlan(
                "SELECT u.user_id_0, u.name, o.order_id, p.payment_id "
                        + "FROM Users u "
                        + "INNER JOIN Orders o ON u.user_id_0 = o.user_id_1 "
                        + "INNER JOIN Payments p ON u.user_id_0 = p.user_id_2");
    }

    @Test
    void testThreeWayLeftOuterJoinRelPlan() {
        util.verifyRelPlan(
                "SELECT u.user_id_0, u.name, o.order_id, p.payment_id "
                        + "FROM Users u "
                        + "LEFT JOIN Orders o ON u.user_id_0 = o.user_id_1 "
                        + "LEFT JOIN Payments p ON u.user_id_0 = p.user_id_2");
    }

    @Test
    void testThreeWayLeftOuterJoinExecPlan() {
        util.verifyExecPlan(
                "SELECT u.user_id_0, u.name, o.order_id, p.payment_id "
                        + "FROM Users u "
                        + "LEFT JOIN Orders o ON u.user_id_0 = o.user_id_1 "
                        + "LEFT JOIN Payments p ON u.user_id_0 = p.user_id_2");
    }

    @Test
    void testFourWayComplexJoinRelPlan() {
        util.verifyRelPlan(
                "SELECT u.user_id_0, u.name, o.order_id, p.payment_id, s.location "
                        + "FROM Users u "
                        + "LEFT JOIN Orders o ON u.user_id_0 = o.user_id_1 "
                        + "INNER JOIN Payments p ON u.user_id_0 = p.user_id_2 AND (u.cash >= p.price OR p.price < 0) "
                        + "LEFT JOIN Shipments s ON p.user_id_2 = s.user_id_3");
    }

    @Test
    void testThreeWayJoinNoJoinKeyExecPlan() {
        util.verifyExecPlan(
                "SELECT u.user_id_0, u.name, o.order_id, p.payment_id "
                        + "FROM Users u "
                        + "LEFT JOIN Orders o ON TRUE "
                        + "INNER JOIN Payments p ON TRUE ");
    }

    @Test
    void testFourWayJoinNoCommonJoinKeyRelPlan() {
        util.verifyRelPlan(
                "SELECT u.user_id_0, u.name, o.order_id, p.payment_id, s.location "
                        + "FROM Users u "
                        + "LEFT JOIN Orders o ON u.user_id_0 = o.user_id_1 "
                        + "INNER JOIN Payments p ON u.user_id_0 = p.user_id_2 "
                        + "LEFT JOIN Shipments s ON p.payment_id = s.user_id_3");
    }

    @Test
    void testFourWayComplexJoinExecPlan() {
        util.verifyExecPlan(
                "SELECT u.user_id_0, u.name, o.order_id, p.payment_id, s.location "
                        + "FROM Users u "
                        + "LEFT JOIN Orders o ON u.user_id_0 = o.user_id_1 "
                        + "INNER JOIN Payments p ON u.user_id_0 = p.user_id_2 AND (u.cash >= p.price OR p.price < 0) "
                        + "LEFT JOIN Shipments s ON p.user_id_2 = s.user_id_3");
    }

    @Test
    void testThreeWayInnerJoinExplain() {
        util.verifyExplain(
                "SELECT u.user_id_0, u.name, o.order_id, p.payment_id "
                        + "FROM Users u "
                        + "INNER JOIN Orders o ON u.user_id_0 = o.user_id_1 "
                        + "INNER JOIN Payments p ON u.user_id_0 = p.user_id_2");
    }

    @Test
    void testThreeWayLeftOuterJoinExplain() {
        util.verifyExplain(
                "SELECT u.user_id_0, u.name, o.order_id, p.payment_id "
                        + "FROM Users u "
                        + "LEFT JOIN Orders o ON u.user_id_0 = o.user_id_1 "
                        + "LEFT JOIN Payments p ON u.user_id_0 = p.user_id_2");
    }

    @Test
    void testFourWayComplexJoinExplain() {
        util.verifyExplain(
                "SELECT u.user_id_0, u.name, o.order_id, p.payment_id, s.location "
                        + "FROM Users u "
                        + "LEFT JOIN Orders o ON u.user_id_0 = o.user_id_1 "
                        + "INNER JOIN Payments p ON u.user_id_0 = p.user_id_2 AND (u.cash >= p.price OR p.price < 0) "
                        + "LEFT JOIN Shipments s ON p.user_id_2 = s.user_id_3");
    }

    @Test
    void testTemporalJoinExcludedFromMultiJoin() {
        // Temporal joins should remain as lookup joins, not be merged into MultiJoin
        util.verifyRelPlan(
                "SELECT s.user_id, s.amount, l.name, l.age "
                        + "FROM StreamTable s "
                        + "JOIN LookupTable FOR SYSTEM_TIME AS OF s.proctime AS l "
                        + "ON s.user_id = l.id");
    }

    @Test
    void testIntervalJoinExcludedFromMultiJoin() {
        // Interval joins (event-time and processing-time) should remain as interval joins
        util.verifyRelPlan(
                "SELECT e1.id, e1.val, e2.price "
                        + "FROM EventTable1 e1 "
                        + "JOIN EventTable2 e2 ON e1.id = e2.id "
                        + "AND e1.rowtime BETWEEN e2.rowtime - INTERVAL '1' MINUTE "
                        + "AND e2.rowtime + INTERVAL '1' MINUTE");
    }

    @Test
    void testThreeWayLeftOuterJoinWithWhereClauseRelPlan() {
        util.verifyRelPlan(
                "SELECT u.user_id_0, u.name, o.order_id, p.payment_id "
                        + "FROM Users u "
                        + "LEFT JOIN Orders o ON u.user_id_0 = o.user_id_1 "
                        + "LEFT JOIN Payments p ON u.user_id_0 = p.user_id_2 "
                        + "WHERE u.name = 'Gus' AND p.price > 10");
    }

    @Test
    void testThreeWayLeftOuterJoinWithWhereClauseExecPlan() {
        util.verifyExecPlan(
                "SELECT u.user_id_0, u.name, o.order_id, p.payment_id "
                        + "FROM Users u "
                        + "LEFT JOIN Orders o ON u.user_id_0 = o.user_id_1 "
                        + "LEFT JOIN Payments p ON u.user_id_0 = p.user_id_2 "
                        + "WHERE u.name = 'Gus' AND p.price > 10");
    }

    @Test
    void testThreeWayLeftOuterJoinWithWhereClauseExplain() {
        util.verifyExplain(
                "SELECT u.user_id_0, u.name, o.order_id, p.payment_id "
                        + "FROM Users u "
                        + "LEFT JOIN Orders o ON u.user_id_0 = o.user_id_1 "
                        + "LEFT JOIN Payments p ON u.user_id_0 = p.user_id_2 "
                        + "WHERE u.name = 'Gus' AND p.price > 10");
    }

    @Test
    void testRegularJoinsAreMergedApartFromTemporalJoin() {
        // Regular joins should still be eligible for MultiJoin but not mixed with temporal joins
        util.verifyRelPlan(
                "SELECT u.user_id_0, u.name, o.order_id, temporal.age "
                        + "FROM Users u "
                        + "INNER JOIN Orders o ON u.user_id_0 = o.user_id_1 "
                        + "INNER JOIN ("
                        + "  SELECT s.user_id, l.age "
                        + "  FROM StreamTable s "
                        + "  JOIN LookupTable FOR SYSTEM_TIME AS OF s.proctime AS l "
                        + "  ON s.user_id = l.id"
                        + ") temporal ON u.user_id_0 = temporal.user_id");
    }

    @Test
    void testFourWayJoinTransitiveCommonJoinKeyRelPlan() {
        util.verifyRelPlan(
                "SELECT u.user_id_0, u.name, o.order_id, p.payment_id, s.location "
                        + "FROM Users u "
                        + "LEFT JOIN Orders o ON u.user_id_0 = o.user_id_1 "
                        + "LEFT JOIN Payments p ON o.user_id_1 = p.user_id_2 "
                        + "LEFT JOIN Shipments s ON p.user_id_2 = s.user_id_3");
    }

    /* Update this to supported with FLINK-37973 https://issues.apache.org/jira/browse/FLINK-37973 */
    @Test
    void testRightJoinNotSupported() {
        util.verifyRelPlan(
                "SELECT u.user_id_0, u.name, o.order_id, p.payment_id "
                        + "FROM Users u "
                        + "RIGHT JOIN Orders o ON u.user_id_0 = o.user_id_1 "
                        + "RIGHT JOIN Payments p ON o.user_id_1 = p.user_id_2");
    }

    @Test
    void testFullOuterNotSupported() {
        util.verifyRelPlan(
                "SELECT u.user_id_0, u.name, o.order_id, p.payment_id "
                        + "FROM Users u "
                        + "FULL OUTER JOIN Orders o ON u.user_id_0 = o.user_id_1 "
                        + "FULL OUTER JOIN Payments p ON o.user_id_1 = p.user_id_2");
    }

    @Test
    void testThreeWayJoinWithTimeAttributesMaterialization() {
        util.verifyRelPlan(
                "SELECT u.name, u.proctime, o.rowtime, p.price "
                        + "FROM UsersWithProctime u "
                        + "JOIN OrdersWithRowtime o ON u.user_id_0 = o.user_id_1 "
                        + "JOIN Payments p ON u.user_id_0 = p.user_id_2");
    }
}
