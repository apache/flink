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
                                + "  user_id_1 STRING,"
                                + "  order_id STRING PRIMARY KEY NOT ENFORCED,"
                                + "  product STRING"
                                + ") WITH ('connector' = 'values', 'changelog-mode' = 'I,D')");

        util.tableEnv()
                .executeSql(
                        "CREATE TABLE Payments ("
                                + "  user_id_2 STRING,"
                                + "  payment_id STRING PRIMARY KEY NOT ENFORCED,"
                                + "  price INT"
                                + ") WITH ('connector' = 'values', 'changelog-mode' = 'I')");

        util.tableEnv()
                .executeSql(
                        "CREATE TABLE Shipments ("
                                + "  user_id_3 STRING,"
                                + "  location STRING"
                                + ") WITH ('connector' = 'values', 'changelog-mode' = 'I,UA,UB,D')");
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
}
