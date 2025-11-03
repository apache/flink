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
import org.junit.jupiter.api.Disabled;
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
                "SELECT u.user_id, u.name, o.order_id, p.payment_id "
                        + "FROM Users u "
                        + "INNER JOIN Orders o ON u.user_id = o.user_id "
                        + "INNER JOIN Payments p ON u.user_id = p.user_id");
    }

    @Test
    void testThreeWayInnerJoinNoCommonJoinKeyRelPlan() {
        util.verifyRelPlan(
                "SELECT u.user_id, u.name, o.order_id, p.payment_id "
                        + "FROM Users u "
                        + "INNER JOIN Orders o ON u.user_id = o.user_id "
                        + "INNER JOIN Payments p ON u.cash = p.price");
    }

    @Test
    void testThreeWayInnerJoinExecPlan() {
        util.verifyExecPlan(
                "SELECT u.user_id, u.name, o.order_id, p.payment_id "
                        + "FROM Users u "
                        + "INNER JOIN Orders o ON u.user_id = o.user_id "
                        + "INNER JOIN Payments p ON u.user_id = p.user_id");
    }

    @Test
    void testThreeWayLeftOuterJoinRelPlan() {
        util.verifyRelPlan(
                "SELECT u.user_id, u.name, o.order_id, p.payment_id "
                        + "FROM Users u "
                        + "LEFT JOIN Orders o ON u.user_id = o.user_id "
                        + "LEFT JOIN Payments p ON u.user_id = p.user_id");
    }

    @Test
    void testThreeWayInnerJoinWithTttlHints() {
        util.verifyRelPlan(
                "SELECT /*+ STATE_TTL(u='1d', o='2d', p='1h') */u.user_id, u.name, o.order_id, p.payment_id "
                        + "FROM Users u "
                        + "INNER JOIN Orders o ON u.user_id = o.user_id "
                        + "INNER JOIN Payments p ON u.user_id = p.user_id");
    }

    @Test
    void testThreeWayInnerJoinWithSingleTttlHint() {
        util.verifyRelPlan(
                "SELECT /*+ STaTE_tTL(o='2d') */u.user_id, u.name, o.order_id, p.payment_id "
                        + "FROM Users u "
                        + "INNER JOIN Orders o ON u.user_id = o.user_id "
                        + "INNER JOIN Payments p ON u.user_id = p.user_id");
    }

    @Test
    void testThreeWayLeftOuterJoinExecPlan() {
        util.verifyExecPlan(
                "SELECT u.user_id, u.name, o.order_id, p.payment_id "
                        + "FROM Users u "
                        + "LEFT JOIN Orders o ON u.user_id = o.user_id "
                        + "LEFT JOIN Payments p ON u.user_id = p.user_id");
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
                "WITH OrdersUnion as ("
                        + "SELECT * FROM Orders "
                        + "UNION ALL "
                        + "SELECT * FROM Orders2"
                        + ") "
                        + "SELECT * FROM OrdersUnion o "
                        + "LEFT JOIN Users u "
                        + "ON o.user_id = u.user_id");
    }

    @Test
    void testTwoWayJoinWithRank() {
        util.verifyRelPlan(
                "WITH JoinedEvents as ("
                        + "SELECT e1.id as id, e1.val, e1.`$rowtime` as `$rowtime`, e2.price "
                        + "FROM EventTable1 e1 "
                        + "JOIN EventTable2 e2 ON e1.id = e2.id) "
                        + "SELECT id, val, `$rowtime` FROM ("
                        + "SELECT *, "
                        + "ROW_NUMBER() OVER (PARTITION BY id ORDER BY `$rowtime` DESC) as ts "
                        + "FROM JoinedEvents) "
                        + "WHERE ts = 1");
    }

    @Test
    void testFourWayComplexJoinRelPlan() {
        util.verifyRelPlan(
                "SELECT u.user_id, u.name, o.order_id, p.payment_id, s.location "
                        + "FROM Users u "
                        + "LEFT JOIN Orders o ON u.user_id = o.user_id "
                        + "INNER JOIN Payments p ON u.user_id = p.user_id AND (u.cash >= p.price OR p.price < 0) "
                        + "LEFT JOIN Shipments s ON p.user_id = s.user_id");
    }

    @Test
    void testThreeWayJoinNoJoinKeyExecPlan() {
        util.verifyExecPlan(
                "SELECT u.user_id, u.name, o.order_id, p.payment_id "
                        + "FROM Users u "
                        + "LEFT JOIN Orders o ON TRUE "
                        + "INNER JOIN Payments p ON TRUE ");
    }

    @Test
    void testFourWayJoinNoCommonJoinKeyRelPlan() {
        util.verifyRelPlan(
                "SELECT u.user_id, u.name, o.order_id, p.payment_id, s.location "
                        + "FROM Users u "
                        + "LEFT JOIN Orders o ON u.user_id = o.user_id "
                        + "LEFT JOIN LookupTable ON u.name = LookupTable.name "
                        + "LEFT JOIN Payments p ON u.user_id = p.user_id "
                        + "LEFT JOIN Shipments s ON o.user_id = s.user_id");
    }

    @Test
    void testFourWayComplexJoinExecPlan() {
        util.verifyExecPlan(
                "SELECT u.user_id, u.name, o.order_id, p.payment_id, s.location "
                        + "FROM Users u "
                        + "LEFT JOIN Orders o ON u.user_id = o.user_id "
                        + "INNER JOIN Payments p ON u.user_id = p.user_id AND (u.cash >= p.price OR p.price < 0) "
                        + "LEFT JOIN Shipments s ON p.user_id = s.user_id");
    }

    @Test
    void testThreeWayInnerJoinExplain() {
        util.verifyExplain(
                "SELECT u.user_id, u.name, o.order_id, p.payment_id "
                        + "FROM Users u "
                        + "INNER JOIN Orders o ON u.user_id = o.user_id "
                        + "INNER JOIN Payments p ON u.user_id = p.user_id");
    }

    @Test
    void testThreeWayLeftOuterJoinExplain() {
        util.verifyExplain(
                "SELECT u.user_id, u.name, o.order_id, p.payment_id "
                        + "FROM Users u "
                        + "LEFT JOIN Orders o ON u.user_id = o.user_id "
                        + "LEFT JOIN Payments p ON u.user_id = p.user_id");
    }

    @Test
    void testFourWayComplexJoinExplain() {
        util.verifyExplain(
                "SELECT u.user_id, u.name, o.order_id, p.payment_id, s.location "
                        + "FROM Users u "
                        + "LEFT JOIN Orders o ON u.user_id = o.user_id "
                        + "INNER JOIN Payments p ON u.user_id = p.user_id AND (u.cash >= p.price OR p.price < 0) "
                        + "LEFT JOIN Shipments s ON p.user_id = s.user_id");
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
                        + "AND e1.`$rowtime` BETWEEN e2.`$rowtime` - INTERVAL '1' MINUTE "
                        + "AND e2.`$rowtime` + INTERVAL '1' MINUTE");
    }

    @Test
    void testThreeWayLeftOuterJoinWithWhereClauseRelPlan() {
        util.verifyRelPlan(
                "SELECT u.user_id, u.name, o.order_id, p.payment_id "
                        + "FROM Users u "
                        + "LEFT JOIN Orders o ON u.user_id = o.user_id "
                        + "LEFT JOIN Payments p ON u.user_id = p.user_id "
                        + "WHERE u.name = 'Gus' AND p.price > 10");
    }

    @Test
    void testThreeWayLeftOuterJoinWithWhereClauseExecPlan() {
        util.verifyExecPlan(
                "SELECT u.user_id, u.name, o.order_id, p.payment_id "
                        + "FROM Users u "
                        + "LEFT JOIN Orders o ON u.user_id = o.user_id "
                        + "LEFT JOIN Payments p ON u.user_id = p.user_id "
                        + "WHERE u.name = 'Gus' AND p.price > 10");
    }

    @Test
    void testThreeWayLeftOuterJoinWithWhereClauseExplain() {
        util.verifyExplain(
                "SELECT u.user_id, u.name, o.order_id, p.payment_id "
                        + "FROM Users u "
                        + "LEFT JOIN Orders o ON u.user_id = o.user_id "
                        + "LEFT JOIN Payments p ON u.user_id = p.user_id "
                        + "WHERE u.name = 'Gus' AND p.price > 10");
    }

    @Test
    void testRegularJoinsAreMergedApartFromTemporalJoin() {
        // Regular joins should still be eligible for MultiJoin but not mixed with temporal joins
        util.verifyRelPlan(
                "SELECT u.user_id, u.name, o.order_id, temporal.age "
                        + "FROM Users u "
                        + "INNER JOIN Orders o ON u.user_id = o.user_id "
                        + "INNER JOIN ("
                        + "  SELECT s.user_id, l.age "
                        + "  FROM StreamTable s "
                        + "  JOIN LookupTable FOR SYSTEM_TIME AS OF s.proctime AS l "
                        + "  ON s.user_id = l.id"
                        + ") temporal ON u.user_id = temporal.user_id");
    }

    @Test
    void testFourWayJoinTransitiveCommonJoinKeyRelPlan() {
        util.verifyRelPlan(
                "SELECT u.user_id, u.name, o.order_id, p.payment_id, s.location "
                        + "FROM Users u "
                        + "LEFT JOIN Orders o ON u.user_id = o.user_id "
                        + "LEFT JOIN Payments p ON o.user_id = p.user_id "
                        + "LEFT JOIN Shipments s ON p.user_id = s.user_id");
    }

    /* Update this to supported with FLINK-37973 https://issues.apache.org/jira/browse/FLINK-37973 */
    @Test
    void testRightJoinNotSupported() {
        util.verifyRelPlan(
                "SELECT u.user_id, u.name, o.order_id, p.payment_id "
                        + "FROM Users u "
                        + "RIGHT JOIN Orders o ON u.user_id = o.user_id "
                        + "RIGHT JOIN Payments p ON o.user_id = p.user_id");
    }

    @Test
    void testFullOuterNotSupported() {
        util.verifyRelPlan(
                "SELECT u.user_id, u.name, o.order_id, p.payment_id "
                        + "FROM Users u "
                        + "FULL OUTER JOIN Orders o ON u.user_id = o.user_id "
                        + "FULL OUTER JOIN Payments p ON o.user_id = p.user_id");
    }

    @Test
    void testThreeWayJoinWithTimeAttributesMaterialization() {
        util.verifyRelPlan(
                "SELECT u.name, u.proctime, o.`$rowtime`, p.price "
                        + "FROM UsersWithProctime u "
                        + "JOIN OrdersWithRowtime o ON u.user_id = o.user_id "
                        + "JOIN Payments p ON u.user_id = p.user_id");
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
                "INSERT INTO sink_two_way "
                        + "SELECT"
                        + "    o.user_id,"
                        + "    o.order_id,"
                        + "    o.product,"
                        + "    u.region_id "
                        + "FROM OrdersPK o "
                        + "LEFT JOIN UsersPK u"
                        + "  ON  u.user_id = o.user_id");
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
                "INSERT INTO sink_two_way "
                        + "SELECT"
                        + "    o.user_id,"
                        + "    o.order_id,"
                        + "    o.product,"
                        + "    u.region_id "
                        + "FROM UsersPK u "
                        + "INNER JOIN OrdersPK o "
                        + "  ON  u.user_id = o.user_id");
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
                "INSERT INTO sink_two_way "
                        + "SELECT"
                        + "    o.user_id,"
                        + "    o.order_id,"
                        + "    o.product,"
                        + "    u.region_id "
                        + "FROM UsersPK u "
                        + "INNER JOIN OrdersSimplePK o "
                        + "  ON  u.user_id = o.user_id");
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
                "INSERT INTO sink_three_way "
                        + "SELECT"
                        + "    o.user_id,"
                        + "    o.order_id,"
                        + "    p.user_id,"
                        + "    p.payment_id,"
                        + "    u.user_id,"
                        + "    u.description "
                        + "FROM UsersPK u "
                        + "JOIN OrdersPK o"
                        + "  ON  o.user_id = u.user_id "
                        + "JOIN PaymentsPK p"
                        + "  ON  o.user_id = p.user_id");
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
                "INSERT INTO sink_four_way "
                        + "SELECT"
                        + "    u.user_id,"
                        + "    o.order_id,"
                        + "    o.user_id,"
                        + "    p.payment_id,"
                        + "    p.user_id,"
                        + "    u.name,"
                        + "    a.location "
                        + "FROM UsersPK u "
                        + "JOIN OrdersPK o"
                        + "  ON  u.user_id = o.user_id AND o.product IS NOT NULL "
                        + "JOIN PaymentsPK p"
                        + "  ON  u.user_id = p.user_id AND p.price >= 0 "
                        + "JOIN AddressPK a"
                        + "  ON  u.user_id = a.user_id AND a.location IS NOT NULL");
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

    /* The following tests contain mapped bugs while adding validation and are disabled for now
     * They will be addressed in FLINK-38576 */
    @Disabled
    @Test
    void testFourWayJoinNoCommonJoinKeyWithFunctionInCondition() {
        util.verifyRelPlan(
                "SELECT u.user_id, u.name, o.order_id, p.payment_id, s.location "
                        + "FROM Users u "
                        + "LEFT JOIN Orders o ON o.user_id = u.user_id "
                        + "LEFT JOIN Payments p ON u.user_id = p.user_id "
                        + "     AND UPPER(u.name) = UPPER(p.payment_id) "
                        + "     AND (FLOOR(u.cash) >= FLOOR(p.price) OR p.price < 0) "
                        + "LEFT JOIN Shipments s ON p.payment_id = s.location ");
    }

    @Disabled
    @Test
    void testComplexCommonJoinKeyMissingProjection() {
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
                "SELECT *\n"
                        + "    FROM Assignments assignments\n"
                        + "    LEFT JOIN Documents AS documents\n"
                        + "        ON assignments.detail_id = documents.detail_id\n"
                        + "        AND assignments.common_id = documents.common_id\n"
                        + "    LEFT JOIN Documents AS other_documents\n"
                        + "        ON assignments.user_id = documents.common_id\n");
    }

    /*
     * In this case we have common_id across conditions but in a more complex way.
     * but ultimately every combination should be possible. Any of the keys should be able to be
     * used in a following join condition:
     * - assignments.common_id
     * - customer.common_id
     * - organizations.common_id
     * - documents.common_id
     * - creators.common_id
     * */
    @Disabled
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
                "SELECT *\n"
                        + "    FROM Assignments assignments\n"
                        + "    LEFT JOIN Customers AS customer\n"
                        + "        ON assignments.user_id = customer.user_id\n"
                        + "        AND assignments.common_id = customer.common_id\n"
                        + "    LEFT JOIN Documents AS documents\n"
                        + "        ON assignments.detail_id = documents.detail_id\n"
                        + "        AND assignments.common_id = documents.common_id\n"
                        + "    LEFT JOIN PhaseDetails AS phase_details\n"
                        + "        ON documents.common_id = phase_details.common_id\n"
                        + "    LEFT JOIN Organizations AS organizations\n"
                        + "        ON customer.depart_num = organizations.org_id\n"
                        + "        AND customer.common_id = organizations.common_id\n"
                        + "    LEFT JOIN Customers AS creators\n"
                        + "        ON documents.creator_nm = creators.depart_num\n"
                        + "        AND documents.common_id = creators.common_id");
    }

    @Test
    void testComplexConditionalLogicWithMultiJoin() {
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
                "SELECT "
                        + "u.user_id, "
                        + "o.order_id, "
                        + "p.payment_id, "
                        + "pc.category_name, "
                        + "CASE "
                        + "  WHEN pc.is_premium = true AND p.price > 1000 THEN 'High-Value Premium' "
                        + "  WHEN pc.is_premium = true THEN 'Premium' "
                        + "  WHEN p.price > 500 THEN 'Standard High-Value' "
                        + "  ELSE 'Standard' "
                        + "END AS product_tier, "
                        + "CASE "
                        + "  WHEN pr.rating >= 4 AND pr.is_verified = true THEN 'Highly Recommended' "
                        + "  WHEN pr.rating >= 3 THEN 'Recommended' "
                        + "  WHEN pr.rating >= 2 THEN 'Average' "
                        + "  ELSE 'Not Recommended' "
                        + "END AS recommendation_status, "
                        + "CASE "
                        + "  WHEN pc.discount_rate > 0.2 THEN p.price * (1 - pc.discount_rate) "
                        + "  ELSE p.price "
                        + "END AS final_price "
                        + "FROM Users u "
                        + "LEFT JOIN Orders o ON u.user_id = o.user_id "
                        + "LEFT JOIN Payments p ON u.user_id = p.user_id "
                        + "LEFT JOIN ProductCategories pc ON o.product = pc.category_id "
                        + "LEFT JOIN ProductReviews pr ON o.product = pr.product_id");
    }

    @Test
    void testComplexCTEWithMultiJoin() {
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
                "WITH user_orders AS ("
                        + "  SELECT u.user_id, u.name, o.order_id, o.product, p.payment_id, p.price "
                        + "  FROM Users u "
                        + "  LEFT JOIN Orders o ON u.user_id = o.user_id "
                        + "  LEFT JOIN Payments p ON u.user_id = p.user_id"
                        + "), "
                        + "order_details AS ("
                        + "  SELECT uo.*, os.status_name, os.is_final, pm.method_name, pm.processing_fee "
                        + "  FROM user_orders uo "
                        + "  LEFT JOIN OrderStatus os ON uo.order_id = os.status_id "
                        + "  LEFT JOIN PaymentMethods pm ON uo.payment_id = pm.method_id"
                        + "), "
                        + "final_summary AS ("
                        + "  SELECT "
                        + "    user_id, "
                        + "    name, "
                        + "    COUNT(order_id) as total_orders, "
                        + "    SUM(price) as total_spent, "
                        + "    AVG(price) as avg_order_value, "
                        + "    COUNT(CASE WHEN is_final = true THEN 1 END) as completed_orders "
                        + "  FROM order_details "
                        + "  GROUP BY user_id, name"
                        + ") "
                        + "SELECT * FROM final_summary");
    }

    @Test
    void testAggregationAndGroupingWithMultiJoin() {
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
                "SELECT "
                        + "u.user_id, "
                        + "u.name, "
                        + "pc.category_name, "
                        + "COUNT(DISTINCT o.order_id) as order_count, "
                        + "SUM(oi.quantity) as total_items, "
                        + "SUM(oi.quantity * oi.unit_price) as total_value, "
                        + "AVG(oi.unit_price) as avg_item_price, "
                        + "MAX(p.price) as max_payment, "
                        + "MIN(p.price) as min_payment, "
                        + "COUNT(CASE WHEN oi.quantity > 5 THEN 1 END) as bulk_orders "
                        + "FROM Users u "
                        + "LEFT JOIN Orders o ON u.user_id = o.user_id "
                        + "LEFT JOIN OrderItems oi ON o.order_id = oi.order_id "
                        + "LEFT JOIN ProductCategories pc ON oi.product_name = pc.category_id "
                        + "LEFT JOIN Payments p ON u.user_id = p.user_id "
                        + "GROUP BY u.user_id, u.name, pc.category_name "
                        + "HAVING COUNT(DISTINCT o.order_id) > 0");
    }

    @Test
    void testFunctionAndExpressionWithMultiJoin() {
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
                "SELECT "
                        + "u.user_id, "
                        + "UPPER(u.name) as user_name_upper, "
                        + "LOWER(o.product) as product_lower, "
                        + "CONCAT(u.name, ' - ', o.product) as user_product, "
                        + "SUBSTRING(pd.description, 1, 50) as description_preview, "
                        + "CHAR_LENGTH(pd.description) as description_length, "
                        + "FLOOR(p.price / 100.0) * 100 as price_rounded, "
                        + "CASE "
                        + "  WHEN p.price > 1000 THEN 'High' "
                        + "  WHEN p.price > 500 THEN 'Medium' "
                        + "  ELSE 'Low' "
                        + "END as price_tier, "
                        + "REGEXP_REPLACE(pd.tags, ',', ' | ') as formatted_tags, "
                        + "TO_TIMESTAMP_LTZ(pd.created_date, 3) as product_created, "
                        + "COALESCE(up.preferred_category, 'None') as user_preference, "
                        + "CASE "
                        + "  WHEN up.notification_level = 'HIGH' THEN 'Frequent Updates' "
                        + "  WHEN up.notification_level = 'MEDIUM' THEN 'Daily Updates' "
                        + "  ELSE 'Weekly Updates' "
                        + "END as notification_frequency "
                        + "FROM Users u "
                        + "LEFT JOIN Orders o ON u.user_id = o.user_id "
                        + "LEFT JOIN Payments p ON u.user_id = p.user_id "
                        + "LEFT JOIN ProductDetails pd ON o.product = pd.product_id "
                        + "LEFT JOIN UserPreferences up ON u.user_id = up.user_id");
    }

    @Test
    void testComplexNestedCTEWithAggregationAndFunctions() {
        util.tableEnv()
                .executeSql(
                        "CREATE TABLE OrderMetrics ("
                                + "  metric_id STRING PRIMARY KEY NOT ENFORCED,"
                                + "  order_id STRING,"
                                + "  metric_type STRING,"
                                + "  metric_value DOUBLE"
                                + ") WITH ('connector' = 'values', 'changelog-mode' = 'I,UA,D')");

        util.verifyRelPlan(
                "WITH base_orders AS ("
                        + "  SELECT u.user_id, u.name, o.order_id, p.payment_id, p.price "
                        + "  FROM Users u "
                        + "  INNER JOIN Orders o ON u.user_id = o.user_id "
                        + "  INNER JOIN Payments p ON u.user_id = p.user_id"
                        + "), "
                        + "enriched_orders AS ("
                        + "  SELECT "
                        + "    bo.*, "
                        + "    om.metric_type, "
                        + "    om.metric_value, "
                        + "    CASE "
                        + "      WHEN bo.price > 1000 THEN 'Premium' "
                        + "      WHEN bo.price > 500 THEN 'Standard' "
                        + "      ELSE 'Basic' "
                        + "    END as order_tier "
                        + "  FROM base_orders bo "
                        + "  LEFT JOIN OrderMetrics om ON bo.order_id = om.order_id"
                        + "), "
                        + "aggregated_metrics AS ("
                        + "  SELECT "
                        + "    user_id, "
                        + "    name, "
                        + "    COUNT(DISTINCT order_id) as total_orders, "
                        + "    SUM(price) as total_spent, "
                        + "    AVG(price) as avg_order_value, "
                        + "    MAX(metric_value) as max_metric, "
                        + "    MIN(metric_value) as min_metric, "
                        + "    COUNT(CASE WHEN order_tier = 'Premium' THEN 1 END) as premium_orders "
                        + "  FROM enriched_orders "
                        + "  GROUP BY user_id, name"
                        + ") "
                        + "SELECT "
                        + "  user_id, "
                        + "  UPPER(name) as user_name, "
                        + "  total_orders, "
                        + "  ROUND(total_spent, 2) as total_spent_rounded, "
                        + "  ROUND(avg_order_value, 2) as avg_order_value_rounded, "
                        + "  CONCAT('User: ', name, ' has ', CAST(total_orders AS STRING), ' orders') as summary, "
                        + "  CASE "
                        + "    WHEN total_orders > 10 THEN 'Frequent Customer' "
                        + "    WHEN total_orders > 5 THEN 'Regular Customer' "
                        + "    ELSE 'Occasional Customer' "
                        + "  END as customer_type "
                        + "FROM aggregated_metrics "
                        + "WHERE total_spent > 0");
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
                "WITH high_budget_depts AS ("
                        + "  SELECT dept_id, dept_name, budget "
                        + "  FROM Departments "
                        + "  WHERE budget > 600000"
                        + "), "
                        + "active_projects AS ("
                        + "  SELECT project_id, project_name, dept_id "
                        + "  FROM Projects "
                        + "  WHERE status = 'ACTIVE'"
                        + ") "
                        + "SELECT "
                        + "  u.user_id, "
                        + "  u.name, "
                        + "  o.order_id, "
                        + "  hbd.dept_name, "
                        + "  ap.project_name, "
                        + "  hbd.budget "
                        + "FROM Users u "
                        + "LEFT JOIN Orders o ON u.user_id = o.user_id "
                        + "LEFT JOIN high_budget_depts hbd ON o.product = hbd.dept_id "
                        + "LEFT JOIN active_projects ap ON hbd.dept_id = ap.dept_id");
    }

    @Test
    void testAggregationAndGroupingWithMultiJoinV2() {
        util.tableEnv()
                .executeSql(
                        "CREATE TABLE Categories ("
                                + "  category_id STRING PRIMARY KEY NOT ENFORCED,"
                                + "  category_name STRING,"
                                + "  parent_category STRING"
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
                "SELECT "
                        + "  c.category_name, "
                        + "  COUNT(DISTINCT u.user_id) AS unique_users, "
                        + "  COUNT(s.sale_id) AS total_sales, "
                        + "  SUM(s.amount) AS total_revenue, "
                        + "  AVG(s.amount) AS avg_sale_amount, "
                        + "  MAX(s.amount) AS max_sale_amount "
                        + "FROM Users u "
                        + "LEFT JOIN Orders o ON u.user_id = o.user_id "
                        + "LEFT JOIN Categories c ON o.product = c.category_id "
                        + "LEFT JOIN Sales s ON u.user_id = s.user_id "
                        + "GROUP BY c.category_name "
                        + "HAVING COUNT(s.sale_id) > 0");
    }

    @Test
    void testFunctionAndExpressionWithMultiJoinV2() {
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
                "SELECT "
                        + "  u.user_id, "
                        + "  u.name, "
                        + "  o.order_id, "
                        + "  pd.product_name, "
                        + "  pd.price, "
                        + "  ROUND(pd.price * 1.1, 2) AS price_with_tax, "
                        + "  CONCAT('Product: ', pd.product_name) AS product_description, "
                        + "  CHAR_LENGTH(r.review_text) AS review_length, "
                        + "  UPPER(SUBSTRING(r.review_text, 1, 10)) AS review_preview, "
                        + "  CASE "
                        + "    WHEN r.rating >= 4 THEN 'High Rating' "
                        + "    WHEN r.rating >= 3 THEN 'Medium Rating' "
                        + "    ELSE 'Low Rating' "
                        + "  END AS rating_category, "
                        + "  TIMESTAMPDIFF(DAY, pd.created_date, CURRENT_DATE) AS days_since_created "
                        + "FROM Users u "
                        + "LEFT JOIN Orders o ON u.user_id = o.user_id "
                        + "LEFT JOIN ProductDetails pd ON o.product = pd.product_id "
                        + "LEFT JOIN Reviews r ON pd.product_id = r.product_id");
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
                "INSERT INTO UnnestSink\n"
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
