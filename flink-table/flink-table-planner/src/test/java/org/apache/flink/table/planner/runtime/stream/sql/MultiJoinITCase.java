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

package org.apache.flink.table.planner.runtime.stream.sql;

import org.apache.flink.table.api.TableEnvironment;
import org.apache.flink.table.api.config.OptimizerConfigOptions;
import org.apache.flink.table.planner.factories.TestValuesTableFactory;
import org.apache.flink.table.planner.runtime.utils.StreamingWithMiniBatchTestBase;
import org.apache.flink.types.Row;

import org.apache.calcite.rel.rules.MultiJoin;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

import java.util.Arrays;
import java.util.Collection;
import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Integration tests for {@link MultiJoin} that verify consistency when executed with a parallelism
 * greater than 1.
 */
@RunWith(Parameterized.class)
public class MultiJoinITCase extends StreamingWithMiniBatchTestBase {

    private TableEnvironment tEnv;

    public MultiJoinITCase(MiniBatchMode miniBatch, StateBackendMode state) {
        super(miniBatch, state);
    }

    List<Row> usersData =
            Arrays.asList(
                    Row.of("u1", "Will", 100),
                    Row.of("u2", "Eleven", 50),
                    Row.of("u3", "Dustin", 70),
                    Row.of("u4", "Mike", 200),
                    Row.of("u5", "Lucas", 100));

    List<Row> ordersData =
            Arrays.asList(
                    Row.of("o1", "u1", "Map"),
                    Row.of("o2", "u1", "Flashlight"),
                    Row.of("o3", "u2", "Waffles"),
                    Row.of("o4", "u3", "Bike"),
                    Row.of("o5", "u4", "Comics"),
                    Row.of("o6", "u5", "Radio set"));

    List<Row> paymentsData =
            Arrays.asList(
                    Row.of("p1", 10, "u1"),
                    Row.of("p2", 20, "u1"),
                    Row.of("p3", 30, "u2"),
                    Row.of("p4", 100, "u1"),
                    Row.of("p5", 50, "u2"),
                    Row.of("p6", 100, "u5"),
                    Row.of("p7", 70, "u6"),
                    Row.of("p8", 200, "u7"),
                    Row.of("p9", 100, "u8"),
                    Row.of("p10", 100, "u9"),
                    Row.of("p11", 999, "u10"));

    @Before
    @Override
    public void before() {
        super.before();
        tEnv = tEnv();
        tEnv.getConfig().set(OptimizerConfigOptions.TABLE_OPTIMIZER_MULTI_JOIN_ENABLED, true);

        String usersDataId = TestValuesTableFactory.registerData(usersData);
        tEnv.executeSql(
                String.format(
                        "CREATE TABLE Users ("
                                + "  `user_id` STRING,"
                                + "  `name` STRING,"
                                + "  `cash` INT"
                                + ") WITH ("
                                + "  'connector' = 'values',"
                                + "  'data-id' = '%s',"
                                + "  'bounded' = 'true'"
                                + ")",
                        usersDataId));

        String ordersDataId = TestValuesTableFactory.registerData(ordersData);
        tEnv.executeSql(
                String.format(
                        "CREATE TABLE Orders ("
                                + "  `order_id` STRING,"
                                + "  `user_id` STRING,"
                                + "  `product` STRING"
                                + ") WITH ("
                                + "  'connector' = 'values',"
                                + "  'data-id' = '%s',"
                                + "  'bounded' = 'true'"
                                + ")",
                        ordersDataId));

        String paymentsDataId = TestValuesTableFactory.registerData(paymentsData);
        tEnv.executeSql(
                String.format(
                        "CREATE TABLE Payments ("
                                + "  `payment_id` STRING,"
                                + "  `price` INT,"
                                + "  `user_id` STRING"
                                + ") WITH ("
                                + "  'connector' = 'values',"
                                + "  'data-id' = '%s',"
                                + "  'bounded' = 'true'"
                                + ")",
                        paymentsDataId));

        tEnv.executeSql(
                "CREATE TABLE ResultSink ("
                        + "  user_id STRING,"
                        + "  name STRING,"
                        + "  order_id STRING,"
                        + "  payment_id STRING,"
                        + "  price INT"
                        + ") WITH ("
                        + "  'connector' = 'values',"
                        + "  'sink-insert-only' = 'false'"
                        + ")");
    }

    @After
    @Override
    public void after() {
        super.after();
        TestValuesTableFactory.clearAllData();
    }

    @Test
    public void testThreeWayInnerJoin() throws Exception {

        tEnv.executeSql(
                        "INSERT INTO ResultSink "
                                + "SELECT u.user_id, u.name, o.order_id, p.payment_id, p.price "
                                + "FROM Users u "
                                + "JOIN Orders o ON u.user_id = o.user_id "
                                + "JOIN Payments p ON o.user_id = p.user_id")
                .await();

        List<String> actual = TestValuesTableFactory.getResultsAsStrings("ResultSink");

        List<String> expected =
                Arrays.asList(
                        "+I[u1, Will, o1, p2, 20]",
                        "+I[u1, Will, o1, p1, 10]",
                        "+I[u1, Will, o1, p4, 100]",
                        "+I[u1, Will, o2, p2, 20]",
                        "+I[u1, Will, o2, p1, 10]",
                        "+I[u1, Will, o2, p4, 100]",
                        "+I[u2, Eleven, o3, p5, 50]",
                        "+I[u2, Eleven, o3, p3, 30]",
                        "+I[u5, Lucas, o6, p6, 100]");

        assertThat(actual).containsExactlyInAnyOrderElementsOf(expected);
    }

    @Test
    public void testThreeWayLeftInnerJoin() throws Exception {

        tEnv.executeSql(
                        "INSERT INTO ResultSink "
                                + "SELECT u.user_id, u.name, o.order_id, p.payment_id, p.price "
                                + "FROM Users u "
                                + "LEFT JOIN Orders o ON u.user_id = o.user_id "
                                + "JOIN Payments p ON o.user_id = p.user_id")
                .await();

        List<String> actual = TestValuesTableFactory.getResultsAsStrings("ResultSink");

        List<String> expected =
                Arrays.asList(
                        "+I[u1, Will, o1, p2, 20]",
                        "+I[u1, Will, o1, p1, 10]",
                        "+I[u1, Will, o1, p4, 100]",
                        "+I[u1, Will, o2, p2, 20]",
                        "+I[u1, Will, o2, p1, 10]",
                        "+I[u1, Will, o2, p4, 100]",
                        "+I[u2, Eleven, o3, p5, 50]",
                        "+I[u2, Eleven, o3, p3, 30]",
                        "+I[u5, Lucas, o6, p6, 100]");

        assertThat(actual).containsExactlyInAnyOrderElementsOf(expected);
    }

    @Test
    public void testThreeWayLeftLeftJoin() throws Exception {

        tEnv.executeSql(
                        "INSERT INTO ResultSink "
                                + "SELECT u.user_id, u.name, o.order_id, p.payment_id, p.price "
                                + "FROM Users u "
                                + "LEFT JOIN Orders o ON u.user_id = o.user_id "
                                + "LEFT JOIN Payments p ON o.user_id = p.user_id")
                .await();

        List<String> actual = TestValuesTableFactory.getResultsAsStrings("ResultSink");

        List<String> expected =
                Arrays.asList(
                        "+I[u3, Dustin, o4, null, null]",
                        "+I[u1, Will, o1, p2, 20]",
                        "+I[u1, Will, o1, p1, 10]",
                        "+I[u1, Will, o1, p4, 100]",
                        "+I[u1, Will, o2, p2, 20]",
                        "+I[u1, Will, o2, p1, 10]",
                        "+I[u1, Will, o2, p4, 100]",
                        "+I[u4, Mike, o5, null, null]",
                        "+I[u5, Lucas, o6, p6, 100]",
                        "+I[u2, Eleven, o3, p5, 50]",
                        "+I[u2, Eleven, o3, p3, 30]");

        assertThat(actual).containsExactlyInAnyOrderElementsOf(expected);
    }

    @Test
    public void testThreeWayInnerLeftJoin() throws Exception {

        tEnv.executeSql(
                        "INSERT INTO ResultSink "
                                + "SELECT u.user_id, u.name, o.order_id, p.payment_id, p.price "
                                + "FROM Users u "
                                + "JOIN Orders o ON u.user_id = o.user_id "
                                + "LEFT JOIN Payments p ON o.user_id = p.user_id")
                .await();

        List<String> actual = TestValuesTableFactory.getResultsAsStrings("ResultSink");

        List<String> expected =
                Arrays.asList(
                        "+I[u3, Dustin, o4, null, null]",
                        "+I[u1, Will, o1, p2, 20]",
                        "+I[u1, Will, o1, p1, 10]",
                        "+I[u1, Will, o1, p4, 100]",
                        "+I[u1, Will, o2, p2, 20]",
                        "+I[u1, Will, o2, p1, 10]",
                        "+I[u1, Will, o2, p4, 100]",
                        "+I[u4, Mike, o5, null, null]",
                        "+I[u5, Lucas, o6, p6, 100]",
                        "+I[u2, Eleven, o3, p5, 50]",
                        "+I[u2, Eleven, o3, p3, 30]");

        assertThat(actual).containsExactlyInAnyOrderElementsOf(expected);
    }

    @Test
    public void testThreeWayInnerJoinNoCommonJoinKey() throws Exception {

        tEnv.executeSql(
                        "INSERT INTO ResultSink "
                                + "SELECT u.user_id, u.name, o.order_id, p.payment_id, p.price "
                                + "FROM Users u "
                                + "JOIN Orders o ON u.user_id = o.user_id "
                                + "JOIN Payments p ON u.cash = p.price")
                .await();

        List<String> actual = TestValuesTableFactory.getResultsAsStrings("ResultSink");

        List<String> expected =
                Arrays.asList(
                        "+I[u4, Mike, o5, p8, 200]",
                        "+I[u3, Dustin, o4, p7, 70]",
                        "+I[u2, Eleven, o3, p5, 50]",
                        "+I[u1, Will, o2, p9, 100]",
                        "+I[u1, Will, o2, p6, 100]",
                        "+I[u1, Will, o2, p4, 100]",
                        "+I[u1, Will, o2, p10, 100]",
                        "+I[u1, Will, o1, p9, 100]",
                        "+I[u1, Will, o1, p6, 100]",
                        "+I[u1, Will, o1, p4, 100]",
                        "+I[u1, Will, o1, p10, 100]",
                        "+I[u5, Lucas, o6, p9, 100]",
                        "+I[u5, Lucas, o6, p6, 100]",
                        "+I[u5, Lucas, o6, p4, 100]",
                        "+I[u5, Lucas, o6, p10, 100]");

        assertThat(actual).containsExactlyInAnyOrderElementsOf(expected);
    }

    @Test
    public void testThreeWayLeftInnerJoinNoCommonJoinKey() throws Exception {

        tEnv.executeSql(
                        "INSERT INTO ResultSink "
                                + "SELECT u.user_id, u.name, o.order_id, p.payment_id, p.price "
                                + "FROM Users u "
                                + "LEFT JOIN Orders o ON u.user_id = o.user_id "
                                + "JOIN Payments p ON u.cash = p.price")
                .await();

        List<String> actual = TestValuesTableFactory.getResultsAsStrings("ResultSink");

        List<String> expected =
                Arrays.asList(
                        "+I[u4, Mike, o5, p8, 200]",
                        "+I[u3, Dustin, o4, p7, 70]",
                        "+I[u2, Eleven, o3, p5, 50]",
                        "+I[u1, Will, o2, p9, 100]",
                        "+I[u1, Will, o2, p6, 100]",
                        "+I[u1, Will, o2, p4, 100]",
                        "+I[u1, Will, o2, p10, 100]",
                        "+I[u1, Will, o1, p9, 100]",
                        "+I[u1, Will, o1, p6, 100]",
                        "+I[u1, Will, o1, p4, 100]",
                        "+I[u1, Will, o1, p10, 100]",
                        "+I[u5, Lucas, o6, p9, 100]",
                        "+I[u5, Lucas, o6, p6, 100]",
                        "+I[u5, Lucas, o6, p4, 100]",
                        "+I[u5, Lucas, o6, p10, 100]");

        assertThat(actual).containsExactlyInAnyOrderElementsOf(expected);
    }

    @Test
    public void testThreeWayLeftLeftJoinNoCommonJoinKey() throws Exception {

        tEnv.executeSql(
                        "INSERT INTO ResultSink "
                                + "SELECT u.user_id, u.name, o.order_id, p.payment_id, p.price "
                                + "FROM Users u "
                                + "LEFT JOIN Orders o ON u.user_id = o.user_id "
                                + "LEFT JOIN Payments p ON u.cash = p.price")
                .await();

        List<String> actual = TestValuesTableFactory.getResultsAsStrings("ResultSink");

        List<String> expected =
                Arrays.asList(
                        "+I[u4, Mike, o5, p8, 200]",
                        "+I[u3, Dustin, o4, p7, 70]",
                        "+I[u2, Eleven, o3, p5, 50]",
                        "+I[u1, Will, o2, p9, 100]",
                        "+I[u1, Will, o2, p6, 100]",
                        "+I[u1, Will, o2, p4, 100]",
                        "+I[u1, Will, o2, p10, 100]",
                        "+I[u1, Will, o1, p9, 100]",
                        "+I[u1, Will, o1, p6, 100]",
                        "+I[u1, Will, o1, p4, 100]",
                        "+I[u1, Will, o1, p10, 100]",
                        "+I[u5, Lucas, o6, p9, 100]",
                        "+I[u5, Lucas, o6, p6, 100]",
                        "+I[u5, Lucas, o6, p4, 100]",
                        "+I[u5, Lucas, o6, p10, 100]");

        assertThat(actual).containsExactlyInAnyOrderElementsOf(expected);
    }

    @Test
    public void testThreeWayInnerLeftJoinNoCommonJoinKey() throws Exception {

        tEnv.executeSql(
                        "INSERT INTO ResultSink "
                                + "SELECT u.user_id, u.name, o.order_id, p.payment_id, p.price "
                                + "FROM Users u "
                                + "JOIN Orders o ON u.user_id = o.user_id "
                                + "LEFT JOIN Payments p ON u.cash = p.price")
                .await();

        List<String> actual = TestValuesTableFactory.getResultsAsStrings("ResultSink");

        List<String> expected =
                Arrays.asList(
                        "+I[u4, Mike, o5, p8, 200]",
                        "+I[u3, Dustin, o4, p7, 70]",
                        "+I[u2, Eleven, o3, p5, 50]",
                        "+I[u1, Will, o2, p9, 100]",
                        "+I[u1, Will, o2, p6, 100]",
                        "+I[u1, Will, o2, p4, 100]",
                        "+I[u1, Will, o2, p10, 100]",
                        "+I[u1, Will, o1, p9, 100]",
                        "+I[u1, Will, o1, p6, 100]",
                        "+I[u1, Will, o1, p4, 100]",
                        "+I[u1, Will, o1, p10, 100]",
                        "+I[u5, Lucas, o6, p9, 100]",
                        "+I[u5, Lucas, o6, p6, 100]",
                        "+I[u5, Lucas, o6, p4, 100]",
                        "+I[u5, Lucas, o6, p10, 100]");

        assertThat(actual).containsExactlyInAnyOrderElementsOf(expected);
    }

    @Test
    public void testThreeWayInnerJoinWithWhere() throws Exception {

        tEnv.executeSql(
                        "INSERT INTO ResultSink "
                                + "SELECT u.user_id, u.name, o.order_id, p.payment_id, p.price "
                                + "FROM Users u, Orders o, Payments p "
                                + "WHERE u.user_id = o.user_id "
                                + "AND o.user_id = p.user_id")
                .await();

        List<String> actual = TestValuesTableFactory.getResultsAsStrings("ResultSink");

        List<String> expected =
                Arrays.asList(
                        "+I[u1, Will, o1, p2, 20]",
                        "+I[u1, Will, o1, p1, 10]",
                        "+I[u1, Will, o1, p4, 100]",
                        "+I[u1, Will, o2, p2, 20]",
                        "+I[u1, Will, o2, p1, 10]",
                        "+I[u1, Will, o2, p4, 100]",
                        "+I[u2, Eleven, o3, p5, 50]",
                        "+I[u2, Eleven, o3, p3, 30]",
                        "+I[u5, Lucas, o6, p6, 100]");

        assertThat(actual).containsExactlyInAnyOrderElementsOf(expected);
    }

    @Test
    public void testThreeWayInnerJoinWithOr() throws Exception {

        tEnv.executeSql(
                        "INSERT INTO ResultSink "
                                + "SELECT u.user_id, u.name, o.order_id, p.payment_id, p.price "
                                + "FROM Users u "
                                + "JOIN Orders o ON u.user_id = o.user_id "
                                + "JOIN Payments p ON o.user_id = p.user_id OR p.price = u.cash")
                .await();

        List<String> actual = TestValuesTableFactory.getResultsAsStrings("ResultSink");

        List<String> expected =
                Arrays.asList(
                        "+I[u4, Mike, o5, p8, 200]",
                        "+I[u3, Dustin, o4, p7, 70]",
                        "+I[u1, Will, o2, p9, 100]",
                        "+I[u1, Will, o2, p2, 20]",
                        "+I[u1, Will, o2, p10, 100]",
                        "+I[u1, Will, o2, p1, 10]",
                        "+I[u1, Will, o2, p4, 100]",
                        "+I[u1, Will, o2, p6, 100]",
                        "+I[u1, Will, o1, p9, 100]",
                        "+I[u1, Will, o1, p2, 20]",
                        "+I[u1, Will, o1, p10, 100]",
                        "+I[u1, Will, o1, p1, 10]",
                        "+I[u1, Will, o1, p4, 100]",
                        "+I[u1, Will, o1, p6, 100]",
                        "+I[u5, Lucas, o6, p9, 100]",
                        "+I[u5, Lucas, o6, p10, 100]",
                        "+I[u5, Lucas, o6, p4, 100]",
                        "+I[u5, Lucas, o6, p6, 100]",
                        "+I[u2, Eleven, o3, p5, 50]",
                        "+I[u2, Eleven, o3, p3, 30]");

        assertThat(actual).containsExactlyInAnyOrderElementsOf(expected);
    }

    @Test
    public void testThreeWayLeftInnerJoinWithOr() throws Exception {

        tEnv.executeSql(
                        "INSERT INTO ResultSink "
                                + "SELECT u.user_id, u.name, o.order_id, p.payment_id, p.price "
                                + "FROM Users u "
                                + "LEFT JOIN Orders o ON u.user_id = o.user_id "
                                + "JOIN Payments p ON o.user_id = p.user_id OR p.price = u.cash")
                .await();

        List<String> actual = TestValuesTableFactory.getResultsAsStrings("ResultSink");

        List<String> expected =
                Arrays.asList(
                        "+I[u4, Mike, o5, p8, 200]",
                        "+I[u3, Dustin, o4, p7, 70]",
                        "+I[u1, Will, o2, p9, 100]",
                        "+I[u1, Will, o2, p2, 20]",
                        "+I[u1, Will, o2, p10, 100]",
                        "+I[u1, Will, o2, p1, 10]",
                        "+I[u1, Will, o2, p4, 100]",
                        "+I[u1, Will, o2, p6, 100]",
                        "+I[u1, Will, o1, p9, 100]",
                        "+I[u1, Will, o1, p2, 20]",
                        "+I[u1, Will, o1, p10, 100]",
                        "+I[u1, Will, o1, p1, 10]",
                        "+I[u1, Will, o1, p4, 100]",
                        "+I[u1, Will, o1, p6, 100]",
                        "+I[u5, Lucas, o6, p9, 100]",
                        "+I[u5, Lucas, o6, p10, 100]",
                        "+I[u5, Lucas, o6, p4, 100]",
                        "+I[u5, Lucas, o6, p6, 100]",
                        "+I[u2, Eleven, o3, p5, 50]",
                        "+I[u2, Eleven, o3, p3, 30]");

        assertThat(actual).containsExactlyInAnyOrderElementsOf(expected);
    }

    @Test
    public void testThreeWayLeftLeftJoinWithOr() throws Exception {

        tEnv.executeSql(
                        "INSERT INTO ResultSink "
                                + "SELECT u.user_id, u.name, o.order_id, p.payment_id, p.price "
                                + "FROM Users u "
                                + "LEFT JOIN Orders o ON u.user_id = o.user_id "
                                + "LEFT JOIN Payments p ON o.user_id = p.user_id OR p.price = u.cash")
                .await();

        List<String> actual = TestValuesTableFactory.getResultsAsStrings("ResultSink");

        List<String> expected =
                Arrays.asList(
                        "+I[u4, Mike, o5, p8, 200]",
                        "+I[u3, Dustin, o4, p7, 70]",
                        "+I[u1, Will, o2, p9, 100]",
                        "+I[u1, Will, o2, p2, 20]",
                        "+I[u1, Will, o2, p10, 100]",
                        "+I[u1, Will, o2, p1, 10]",
                        "+I[u1, Will, o2, p4, 100]",
                        "+I[u1, Will, o2, p6, 100]",
                        "+I[u1, Will, o1, p9, 100]",
                        "+I[u1, Will, o1, p2, 20]",
                        "+I[u1, Will, o1, p10, 100]",
                        "+I[u1, Will, o1, p1, 10]",
                        "+I[u1, Will, o1, p4, 100]",
                        "+I[u1, Will, o1, p6, 100]",
                        "+I[u5, Lucas, o6, p9, 100]",
                        "+I[u5, Lucas, o6, p10, 100]",
                        "+I[u5, Lucas, o6, p4, 100]",
                        "+I[u5, Lucas, o6, p6, 100]",
                        "+I[u2, Eleven, o3, p5, 50]",
                        "+I[u2, Eleven, o3, p3, 30]");

        assertThat(actual).containsExactlyInAnyOrderElementsOf(expected);
    }

    @Test
    public void testThreeWayInnerLeftJoinWithOr() throws Exception {

        tEnv.executeSql(
                        "INSERT INTO ResultSink "
                                + "SELECT u.user_id, u.name, o.order_id, p.payment_id, p.price "
                                + "FROM Users u "
                                + "JOIN Orders o ON u.user_id = o.user_id "
                                + "LEFT JOIN Payments p ON o.user_id = p.user_id OR p.price = u.cash")
                .await();

        List<String> actual = TestValuesTableFactory.getResultsAsStrings("ResultSink");

        List<String> expected =
                Arrays.asList(
                        "+I[u4, Mike, o5, p8, 200]",
                        "+I[u3, Dustin, o4, p7, 70]",
                        "+I[u1, Will, o2, p9, 100]",
                        "+I[u1, Will, o2, p2, 20]",
                        "+I[u1, Will, o2, p10, 100]",
                        "+I[u1, Will, o2, p1, 10]",
                        "+I[u1, Will, o2, p4, 100]",
                        "+I[u1, Will, o2, p6, 100]",
                        "+I[u1, Will, o1, p9, 100]",
                        "+I[u1, Will, o1, p2, 20]",
                        "+I[u1, Will, o1, p10, 100]",
                        "+I[u1, Will, o1, p1, 10]",
                        "+I[u1, Will, o1, p4, 100]",
                        "+I[u1, Will, o1, p6, 100]",
                        "+I[u5, Lucas, o6, p9, 100]",
                        "+I[u5, Lucas, o6, p10, 100]",
                        "+I[u5, Lucas, o6, p4, 100]",
                        "+I[u5, Lucas, o6, p6, 100]",
                        "+I[u2, Eleven, o3, p5, 50]",
                        "+I[u2, Eleven, o3, p3, 30]");

        assertThat(actual).containsExactlyInAnyOrderElementsOf(expected);
    }

    @Test
    public void testThreeWayInnerJoinComplexCondition() throws Exception {

        tEnv.executeSql(
                        "INSERT INTO ResultSink "
                                + "SELECT u.user_id, u.name, o.order_id, p.payment_id, p.price "
                                + "FROM Users u "
                                + "JOIN Orders o ON u.user_id = o.user_id AND u.cash >= 100"
                                + "JOIN Payments p ON o.user_id = p.user_id AND p.price = u.cash AND p.price < 200")
                .await();

        List<String> actual = TestValuesTableFactory.getResultsAsStrings("ResultSink");

        List<String> expected =
                Arrays.asList(
                        "+I[u5, Lucas, o6, p6, 100]",
                        "+I[u1, Will, o2, p4, 100]",
                        "+I[u1, Will, o1, p4, 100]");

        assertThat(actual).containsExactlyInAnyOrderElementsOf(expected);
    }

    @Test
    public void testThreeWayLeftInnerJoinComplexCondition() throws Exception {

        tEnv.executeSql(
                        "INSERT INTO ResultSink "
                                + "SELECT u.user_id, u.name, o.order_id, p.payment_id, p.price "
                                + "FROM Users u "
                                + "LEFT JOIN Orders o ON u.user_id = o.user_id AND u.cash >= 100"
                                + "JOIN Payments p ON o.user_id = p.user_id AND p.price = u.cash AND p.price < 200")
                .await();

        List<String> actual = TestValuesTableFactory.getResultsAsStrings("ResultSink");

        List<String> expected =
                Arrays.asList(
                        "+I[u5, Lucas, o6, p6, 100]",
                        "+I[u1, Will, o2, p4, 100]",
                        "+I[u1, Will, o1, p4, 100]");

        assertThat(actual).containsExactlyInAnyOrderElementsOf(expected);
    }

    @Test
    public void testThreeWayLeftLeftJoinComplexCondition() throws Exception {

        tEnv.executeSql(
                        "INSERT INTO ResultSink "
                                + "SELECT u.user_id, u.name, o.order_id, p.payment_id, p.price "
                                + "FROM Users u "
                                + "LEFT JOIN Orders o ON u.user_id = o.user_id AND u.cash >= 100"
                                + "LEFT JOIN Payments p ON o.user_id = p.user_id AND p.price = u.cash AND p.price < 200")
                .await();

        List<String> actual = TestValuesTableFactory.getResultsAsStrings("ResultSink");

        List<String> expected =
                Arrays.asList(
                        "+I[u4, Mike, o5, null, null]",
                        "+I[u3, Dustin, null, null, null]",
                        "+I[u5, Lucas, o6, p6, 100]",
                        "+I[u2, Eleven, null, null, null]",
                        "+I[u1, Will, o1, p4, 100]",
                        "+I[u1, Will, o2, p4, 100]");

        assertThat(actual).containsExactlyInAnyOrderElementsOf(expected);
    }

    @Test
    public void testThreeWayInnerLeftJoinComplexCondition() throws Exception {

        tEnv.executeSql(
                        "INSERT INTO ResultSink "
                                + "SELECT u.user_id, u.name, o.order_id, p.payment_id, p.price "
                                + "FROM Users u "
                                + "JOIN Orders o ON u.user_id = o.user_id AND u.cash >= 100"
                                + "LEFT JOIN Payments p ON o.user_id = p.user_id AND p.price = u.cash AND p.price < 200")
                .await();

        List<String> actual = TestValuesTableFactory.getResultsAsStrings("ResultSink");

        List<String> expected =
                Arrays.asList(
                        "+I[u4, Mike, o5, null, null]",
                        "+I[u5, Lucas, o6, p6, 100]",
                        "+I[u1, Will, o1, p4, 100]",
                        "+I[u1, Will, o2, p4, 100]");

        assertThat(actual).containsExactlyInAnyOrderElementsOf(expected);
    }

    @Test
    public void testThreeWayInnerJoinComplexConditionV2() throws Exception {

        tEnv.executeSql(
                        "INSERT INTO ResultSink "
                                + "SELECT u.user_id, u.name, o.order_id, p.payment_id, p.price "
                                + "FROM Users u "
                                + "JOIN Orders o ON u.user_id = o.user_id AND u.cash >= 100 "
                                + "JOIN Payments p ON p.price = u.cash AND (o.user_id = p.user_id OR u.name = 'Lucas') ")
                .await();

        List<String> actual = TestValuesTableFactory.getResultsAsStrings("ResultSink");

        List<String> expected =
                Arrays.asList(
                        "+I[u1, Will, o1, p4, 100]",
                        "+I[u1, Will, o2, p4, 100]",
                        "+I[u5, Lucas, o6, p9, 100]",
                        "+I[u5, Lucas, o6, p10, 100]",
                        "+I[u5, Lucas, o6, p4, 100]",
                        "+I[u5, Lucas, o6, p6, 100]");

        assertThat(actual).containsExactlyInAnyOrderElementsOf(expected);
    }

    @Test
    public void testThreeWayLeftInnerJoinComplexConditionV2() throws Exception {

        tEnv.executeSql(
                        "INSERT INTO ResultSink "
                                + "SELECT u.user_id, u.name, o.order_id, p.payment_id, p.price "
                                + "FROM Users u "
                                + "LEFT JOIN Orders o ON u.user_id = o.user_id AND u.cash >= 100 "
                                + "JOIN Payments p ON p.price = u.cash AND (o.user_id = p.user_id OR u.name = 'Lucas') ")
                .await();

        List<String> actual = TestValuesTableFactory.getResultsAsStrings("ResultSink");

        List<String> expected =
                Arrays.asList(
                        "+I[u1, Will, o1, p4, 100]",
                        "+I[u1, Will, o2, p4, 100]",
                        "+I[u5, Lucas, o6, p9, 100]",
                        "+I[u5, Lucas, o6, p10, 100]",
                        "+I[u5, Lucas, o6, p4, 100]",
                        "+I[u5, Lucas, o6, p6, 100]");

        assertThat(actual).containsExactlyInAnyOrderElementsOf(expected);
    }

    @Test
    public void testThreeWayLeftLeftJoinComplexConditionV2() throws Exception {

        tEnv.executeSql(
                        "INSERT INTO ResultSink "
                                + "SELECT u.user_id, u.name, o.order_id, p.payment_id, p.price "
                                + "FROM Users u "
                                + "LEFT JOIN Orders o ON u.user_id = o.user_id AND u.cash >= 100 "
                                + "LEFT JOIN Payments p ON p.price = u.cash AND (o.user_id = p.user_id OR u.name = 'Lucas') ")
                .await();

        List<String> actual = TestValuesTableFactory.getResultsAsStrings("ResultSink");

        List<String> expected =
                Arrays.asList(
                        "+I[u4, Mike, o5, null, null]",
                        "+I[u3, Dustin, null, null, null]",
                        "+I[u2, Eleven, null, null, null]",
                        "+I[u5, Lucas, o6, p9, 100]",
                        "+I[u5, Lucas, o6, p10, 100]",
                        "+I[u5, Lucas, o6, p4, 100]",
                        "+I[u5, Lucas, o6, p6, 100]",
                        "+I[u1, Will, o2, p4, 100]",
                        "+I[u1, Will, o1, p4, 100]");

        assertThat(actual).containsExactlyInAnyOrderElementsOf(expected);
    }

    @Test
    public void testThreeWayInnerLeftJoinComplexConditionV2() throws Exception {

        tEnv.executeSql(
                        "INSERT INTO ResultSink "
                                + "SELECT u.user_id, u.name, o.order_id, p.payment_id, p.price "
                                + "FROM Users u "
                                + "JOIN Orders o ON u.user_id = o.user_id AND u.cash >= 100 "
                                + "LEFT JOIN Payments p ON p.price = u.cash AND (o.user_id = p.user_id OR u.name = 'Lucas') ")
                .await();

        List<String> actual = TestValuesTableFactory.getResultsAsStrings("ResultSink");

        List<String> expected =
                Arrays.asList(
                        "+I[u4, Mike, o5, null, null]",
                        "+I[u1, Will, o2, p4, 100]",
                        "+I[u1, Will, o1, p4, 100]",
                        "+I[u5, Lucas, o6, p9, 100]",
                        "+I[u5, Lucas, o6, p10, 100]",
                        "+I[u5, Lucas, o6, p4, 100]",
                        "+I[u5, Lucas, o6, p6, 100]");

        assertThat(actual).containsExactlyInAnyOrderElementsOf(expected);
    }

    @Test
    public void testThreeWayInnerJoinConstantCondition() throws Exception {

        tEnv.executeSql(
                        "INSERT INTO ResultSink "
                                + "SELECT u.user_id, u.name, o.order_id, p.payment_id, p.price "
                                + "FROM Users u "
                                + "JOIN Orders o ON TRUE "
                                + "JOIN Payments p ON o.user_id = p.user_id AND p.price = u.cash AND p.price < 200")
                .await();

        List<String> actual = TestValuesTableFactory.getResultsAsStrings("ResultSink");

        List<String> expected =
                Arrays.asList(
                        "+I[u2, Eleven, o3, p5, 50]",
                        "+I[u5, Lucas, o6, p6, 100]",
                        "+I[u1, Will, o6, p6, 100]",
                        "+I[u1, Will, o1, p4, 100]",
                        "+I[u1, Will, o2, p4, 100]",
                        "+I[u5, Lucas, o2, p4, 100]",
                        "+I[u5, Lucas, o1, p4, 100]");

        assertThat(actual).containsExactlyInAnyOrderElementsOf(expected);
    }

    @Test
    public void testThreeWayLeftInnerJoinConstantCondition() throws Exception {

        tEnv.executeSql(
                        "INSERT INTO ResultSink "
                                + "SELECT u.user_id, u.name, o.order_id, p.payment_id, p.price "
                                + "FROM Users u "
                                + "LEFT JOIN Orders o ON TRUE "
                                + "JOIN Payments p ON o.user_id = p.user_id AND p.price = u.cash AND p.price < 200")
                .await();

        List<String> actual = TestValuesTableFactory.getResultsAsStrings("ResultSink");

        List<String> expected =
                Arrays.asList(
                        "+I[u2, Eleven, o3, p5, 50]",
                        "+I[u5, Lucas, o6, p6, 100]",
                        "+I[u1, Will, o6, p6, 100]",
                        "+I[u1, Will, o1, p4, 100]",
                        "+I[u1, Will, o2, p4, 100]",
                        "+I[u5, Lucas, o2, p4, 100]",
                        "+I[u5, Lucas, o1, p4, 100]");

        assertThat(actual).containsExactlyInAnyOrderElementsOf(expected);
    }

    @Test
    public void testThreeWayLeftLeftJoinConstantCondition() throws Exception {

        tEnv.executeSql(
                        "INSERT INTO ResultSink "
                                + "SELECT u.user_id, u.name, o.order_id, p.payment_id, p.price "
                                + "FROM Users u "
                                + "LEFT JOIN Orders o ON TRUE "
                                + "LEFT JOIN Payments p ON o.user_id = p.user_id AND p.price = u.cash AND p.price < 200")
                .await();

        List<String> actual = TestValuesTableFactory.getResultsAsStrings("ResultSink");

        List<String> expected =
                Arrays.asList(
                        "+I[u1, Will, o3, null, null]",
                        "+I[u2, Eleven, o3, p5, 50]",
                        "+I[u3, Dustin, o2, null, null]",
                        "+I[u3, Dustin, o4, null, null]",
                        "+I[u5, Lucas, o3, null, null]",
                        "+I[u3, Dustin, o1, null, null]",
                        "+I[u4, Mike, o5, null, null]",
                        "+I[u4, Mike, o6, null, null]",
                        "+I[u4, Mike, o2, null, null]",
                        "+I[u4, Mike, o1, null, null]",
                        "+I[u4, Mike, o4, null, null]",
                        "+I[u2, Eleven, o5, null, null]",
                        "+I[u5, Lucas, o6, p6, 100]",
                        "+I[u2, Eleven, o6, null, null]",
                        "+I[u1, Will, o6, p6, 100]",
                        "+I[u2, Eleven, o4, null, null]",
                        "+I[u1, Will, o2, p4, 100]",
                        "+I[u1, Will, o1, p4, 100]",
                        "+I[u5, Lucas, o1, p4, 100]",
                        "+I[u5, Lucas, o2, p4, 100]",
                        "+I[u1, Will, o4, null, null]",
                        "+I[u5, Lucas, o5, null, null]",
                        "+I[u1, Will, o5, null, null]",
                        "+I[u5, Lucas, o4, null, null]",
                        "+I[u2, Eleven, o2, null, null]",
                        "+I[u2, Eleven, o1, null, null]",
                        "+I[u3, Dustin, o3, null, null]",
                        "+I[u3, Dustin, o6, null, null]",
                        "+I[u4, Mike, o3, null, null]",
                        "+I[u3, Dustin, o5, null, null]");

        assertThat(actual).containsExactlyInAnyOrderElementsOf(expected);
    }

    @Test
    public void testThreeWayInnerLeftJoinConstantCondition() throws Exception {

        tEnv.executeSql(
                        "INSERT INTO ResultSink "
                                + "SELECT u.user_id, u.name, o.order_id, p.payment_id, p.price "
                                + "FROM Users u "
                                + "JOIN Orders o ON TRUE "
                                + "LEFT JOIN Payments p ON o.user_id = p.user_id AND p.price = u.cash AND p.price < 200")
                .await();

        List<String> actual = TestValuesTableFactory.getResultsAsStrings("ResultSink");

        List<String> expected =
                Arrays.asList(
                        "+I[u1, Will, o3, null, null]",
                        "+I[u3, Dustin, o2, null, null]",
                        "+I[u3, Dustin, o1, null, null]",
                        "+I[u3, Dustin, o4, null, null]",
                        "+I[u4, Mike, o5, null, null]",
                        "+I[u2, Eleven, o3, p5, 50]",
                        "+I[u5, Lucas, o3, null, null]",
                        "+I[u4, Mike, o6, null, null]",
                        "+I[u4, Mike, o2, null, null]",
                        "+I[u4, Mike, o1, null, null]",
                        "+I[u2, Eleven, o5, null, null]",
                        "+I[u1, Will, o6, p6, 100]",
                        "+I[u2, Eleven, o6, null, null]",
                        "+I[u4, Mike, o4, null, null]",
                        "+I[u5, Lucas, o6, p6, 100]",
                        "+I[u2, Eleven, o4, null, null]",
                        "+I[u1, Will, o1, p4, 100]",
                        "+I[u1, Will, o2, p4, 100]",
                        "+I[u5, Lucas, o2, p4, 100]",
                        "+I[u1, Will, o5, null, null]",
                        "+I[u1, Will, o4, null, null]",
                        "+I[u5, Lucas, o1, p4, 100]",
                        "+I[u5, Lucas, o5, null, null]",
                        "+I[u5, Lucas, o4, null, null]",
                        "+I[u2, Eleven, o2, null, null]",
                        "+I[u2, Eleven, o1, null, null]",
                        "+I[u3, Dustin, o3, null, null]",
                        "+I[u3, Dustin, o6, null, null]",
                        "+I[u4, Mike, o3, null, null]",
                        "+I[u3, Dustin, o5, null, null]");

        assertThat(actual).containsExactlyInAnyOrderElementsOf(expected);
    }

    @Parameterized.Parameters(name = "miniBatch={0}, stateBackend={1}")
    public static Collection<Object[]> parameters() {
        return Arrays.asList(
                new Object[] {MiniBatchOn(), HEAP_BACKEND()},
                new Object[] {MiniBatchOff(), HEAP_BACKEND()},
                new Object[] {MiniBatchOn(), ROCKSDB_BACKEND()},
                new Object[] {MiniBatchOff(), ROCKSDB_BACKEND()});
    }
}
