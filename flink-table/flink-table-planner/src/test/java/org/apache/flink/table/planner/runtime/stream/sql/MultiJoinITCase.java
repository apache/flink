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

import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.table.api.DataTypes;
import org.apache.flink.table.api.Schema;
import org.apache.flink.table.api.bridge.scala.StreamTableEnvironment;
import org.apache.flink.table.api.config.OptimizerConfigOptions;
import org.apache.flink.table.planner.factories.TestValuesTableFactory;
import org.apache.flink.table.planner.runtime.utils.StreamingWithMiniBatchTestBase;
import org.apache.flink.table.types.AbstractDataType;
import org.apache.flink.table.types.UnresolvedDataType;
import org.apache.flink.types.Row;

import org.apache.calcite.rel.rules.MultiJoin;
import org.junit.After;
import org.junit.Before;
import org.junit.Ignore;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

import java.util.Arrays;
import java.util.Collection;
import java.util.List;
import java.util.concurrent.ExecutionException;

import scala.collection.JavaConverters;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Integration tests for {@link MultiJoin} that verify consistency when executed with a parallelism
 * greater than 1.
 */
@RunWith(Parameterized.class)
public class MultiJoinITCase extends StreamingWithMiniBatchTestBase {

    private StreamTableEnvironment tEnv;

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

    // Table data for consistency with JoinITCase
    List<Row> simple3Data =
            Arrays.asList(
                    Row.of(1, 1L, "Hi"),
                    Row.of(2, 2L, "Hello"),
                    Row.of(3, 2L, "Hello world"),
                    Row.of(5, 2L, "Hello world"));

    List<Row> simple5Data =
            Arrays.asList(
                    Row.of(1, 1L, 0, "Hallo", 1L),
                    Row.of(2, 2L, 1, "Hallo Welt", 2L),
                    Row.of(2, 3L, 2, "Hallo Welt wie", 1L),
                    Row.of(3, 4L, 3, "Hallo Welt wie gehts?", 2L),
                    Row.of(3, 5L, 4, "ABC", 2L),
                    Row.of(3, 6L, 5, "BCD", 3L));

    @Before
    @Override
    public void before() {
        super.before();

        tEnv = tEnv();
        tEnv.getConfig().set(OptimizerConfigOptions.TABLE_OPTIMIZER_MULTI_JOIN_ENABLED, true);
    }

    private void createTableView(
            String name,
            List<Row> data,
            String[] typeNames,
            TypeInformation<?>[] types,
            String[] primaryKeys) {
        TypeInformation<Row> rowTypeInformation = Types.ROW_NAMED(typeNames, types);
        DataStream<Row> dataStream =
                failingDataSource(JavaConverters.asScalaBuffer(data).toSeq(), rowTypeInformation);
        Schema.Builder schemaBuilder = Schema.newBuilder();
        for (int i = 0; i < typeNames.length; i++) {
            AbstractDataType<UnresolvedDataType> dt = DataTypes.of(types[i]);

            if (primaryKeys != null && Arrays.asList(primaryKeys).contains(typeNames[i])) {
                dt = dt.notNull();
            }
            schemaBuilder.column(typeNames[i], dt);
        }
        if (primaryKeys != null && primaryKeys.length > 0) {
            schemaBuilder.primaryKey(primaryKeys);
        }
        tEnv.createTemporaryView(name, dataStream, schemaBuilder.build());
    }

    private void createDefaultViews() {
        createTableView(
                "Users",
                usersData,
                new String[] {"user_id", "name", "cash"},
                new TypeInformation[] {Types.STRING, Types.STRING, Types.INT},
                null);

        createTableView(
                "Orders",
                ordersData,
                new String[] {"order_id", "user_id", "product"},
                new TypeInformation[] {Types.STRING, Types.STRING, Types.STRING},
                null);

        createTableView(
                "Payments",
                paymentsData,
                new String[] {"payment_id", "price", "user_id"},
                new TypeInformation[] {Types.STRING, Types.INT, Types.STRING},
                null);
    }

    private void createDefaultSink() {
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

    /**
     * This test will be enabled after <a
     * href="https://issues.apache.org/jira/browse/FLINK-39015">FLINK-39015</a> is merged.
     */
    @Ignore
    @Test
    public void testTwoWayInnerJoinWithEqualPk() throws ExecutionException, InterruptedException {
        createTableView(
                "A",
                simple3Data,
                new String[] {"a1", "a2", "a3"},
                new TypeInformation[] {Types.INT, Types.LONG, Types.STRING},
                null);

        createTableView(
                "B",
                simple5Data,
                new String[] {"b1", "b2", "b3", "b4", "b5"},
                new TypeInformation[] {Types.INT, Types.LONG, Types.INT, Types.STRING, Types.LONG},
                null);

        tEnv.executeSql(
                "CREATE TABLE ABSink ("
                        + "  a1 INT,"
                        + "  b1 INT"
                        + ") WITH ("
                        + "  'connector' = 'values',"
                        + "  'sink-insert-only' = 'false'"
                        + ")");

        String query1 = "SELECT SUM(a2) AS a2, a1 FROM A group by a1";
        String query2 = "SELECT SUM(b2) AS b2, b1 FROM B group by b1";
        String query =
                "INSERT INTO ABSink SELECT a1, b1 FROM ("
                        + query1
                        + ") JOIN ("
                        + query2
                        + ") ON a1 = b1";
        tEnv.executeSql(query).await();

        List<String> actual = TestValuesTableFactory.getResultsAsStrings("ABSink");

        List<String> expected = Arrays.asList("+I[1, 1]", "+I[2, 2]", "+I[3, 3]");

        assertThat(actual).containsExactlyInAnyOrderElementsOf(expected);
    }

    @Test
    public void testThreeWayInnerJoinMatchingBooleans()
            throws ExecutionException, InterruptedException {
        List<Row> tableAData =
                Arrays.asList(Row.of(1, true), Row.of(2, false), Row.of(3, true), Row.of(4, false));

        List<Row> tableBData = Arrays.asList(Row.of(5, true), Row.of(6, false), Row.of(7, false));
        List<Row> tableCData = Arrays.asList(Row.of(8, false), Row.of(9, false), Row.of(10, true));

        createTableView(
                "A",
                tableAData,
                new String[] {"a1", "a2"},
                new TypeInformation[] {Types.INT, Types.BOOLEAN},
                new String[] {"a1"});

        createTableView(
                "B",
                tableBData,
                new String[] {"b1", "b2"},
                new TypeInformation[] {Types.INT, Types.BOOLEAN},
                new String[] {"b1"});

        createTableView(
                "C",
                tableCData,
                new String[] {"c1", "c2"},
                new TypeInformation[] {Types.INT, Types.BOOLEAN},
                new String[] {"c1"});

        tEnv.executeSql(
                "CREATE TABLE ABCSink ("
                        + "  a1 INT,"
                        + "  b1 INT,"
                        + "  c1 INT"
                        + ") WITH ("
                        + "  'connector' = 'values',"
                        + "  'sink-insert-only' = 'false'"
                        + ")");

        tEnv.executeSql(
                        "INSERT INTO ABCSink "
                                + "SELECT a1, b1, c1 "
                                + "FROM A "
                                + "JOIN B ON a2 = b2 "
                                + "JOIN C ON b2 = c2")
                .await();

        List<String> actual = TestValuesTableFactory.getResultsAsStrings("ABCSink");

        List<String> expected =
                Arrays.asList(
                        "+I[1, 5, 10]",
                        "+I[3, 5, 10]",
                        "+I[2, 7, 8]",
                        "+I[2, 7, 9]",
                        "+I[2, 6, 8]",
                        "+I[2, 6, 9]",
                        "+I[4, 6, 8]",
                        "+I[4, 6, 9]",
                        "+I[4, 7, 8]",
                        "+I[4, 7, 9]");

        assertThat(actual).containsExactlyInAnyOrderElementsOf(expected);
    }

    @Test
    public void testThreeWayInnerJoinMatchingBooleansNoCommonJoinKey()
            throws ExecutionException, InterruptedException {
        List<Row> tableAData =
                Arrays.asList(Row.of(1, true), Row.of(2, false), Row.of(3, true), Row.of(4, false));

        List<Row> tableBData = Arrays.asList(Row.of(5, true), Row.of(6, false), Row.of(7, false));
        List<Row> tableCData = Arrays.asList(Row.of(8, false), Row.of(9, false), Row.of(10, true));

        createTableView(
                "A",
                tableAData,
                new String[] {"a1", "a2"},
                new TypeInformation[] {Types.INT, Types.BOOLEAN},
                new String[] {"a1"});

        createTableView(
                "B",
                tableBData,
                new String[] {"b1", "b2"},
                new TypeInformation[] {Types.INT, Types.BOOLEAN},
                new String[] {"b1"});

        createTableView(
                "C",
                tableCData,
                new String[] {"c1", "c2"},
                new TypeInformation[] {Types.INT, Types.BOOLEAN},
                new String[] {"c1"});

        tEnv.executeSql(
                "CREATE TABLE ABCSink ("
                        + "  a1 INT,"
                        + "  b1 INT,"
                        + "  c1 INT"
                        + ") WITH ("
                        + "  'connector' = 'values',"
                        + "  'sink-insert-only' = 'false'"
                        + ")");

        tEnv.executeSql(
                        "INSERT INTO ABCSink "
                                + "SELECT a1, b1, c1 "
                                + "FROM A "
                                + "JOIN B ON a2 = true "
                                + "JOIN C ON b2 = false")
                .await();

        List<String> actual = TestValuesTableFactory.getResultsAsStrings("ABCSink");

        List<String> expected =
                Arrays.asList(
                        "+I[3, 6, 8]",
                        "+I[3, 6, 9]",
                        "+I[3, 6, 10]",
                        "+I[3, 7, 8]",
                        "+I[3, 7, 9]",
                        "+I[3, 7, 10]",
                        "+I[1, 6, 8]",
                        "+I[1, 6, 9]",
                        "+I[1, 6, 10]",
                        "+I[1, 7, 8]",
                        "+I[1, 7, 9]",
                        "+I[1, 7, 10]");

        assertThat(actual).containsExactlyInAnyOrderElementsOf(expected);
    }

    @Test
    public void testThreeWayInnerJoinMatchingBooleansV2()
            throws ExecutionException, InterruptedException {
        List<Row> tableAData =
                Arrays.asList(Row.of(1, true), Row.of(2, false), Row.of(3, true), Row.of(4, false));

        List<Row> tableBData = Arrays.asList(Row.of(5, true), Row.of(6, false), Row.of(7, false));
        List<Row> tableCData = Arrays.asList(Row.of(8, false), Row.of(9, false), Row.of(10, true));

        createTableView(
                "A",
                tableAData,
                new String[] {"a1", "a2"},
                new TypeInformation[] {Types.INT, Types.BOOLEAN},
                new String[] {"a1"});

        createTableView(
                "B",
                tableBData,
                new String[] {"b1", "b2"},
                new TypeInformation[] {Types.INT, Types.BOOLEAN},
                new String[] {"b1"});

        createTableView(
                "C",
                tableCData,
                new String[] {"c1", "c2"},
                new TypeInformation[] {Types.INT, Types.BOOLEAN},
                new String[] {"c1"});

        tEnv.executeSql(
                "CREATE TABLE ABCSink ("
                        + "  a1 INT,"
                        + "  b1 INT,"
                        + "  c1 INT"
                        + ") WITH ("
                        + "  'connector' = 'values',"
                        + "  'sink-insert-only' = 'false'"
                        + ")");

        tEnv.executeSql(
                        "INSERT INTO ABCSink "
                                + "SELECT a1, b1, c1 "
                                + "FROM A "
                                + "JOIN B ON a2 = true "
                                + "JOIN C ON b2 = true AND c2 = true")
                .await();

        List<String> actual = TestValuesTableFactory.getResultsAsStrings("ABCSink");

        List<String> expected = Arrays.asList("+I[1, 5, 10]", "+I[3, 5, 10]");

        assertThat(actual).containsExactlyInAnyOrderElementsOf(expected);
    }

    @Test
    public void testThreeWayInnerJoinNestedType() throws ExecutionException, InterruptedException {
        List<Row> tableAData =
                Arrays.asList(
                        Row.of(1, Row.of("u1", "EU")),
                        Row.of(2, Row.of("u2", "US")),
                        Row.of(3, Row.of("u3", "EU")),
                        Row.of(4, Row.of("u4", "EU")));

        List<Row> tableBData =
                Arrays.asList(
                        Row.of(5, Row.of("u1", "EU")),
                        Row.of(6, Row.of("u2", "US")),
                        Row.of(7, Row.of("u5", "EU")));

        List<Row> tableCData =
                Arrays.asList(
                        Row.of(8, Row.of("u1", "EU")),
                        Row.of(9, Row.of("u2", "US")),
                        Row.of(10, Row.of("u3", "EU")));

        TypeInformation<Row> userType =
                Types.ROW_NAMED(new String[] {"id", "region"}, Types.STRING, Types.STRING);

        createTableView(
                "A",
                tableAData,
                new String[] {"id", "user_info"},
                new TypeInformation[] {Types.INT, userType},
                new String[] {"id"});

        createTableView(
                "B",
                tableBData,
                new String[] {"id", "user_info"},
                new TypeInformation[] {Types.INT, userType},
                new String[] {"id"});

        createTableView(
                "C",
                tableCData,
                new String[] {"id", "user_info"},
                new TypeInformation[] {Types.INT, userType},
                new String[] {"id"});

        tEnv.executeSql(
                "CREATE TABLE ABCSink ("
                        + "  a1 INT,"
                        + "  b1 INT,"
                        + "  c1 INT"
                        + ") WITH ("
                        + "  'connector' = 'values',"
                        + "  'sink-insert-only' = 'false'"
                        + ")");

        tEnv.executeSql(
                        "INSERT INTO ABCSink "
                                + "SELECT A.id, B.id, C.id "
                                + "FROM A "
                                + "JOIN B ON A.user_info = B.user_info "
                                + "JOIN C ON B.user_info = C.user_info")
                .await();

        List<String> actual = TestValuesTableFactory.getResultsAsStrings("ABCSink");

        List<String> expected = Arrays.asList("+I[2, 6, 9]", "+I[1, 5, 8]");

        assertThat(actual).containsExactlyInAnyOrderElementsOf(expected);
    }

    @Test
    public void testThreeWayInnerJoin() throws Exception {
        createDefaultViews();
        createDefaultSink();

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
        createDefaultViews();
        createDefaultSink();

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
        createDefaultViews();
        createDefaultSink();

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
        createDefaultViews();
        createDefaultSink();

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
    public void testThreeWayInnerJoinWithIsNotDistinct() throws Exception {
        createDefaultViews();
        createDefaultSink();

        // Is not distinct from does not respect common join key
        tEnv.executeSql(
                        "INSERT INTO ResultSink "
                                + "SELECT u.user_id, u.name, o.order_id, p.payment_id, p.price "
                                + "FROM Users u "
                                + "JOIN Orders o ON u.user_id IS NOT DISTINCT FROM o.user_id "
                                + "JOIN Payments p ON o.user_id IS NOT DISTINCT FROM p.user_id")
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
    public void testThreeWayInnerJoinNoCommonJoinKey() throws Exception {
        createDefaultViews();
        createDefaultSink();

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
        createDefaultViews();
        createDefaultSink();

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
        createDefaultViews();
        createDefaultSink();

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
        createDefaultViews();
        createDefaultSink();

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
        createDefaultViews();
        createDefaultSink();

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

    /**
     * Tests with OR in join condition will be enabled after <a
     * href="https://issues.apache.org/jira/browse/FLINK-38916">FLINK-38916</a> is resolved.
     */
    @Ignore
    @Test
    public void testThreeWayInnerJoinWithOr() throws Exception {
        createDefaultViews();
        createDefaultSink();

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

    @Ignore
    @Test
    public void testThreeWayLeftInnerJoinWithOr() throws Exception {
        createDefaultViews();
        createDefaultSink();

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

    @Ignore
    @Test
    public void testThreeWayLeftLeftJoinWithOr() throws Exception {
        createDefaultViews();
        createDefaultSink();

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

    @Ignore
    @Test
    public void testThreeWayInnerLeftJoinWithOr() throws Exception {
        createDefaultViews();
        createDefaultSink();

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
        createDefaultViews();
        createDefaultSink();

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
        createDefaultViews();
        createDefaultSink();

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
        createDefaultViews();
        createDefaultSink();

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
        createDefaultViews();
        createDefaultSink();

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

    @Ignore
    @Test
    public void testThreeWayInnerJoinComplexConditionV2() throws Exception {
        createDefaultViews();
        createDefaultSink();

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

    @Ignore
    @Test
    public void testThreeWayLeftInnerJoinComplexConditionV2() throws Exception {
        createDefaultViews();
        createDefaultSink();

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

    @Ignore
    @Test
    public void testThreeWayLeftLeftJoinComplexConditionV2() throws Exception {
        createDefaultViews();
        createDefaultSink();

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

    @Ignore
    @Test
    public void testThreeWayInnerLeftJoinComplexConditionV2() throws Exception {
        createDefaultViews();
        createDefaultSink();

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
        createDefaultViews();
        createDefaultSink();

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
        createDefaultViews();
        createDefaultSink();

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
        createDefaultViews();
        createDefaultSink();

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
        createDefaultViews();
        createDefaultSink();

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
