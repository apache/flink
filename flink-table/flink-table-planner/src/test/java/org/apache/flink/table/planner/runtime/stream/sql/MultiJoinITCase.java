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
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.DataTypes;
import org.apache.flink.table.api.Schema;
import org.apache.flink.table.api.TableException;
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
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TestName;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.nio.file.StandardOpenOption;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.List;

import scala.collection.JavaConverters;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Integration tests for {@link MultiJoin} that verify consistency when executed with a parallelism
 * greater than 1.
 */
@RunWith(Parameterized.class)
public class MultiJoinITCase extends StreamingWithMiniBatchTestBase {

    private StreamExecutionEnvironment env;

    @Rule public TestName testName = new TestName();

    public MultiJoinITCase(MiniBatchMode miniBatch, StateBackendMode state) {
        super(miniBatch, state);
    }

    static List<Row> usersData =
            Arrays.asList(
                    Row.of("u1", "Will", 100),
                    Row.of("u2", "Eleven", 50),
                    Row.of("u3", "Dustin", 70),
                    Row.of("u4", "Mike", 200),
                    Row.of("u5", "Lucas", 100));

    static List<Row> ordersData =
            Arrays.asList(
                    Row.of("o1", "u1", "Map"),
                    Row.of("o2", "u1", "Flashlight"),
                    Row.of("o3", "u2", "Waffles"),
                    Row.of("o4", "u3", "Bike"),
                    Row.of("o5", "u4", "Comics"),
                    Row.of("o6", "u5", "Radio set"));

    static List<Row> paymentsData =
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

        env = env();
    }

    @After
    @Override
    public void after() {
        super.after();
    }

    private void createTableView(TableInfo tableInfo, StreamTableEnvironment env) {
        String[] columnNames = tableInfo.getColumnNames();
        TypeInformation<?>[] columnTypes = tableInfo.getColumnTypes();
        List<String> primaryKeys = tableInfo.getPrimaryKeys();

        TypeInformation<Row> rowTypeInformation =
                Types.ROW_NAMED(columnNames, tableInfo.getColumnTypes());
        DataStream<Row> dataStream =
                failingDataSource(
                        JavaConverters.asScalaBuffer(tableInfo.getData()).toSeq(),
                        rowTypeInformation);
        Schema.Builder schemaBuilder = Schema.newBuilder();

        for (int i = 0; i < columnNames.length; i++) {
            AbstractDataType<UnresolvedDataType> dt = DataTypes.of(columnTypes[i]);

            if (primaryKeys.contains(columnNames[i])) {
                dt = dt.notNull();
            }

            schemaBuilder.column(columnNames[i], dt);
        }

        if (!primaryKeys.isEmpty()) {
            schemaBuilder.primaryKey(primaryKeys);
        }

        env.createTemporaryView(tableInfo.getName(), dataStream, schemaBuilder.build());
    }

    public List<String> executeQuery(
            TableInfo[] sourceTables, TableInfo sinkTable, String query, boolean useMultiJoin)
            throws Exception {
        TestValuesTableFactory.clearAllData();

        StreamTableEnvironment tEnv = StreamTableEnvironment.create(env);

        tEnv.getConfig()
                .set(OptimizerConfigOptions.TABLE_OPTIMIZER_MULTI_JOIN_ENABLED, useMultiJoin);

        for (TableInfo sourceTable : sourceTables) {
            createTableView(sourceTable, tEnv);
        }

        createSinkTable(tEnv, sinkTable);

        tEnv.executeSql(query).await();

        return TestValuesTableFactory.getResultsAsStrings(sinkTable.getName());
    }

    private void createSinkTable(StreamTableEnvironment tEnv, TableInfo sinkTable) {
        StringBuilder sinkQuery = new StringBuilder("CREATE TABLE " + sinkTable.getName() + " (");
        String[] sinkColumnNames = sinkTable.getColumnNames();
        TypeInformation<?>[] sinkColumnTypes = sinkTable.getColumnTypes();
        for (int i = 0; i < sinkColumnNames.length; i++) {
            sinkQuery.append(sinkColumnNames[i]).append(" ").append(sinkColumnTypes[i]);
            if (i < sinkColumnNames.length - 1) {
                sinkQuery.append(", ");
            }
        }
        sinkQuery.append(
                ") WITH (" + "  'connector' = 'values'," + "  'sink-insert-only' = 'false'" + ")");

        tEnv.executeSql(sinkQuery.toString());
    }

    private List<String> getExpectedResult(
            TableInfo[] sourceTables, TableInfo sinkTable, String query, String name)
            throws Exception {
        name = name.replaceAll("\\[.*?]", "");
        Path pathToTestFile =
                Paths.get(
                        String.format(
                                "%s/src/test/resources/multijoin-it-case/%s.out",
                                System.getProperty("user.dir"), name));
        File testFile = pathToTestFile.toFile();
        List<String> result;

        if (testFile.exists()) {
            try {
                result = Files.readAllLines(pathToTestFile);
            } catch (IOException e) {
                throw new TableException(
                        "Cannot read the test result from the file '" + testFile + "'.", e);
            }
        } else {
            result = executeQuery(sourceTables, sinkTable, query, false);
            try {
                Files.createDirectories(testFile.toPath().getParent());
                Files.write(
                        pathToTestFile,
                        result,
                        StandardOpenOption.CREATE,
                        StandardOpenOption.TRUNCATE_EXISTING,
                        StandardOpenOption.WRITE);
            } catch (IOException e) {
                throw new TableException(
                        "Cannot write the test result to the file '" + testFile + "'.", e);
            }
        }

        return result;
    }

    @Test
    public void testTwoWayInnerJoinWithEqualPk() throws Exception {
        TableInfo[] sourceTables = new TableInfo[2];
        sourceTables[0] =
                TableInfo.of("A", simple3Data)
                        .column("a1", Types.INT)
                        .column("a2", Types.LONG)
                        .column("a3", Types.STRING);
        sourceTables[1] =
                TableInfo.of("B", simple5Data)
                        .column("b1", Types.INT)
                        .column("b2", Types.LONG)
                        .column("b3", Types.INT)
                        .column("b4", Types.STRING)
                        .column("b5", Types.LONG);

        TableInfo sinkTable =
                TableInfo.of("ABSink", null).column("a1", Types.INT).column("b1", Types.INT);

        String query1 = "SELECT SUM(a2) AS a2, a1 FROM A group by a1";
        String query2 = "SELECT SUM(b2) AS b2, b1 FROM B group by b1";
        String query =
                "INSERT INTO ABSink SELECT a1, b1 FROM ("
                        + query1
                        + ") JOIN ("
                        + query2
                        + ") ON a1 = b1";

        List<String> expected =
                getExpectedResult(sourceTables, sinkTable, query, testName.getMethodName());
        List<String> actual = executeQuery(sourceTables, sinkTable, query, true);

        assertThat(actual).containsExactlyInAnyOrderElementsOf(expected);
    }

    @Test
    public void testThreeWayInnerJoinWithoutPk() throws Exception {
        List<Row> tableAData =
                Arrays.asList(
                        Row.of(1, 1L),
                        Row.of(1, 2L),
                        Row.of(1, 2L),
                        Row.of(1, 5L),
                        Row.of(2, 7L),
                        Row.of(1, 9L),
                        Row.of(1, 8L),
                        Row.of(3, 8L));
        List<Row> tableBData =
                Arrays.asList(Row.of(1, 1L), Row.of(2, 2L), Row.of(3, 2L), Row.of(1, 4L));
        List<Row> tableCData =
                Arrays.asList(Row.of(1, 1L), Row.of(2, 2L), Row.of(3, 2L), Row.of(2, 1L));

        TableInfo[] sourceTables = new TableInfo[3];
        sourceTables[0] =
                TableInfo.of("A", tableAData).column("a1", Types.INT).column("a2", Types.LONG);
        sourceTables[1] =
                TableInfo.of("B", tableBData).column("b1", Types.INT).column("b2", Types.LONG);
        sourceTables[2] =
                TableInfo.of("C", tableCData).column("c1", Types.INT).column("c2", Types.LONG);

        TableInfo sinkTable =
                TableInfo.of("ABCSink", null)
                        .column("a1", Types.INT)
                        .column("b1", Types.INT)
                        .column("c1", Types.INT);

        String query1 = "SELECT SUM(a2) AS a2, a1 FROM A group by a1";
        String query2 = "SELECT SUM(b2) AS b2, b1 FROM B group by b1";
        String query3 = "SELECT SUM(c2) AS c2, c1 FROM C group by c1";
        String query =
                "INSERT INTO ABCSink SELECT a1, b1, c1 FROM ("
                        + query1
                        + ") JOIN ("
                        + query2
                        + ") ON a1 = b1 JOIN ("
                        + query3
                        + ") ON c2 = b2";

        List<String> expected =
                getExpectedResult(sourceTables, sinkTable, query, testName.getMethodName());
        List<String> actual = executeQuery(sourceTables, sinkTable, query, true);

        assertThat(actual).containsExactlyInAnyOrderElementsOf(expected);
    }

    @Test
    public void testThreeWayInnerJoinMatchingBooleans() throws Exception {
        List<Row> tableAData =
                Arrays.asList(Row.of(1, true), Row.of(2, false), Row.of(3, true), Row.of(4, false));
        List<Row> tableBData = Arrays.asList(Row.of(5, true), Row.of(6, false), Row.of(7, false));
        List<Row> tableCData = Arrays.asList(Row.of(8, false), Row.of(9, false), Row.of(10, true));

        TableInfo[] sourceTables = new TableInfo[3];
        sourceTables[0] =
                TableInfo.of("A", tableAData)
                        .column("a1", Types.INT)
                        .column("a2", Types.BOOLEAN)
                        .primaryKey("a1");
        sourceTables[1] =
                TableInfo.of("B", tableBData)
                        .column("b1", Types.INT)
                        .column("b2", Types.BOOLEAN)
                        .primaryKey("b1");
        sourceTables[2] =
                TableInfo.of("C", tableCData)
                        .column("c1", Types.INT)
                        .column("c2", Types.BOOLEAN)
                        .primaryKey("c1");

        TableInfo sinkTable =
                TableInfo.of("ABCSink", null)
                        .column("a1", Types.INT)
                        .column("b1", Types.INT)
                        .column("c1", Types.INT);

        String query =
                "INSERT INTO ABCSink "
                        + "SELECT a1, b1, c1 "
                        + "FROM A "
                        + "JOIN B ON a2 = b2 "
                        + "JOIN C ON b2 = c2";

        List<String> expected =
                getExpectedResult(sourceTables, sinkTable, query, testName.getMethodName());
        List<String> actual = executeQuery(sourceTables, sinkTable, query, true);

        assertThat(actual).containsExactlyInAnyOrderElementsOf(expected);
    }

    @Test
    public void testThreeWayInnerJoinMatchingBooleansNoCommonJoinKey() throws Exception {
        List<Row> tableAData =
                Arrays.asList(Row.of(1, true), Row.of(2, false), Row.of(3, true), Row.of(4, false));

        List<Row> tableBData = Arrays.asList(Row.of(5, true), Row.of(6, false), Row.of(7, false));
        List<Row> tableCData = Arrays.asList(Row.of(8, false), Row.of(9, false), Row.of(10, true));

        TableInfo[] sourceTables = new TableInfo[3];
        sourceTables[0] =
                TableInfo.of("A", tableAData)
                        .column("a1", Types.INT)
                        .column("a2", Types.BOOLEAN)
                        .primaryKey("a1");
        sourceTables[1] =
                TableInfo.of("B", tableBData)
                        .column("b1", Types.INT)
                        .column("b2", Types.BOOLEAN)
                        .primaryKey("b1");
        sourceTables[2] =
                TableInfo.of("C", tableCData)
                        .column("c1", Types.INT)
                        .column("c2", Types.BOOLEAN)
                        .primaryKey("c1");

        TableInfo sinkTable =
                TableInfo.of("ABCSink", null)
                        .column("a1", Types.INT)
                        .column("b1", Types.INT)
                        .column("c1", Types.INT);

        String query =
                "INSERT INTO ABCSink "
                        + "SELECT a1, b1, c1 "
                        + "FROM A "
                        + "JOIN B ON a2 = true "
                        + "JOIN C ON b2 = false";

        List<String> expected =
                getExpectedResult(sourceTables, sinkTable, query, testName.getMethodName());
        List<String> actual = executeQuery(sourceTables, sinkTable, query, true);

        assertThat(actual).containsExactlyInAnyOrderElementsOf(expected);
    }

    @Test
    public void testThreeWayInnerJoinMatchingBooleansV2() throws Exception {
        List<Row> tableAData =
                Arrays.asList(Row.of(1, true), Row.of(2, false), Row.of(3, true), Row.of(4, false));

        List<Row> tableBData = Arrays.asList(Row.of(5, true), Row.of(6, false), Row.of(7, false));
        List<Row> tableCData = Arrays.asList(Row.of(8, false), Row.of(9, false), Row.of(10, true));

        TableInfo[] sourceTables = new TableInfo[3];
        sourceTables[0] =
                TableInfo.of("A", tableAData)
                        .column("a1", Types.INT)
                        .column("a2", Types.BOOLEAN)
                        .primaryKey("a1");
        sourceTables[1] =
                TableInfo.of("B", tableBData)
                        .column("b1", Types.INT)
                        .column("b2", Types.BOOLEAN)
                        .primaryKey("b1");
        sourceTables[2] =
                TableInfo.of("C", tableCData)
                        .column("c1", Types.INT)
                        .column("c2", Types.BOOLEAN)
                        .primaryKey("c1");

        TableInfo sinkTable =
                TableInfo.of("ABCSink", null)
                        .column("a1", Types.INT)
                        .column("b1", Types.INT)
                        .column("c1", Types.INT);

        String query =
                "INSERT INTO ABCSink "
                        + "SELECT a1, b1, c1 "
                        + "FROM A "
                        + "JOIN B ON a2 = true "
                        + "JOIN C ON b2 = true AND c2 = true";

        List<String> expected =
                getExpectedResult(sourceTables, sinkTable, query, testName.getMethodName());
        List<String> actual = executeQuery(sourceTables, sinkTable, query, true);

        assertThat(actual).containsExactlyInAnyOrderElementsOf(expected);
    }

    @Test
    public void testThreeWayInnerJoinNestedType() throws Exception {
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

        TableInfo[] sourceTables = new TableInfo[3];
        sourceTables[0] =
                TableInfo.of("A", tableAData)
                        .column("id", Types.INT)
                        .column("user_info", userType)
                        .primaryKey("id");
        sourceTables[1] =
                TableInfo.of("B", tableBData)
                        .column("id", Types.INT)
                        .column("user_info", userType)
                        .primaryKey("id");
        sourceTables[2] =
                TableInfo.of("C", tableCData)
                        .column("id", Types.INT)
                        .column("user_info", userType)
                        .primaryKey("id");

        TableInfo sinkTable =
                TableInfo.of("ABCSink", null)
                        .column("a1", Types.INT)
                        .column("b1", Types.INT)
                        .column("c1", Types.INT);

        String query =
                "INSERT INTO ABCSink "
                        + "SELECT A.id, B.id, C.id "
                        + "FROM A "
                        + "JOIN B ON A.user_info = B.user_info "
                        + "JOIN C ON B.user_info = C.user_info";

        List<String> expected =
                getExpectedResult(sourceTables, sinkTable, query, testName.getMethodName());
        List<String> actual = executeQuery(sourceTables, sinkTable, query, true);

        assertThat(actual).containsExactlyInAnyOrderElementsOf(expected);
    }

    @Test
    public void testThreeWayInnerJoin() throws Exception {
        TableInfo[] sourceTables = new TableInfo[3];
        sourceTables[0] = TableInfo.USERS;
        sourceTables[1] = TableInfo.ORDERS;
        sourceTables[2] = TableInfo.PAYMENTS;

        String query =
                "INSERT INTO ResultSink "
                        + "SELECT u.user_id, u.name, o.order_id, p.payment_id, p.price "
                        + "FROM Users u "
                        + "JOIN Orders o ON u.user_id = o.user_id "
                        + "JOIN Payments p ON o.user_id = p.user_id";

        List<String> expected =
                getExpectedResult(
                        sourceTables, TableInfo.RESULT_SINK, query, testName.getMethodName());
        List<String> actual = executeQuery(sourceTables, TableInfo.RESULT_SINK, query, true);

        assertThat(actual).containsExactlyInAnyOrderElementsOf(expected);
    }

    @Test
    public void testThreeWayLeftInnerJoin() throws Exception {
        TableInfo[] sourceTables = new TableInfo[3];
        sourceTables[0] = TableInfo.USERS;
        sourceTables[1] = TableInfo.ORDERS;
        sourceTables[2] = TableInfo.PAYMENTS;

        String query =
                "INSERT INTO ResultSink "
                        + "SELECT u.user_id, u.name, o.order_id, p.payment_id, p.price "
                        + "FROM Users u "
                        + "LEFT JOIN Orders o ON u.user_id = o.user_id "
                        + "JOIN Payments p ON o.user_id = p.user_id";

        List<String> expected =
                getExpectedResult(
                        sourceTables, TableInfo.RESULT_SINK, query, testName.getMethodName());
        List<String> actual = executeQuery(sourceTables, TableInfo.RESULT_SINK, query, true);

        assertThat(actual).containsExactlyInAnyOrderElementsOf(expected);
    }

    @Test
    public void testThreeWayLeftLeftJoin() throws Exception {
        TableInfo[] sourceTables = new TableInfo[3];
        sourceTables[0] = TableInfo.USERS;
        sourceTables[1] = TableInfo.ORDERS;
        sourceTables[2] = TableInfo.PAYMENTS;

        String query =
                "INSERT INTO ResultSink "
                        + "SELECT u.user_id, u.name, o.order_id, p.payment_id, p.price "
                        + "FROM Users u "
                        + "LEFT JOIN Orders o ON u.user_id = o.user_id "
                        + "LEFT JOIN Payments p ON o.user_id = p.user_id";

        List<String> expected =
                getExpectedResult(
                        sourceTables, TableInfo.RESULT_SINK, query, testName.getMethodName());
        List<String> actual = executeQuery(sourceTables, TableInfo.RESULT_SINK, query, true);

        assertThat(actual).containsExactlyInAnyOrderElementsOf(expected);
    }

    @Test
    public void testThreeWayInnerLeftJoin() throws Exception {
        TableInfo[] sourceTables = new TableInfo[3];
        sourceTables[0] = TableInfo.USERS;
        sourceTables[1] = TableInfo.ORDERS;
        sourceTables[2] = TableInfo.PAYMENTS;

        String query =
                "INSERT INTO ResultSink "
                        + "SELECT u.user_id, u.name, o.order_id, p.payment_id, p.price "
                        + "FROM Users u "
                        + "JOIN Orders o ON u.user_id = o.user_id "
                        + "LEFT JOIN Payments p ON o.user_id = p.user_id";

        List<String> expected =
                getExpectedResult(
                        sourceTables, TableInfo.RESULT_SINK, query, testName.getMethodName());
        List<String> actual = executeQuery(sourceTables, TableInfo.RESULT_SINK, query, true);

        assertThat(actual).containsExactlyInAnyOrderElementsOf(expected);
    }

    @Test
    public void testThreeWayInnerJoinNoCommonJoinKey() throws Exception {
        TableInfo[] sourceTables = new TableInfo[3];
        sourceTables[0] = TableInfo.USERS;
        sourceTables[1] = TableInfo.ORDERS;
        sourceTables[2] = TableInfo.PAYMENTS;

        String query =
                "INSERT INTO ResultSink "
                        + "SELECT u.user_id, u.name, o.order_id, p.payment_id, p.price "
                        + "FROM Users u "
                        + "JOIN Orders o ON u.user_id = o.user_id "
                        + "JOIN Payments p ON u.cash = p.price";

        List<String> expected =
                getExpectedResult(
                        sourceTables, TableInfo.RESULT_SINK, query, testName.getMethodName());
        List<String> actual = executeQuery(sourceTables, TableInfo.RESULT_SINK, query, true);

        assertThat(actual).containsExactlyInAnyOrderElementsOf(expected);
    }

    @Test
    public void testThreeWayLeftInnerJoinNoCommonJoinKey() throws Exception {
        TableInfo[] sourceTables = new TableInfo[3];
        sourceTables[0] = TableInfo.USERS;
        sourceTables[1] = TableInfo.ORDERS;
        sourceTables[2] = TableInfo.PAYMENTS;

        String query =
                "INSERT INTO ResultSink "
                        + "SELECT u.user_id, u.name, o.order_id, p.payment_id, p.price "
                        + "FROM Users u "
                        + "LEFT JOIN Orders o ON u.user_id = o.user_id "
                        + "JOIN Payments p ON u.cash = p.price";

        List<String> expected =
                getExpectedResult(
                        sourceTables, TableInfo.RESULT_SINK, query, testName.getMethodName());
        List<String> actual = executeQuery(sourceTables, TableInfo.RESULT_SINK, query, true);

        assertThat(actual).containsExactlyInAnyOrderElementsOf(expected);
    }

    @Test
    public void testThreeWayInnerJoinWithIsNotDistinct() throws Exception {
        TableInfo[] sourceTables = new TableInfo[3];
        sourceTables[0] = TableInfo.USERS;
        sourceTables[1] = TableInfo.ORDERS;
        sourceTables[2] = TableInfo.PAYMENTS;

        String query =
                "INSERT INTO ResultSink "
                        + "SELECT u.user_id, u.name, o.order_id, p.payment_id, p.price "
                        + "FROM Users u "
                        + "JOIN Orders o ON u.user_id IS NOT DISTINCT FROM o.user_id "
                        + "JOIN Payments p ON o.user_id IS NOT DISTINCT FROM p.user_id";

        List<String> expected =
                getExpectedResult(
                        sourceTables, TableInfo.RESULT_SINK, query, testName.getMethodName());
        List<String> actual = executeQuery(sourceTables, TableInfo.RESULT_SINK, query, true);

        assertThat(actual).containsExactlyInAnyOrderElementsOf(expected);
    }

    @Test
    public void testThreeWayLeftLeftJoinNoCommonJoinKey() throws Exception {
        TableInfo[] sourceTables = new TableInfo[3];
        sourceTables[0] = TableInfo.USERS;
        sourceTables[1] = TableInfo.ORDERS;
        sourceTables[2] = TableInfo.PAYMENTS;

        String query =
                "INSERT INTO ResultSink "
                        + "SELECT u.user_id, u.name, o.order_id, p.payment_id, p.price "
                        + "FROM Users u "
                        + "LEFT JOIN Orders o ON u.user_id = o.user_id "
                        + "LEFT JOIN Payments p ON u.cash = p.price";

        List<String> expected =
                getExpectedResult(
                        sourceTables, TableInfo.RESULT_SINK, query, testName.getMethodName());
        List<String> actual = executeQuery(sourceTables, TableInfo.RESULT_SINK, query, true);

        assertThat(actual).containsExactlyInAnyOrderElementsOf(expected);
    }

    @Test
    public void testThreeWayInnerLeftJoinNoCommonJoinKey() throws Exception {
        TableInfo[] sourceTables = new TableInfo[3];
        sourceTables[0] = TableInfo.USERS;
        sourceTables[1] = TableInfo.ORDERS;
        sourceTables[2] = TableInfo.PAYMENTS;

        String query =
                "INSERT INTO ResultSink "
                        + "SELECT u.user_id, u.name, o.order_id, p.payment_id, p.price "
                        + "FROM Users u "
                        + "JOIN Orders o ON u.user_id = o.user_id "
                        + "LEFT JOIN Payments p ON u.cash = p.price";

        List<String> expected =
                getExpectedResult(
                        sourceTables, TableInfo.RESULT_SINK, query, testName.getMethodName());
        List<String> actual = executeQuery(sourceTables, TableInfo.RESULT_SINK, query, true);

        assertThat(actual).containsExactlyInAnyOrderElementsOf(expected);
    }

    @Test
    public void testThreeWayInnerJoinWithWhere() throws Exception {
        TableInfo[] sourceTables = new TableInfo[3];
        sourceTables[0] = TableInfo.USERS;
        sourceTables[1] = TableInfo.ORDERS;
        sourceTables[2] = TableInfo.PAYMENTS;

        String query =
                "INSERT INTO ResultSink "
                        + "SELECT u.user_id, u.name, o.order_id, p.payment_id, p.price "
                        + "FROM Users u, Orders o, Payments p "
                        + "WHERE u.user_id = o.user_id "
                        + "AND o.user_id = p.user_id";

        List<String> expected =
                getExpectedResult(
                        sourceTables, TableInfo.RESULT_SINK, query, testName.getMethodName());
        List<String> actual = executeQuery(sourceTables, TableInfo.RESULT_SINK, query, true);

        assertThat(actual).containsExactlyInAnyOrderElementsOf(expected);
    }

    @Test
    public void testThreeWayInnerJoinWithOr() throws Exception {
        TableInfo[] sourceTables = new TableInfo[3];
        sourceTables[0] = TableInfo.USERS;
        sourceTables[1] = TableInfo.ORDERS;
        sourceTables[2] = TableInfo.PAYMENTS;

        String query =
                "INSERT INTO ResultSink "
                        + "SELECT u.user_id, u.name, o.order_id, p.payment_id, p.price "
                        + "FROM Users u "
                        + "JOIN Orders o ON u.user_id = o.user_id "
                        + "JOIN Payments p ON o.user_id = p.user_id OR p.price = u.cash";

        List<String> expected =
                getExpectedResult(
                        sourceTables, TableInfo.RESULT_SINK, query, testName.getMethodName());
        List<String> actual = executeQuery(sourceTables, TableInfo.RESULT_SINK, query, true);

        assertThat(actual).containsExactlyInAnyOrderElementsOf(expected);
    }

    @Test
    public void testThreeWayLeftInnerJoinWithOr() throws Exception {
        TableInfo[] sourceTables = new TableInfo[3];
        sourceTables[0] = TableInfo.USERS;
        sourceTables[1] = TableInfo.ORDERS;
        sourceTables[2] = TableInfo.PAYMENTS;

        String query =
                "INSERT INTO ResultSink "
                        + "SELECT u.user_id, u.name, o.order_id, p.payment_id, p.price "
                        + "FROM Users u "
                        + "LEFT JOIN Orders o ON u.user_id = o.user_id "
                        + "JOIN Payments p ON o.user_id = p.user_id OR p.price = u.cash";

        List<String> expected =
                getExpectedResult(
                        sourceTables, TableInfo.RESULT_SINK, query, testName.getMethodName());
        List<String> actual = executeQuery(sourceTables, TableInfo.RESULT_SINK, query, true);

        assertThat(actual).containsExactlyInAnyOrderElementsOf(expected);
    }

    @Test
    public void testThreeWayLeftLeftJoinWithOr() throws Exception {
        TableInfo[] sourceTables = new TableInfo[3];
        sourceTables[0] = TableInfo.USERS;
        sourceTables[1] = TableInfo.ORDERS;
        sourceTables[2] = TableInfo.PAYMENTS;

        String query =
                "INSERT INTO ResultSink "
                        + "SELECT u.user_id, u.name, o.order_id, p.payment_id, p.price "
                        + "FROM Users u "
                        + "LEFT JOIN Orders o ON u.user_id = o.user_id "
                        + "LEFT JOIN Payments p ON o.user_id = p.user_id OR p.price = u.cash";

        List<String> expected =
                getExpectedResult(
                        sourceTables, TableInfo.RESULT_SINK, query, testName.getMethodName());
        List<String> actual = executeQuery(sourceTables, TableInfo.RESULT_SINK, query, true);

        assertThat(actual).containsExactlyInAnyOrderElementsOf(expected);
    }

    @Test
    public void testThreeWayInnerLeftJoinWithOr() throws Exception {
        TableInfo[] sourceTables = new TableInfo[3];
        sourceTables[0] = TableInfo.USERS;
        sourceTables[1] = TableInfo.ORDERS;
        sourceTables[2] = TableInfo.PAYMENTS;

        String query =
                "INSERT INTO ResultSink "
                        + "SELECT u.user_id, u.name, o.order_id, p.payment_id, p.price "
                        + "FROM Users u "
                        + "JOIN Orders o ON u.user_id = o.user_id "
                        + "LEFT JOIN Payments p ON o.user_id = p.user_id OR p.price = u.cash";

        List<String> expected =
                getExpectedResult(
                        sourceTables, TableInfo.RESULT_SINK, query, testName.getMethodName());
        List<String> actual = executeQuery(sourceTables, TableInfo.RESULT_SINK, query, true);

        assertThat(actual).containsExactlyInAnyOrderElementsOf(expected);
    }

    @Test
    public void testThreeWayInnerJoinComplexCondition() throws Exception {
        TableInfo[] sourceTables = new TableInfo[3];
        sourceTables[0] = TableInfo.USERS;
        sourceTables[1] = TableInfo.ORDERS;
        sourceTables[2] = TableInfo.PAYMENTS;

        String query =
                "INSERT INTO ResultSink "
                        + "SELECT u.user_id, u.name, o.order_id, p.payment_id, p.price "
                        + "FROM Users u "
                        + "JOIN Orders o ON u.user_id = o.user_id AND u.cash >= 100"
                        + "JOIN Payments p ON o.user_id = p.user_id AND p.price = u.cash AND p.price < 200";

        List<String> expected =
                getExpectedResult(
                        sourceTables, TableInfo.RESULT_SINK, query, testName.getMethodName());
        List<String> actual = executeQuery(sourceTables, TableInfo.RESULT_SINK, query, true);

        assertThat(actual).containsExactlyInAnyOrderElementsOf(expected);
    }

    @Test
    public void testThreeWayLeftInnerJoinComplexCondition() throws Exception {
        TableInfo[] sourceTables = new TableInfo[3];
        sourceTables[0] = TableInfo.USERS;
        sourceTables[1] = TableInfo.ORDERS;
        sourceTables[2] = TableInfo.PAYMENTS;

        String query =
                "INSERT INTO ResultSink "
                        + "SELECT u.user_id, u.name, o.order_id, p.payment_id, p.price "
                        + "FROM Users u "
                        + "LEFT JOIN Orders o ON u.user_id = o.user_id AND u.cash >= 100"
                        + "JOIN Payments p ON o.user_id = p.user_id AND p.price = u.cash AND p.price < 200";

        List<String> expected =
                getExpectedResult(
                        sourceTables, TableInfo.RESULT_SINK, query, testName.getMethodName());
        List<String> actual = executeQuery(sourceTables, TableInfo.RESULT_SINK, query, true);

        assertThat(actual).containsExactlyInAnyOrderElementsOf(expected);
    }

    @Test
    public void testThreeWayLeftLeftJoinComplexCondition() throws Exception {
        TableInfo[] sourceTables = new TableInfo[3];
        sourceTables[0] = TableInfo.USERS;
        sourceTables[1] = TableInfo.ORDERS;
        sourceTables[2] = TableInfo.PAYMENTS;

        String query =
                "INSERT INTO ResultSink "
                        + "SELECT u.user_id, u.name, o.order_id, p.payment_id, p.price "
                        + "FROM Users u "
                        + "LEFT JOIN Orders o ON u.user_id = o.user_id AND u.cash >= 100"
                        + "LEFT JOIN Payments p ON o.user_id = p.user_id AND p.price = u.cash AND p.price < 200";

        List<String> expected =
                getExpectedResult(
                        sourceTables, TableInfo.RESULT_SINK, query, testName.getMethodName());
        List<String> actual = executeQuery(sourceTables, TableInfo.RESULT_SINK, query, true);

        assertThat(actual).containsExactlyInAnyOrderElementsOf(expected);
    }

    @Test
    public void testThreeWayInnerLeftJoinComplexCondition() throws Exception {
        TableInfo[] sourceTables = new TableInfo[3];
        sourceTables[0] = TableInfo.USERS;
        sourceTables[1] = TableInfo.ORDERS;
        sourceTables[2] = TableInfo.PAYMENTS;

        String query =
                "INSERT INTO ResultSink "
                        + "SELECT u.user_id, u.name, o.order_id, p.payment_id, p.price "
                        + "FROM Users u "
                        + "JOIN Orders o ON u.user_id = o.user_id AND u.cash >= 100"
                        + "LEFT JOIN Payments p ON o.user_id = p.user_id AND p.price = u.cash AND p.price < 200";

        List<String> expected =
                getExpectedResult(
                        sourceTables, TableInfo.RESULT_SINK, query, testName.getMethodName());
        List<String> actual = executeQuery(sourceTables, TableInfo.RESULT_SINK, query, true);

        assertThat(actual).containsExactlyInAnyOrderElementsOf(expected);
    }

    @Test
    public void testThreeWayInnerJoinComplexConditionV2() throws Exception {
        TableInfo[] sourceTables = new TableInfo[3];
        sourceTables[0] = TableInfo.USERS;
        sourceTables[1] = TableInfo.ORDERS;
        sourceTables[2] = TableInfo.PAYMENTS;

        String query =
                "INSERT INTO ResultSink "
                        + "SELECT u.user_id, u.name, o.order_id, p.payment_id, p.price "
                        + "FROM Users u "
                        + "JOIN Orders o ON u.user_id = o.user_id AND u.cash >= 100 "
                        + "JOIN Payments p ON p.price = u.cash AND (o.user_id = p.user_id OR u.name = 'Lucas') ";

        List<String> expected =
                getExpectedResult(
                        sourceTables, TableInfo.RESULT_SINK, query, testName.getMethodName());
        List<String> actual = executeQuery(sourceTables, TableInfo.RESULT_SINK, query, true);

        assertThat(actual).containsExactlyInAnyOrderElementsOf(expected);
    }

    @Test
    public void testThreeWayLeftInnerJoinComplexConditionV2() throws Exception {
        TableInfo[] sourceTables = new TableInfo[3];
        sourceTables[0] = TableInfo.USERS;
        sourceTables[1] = TableInfo.ORDERS;
        sourceTables[2] = TableInfo.PAYMENTS;

        String query =
                "INSERT INTO ResultSink "
                        + "SELECT u.user_id, u.name, o.order_id, p.payment_id, p.price "
                        + "FROM Users u "
                        + "LEFT JOIN Orders o ON u.user_id = o.user_id AND u.cash >= 100 "
                        + "JOIN Payments p ON p.price = u.cash AND (o.user_id = p.user_id OR u.name = 'Lucas') ";

        List<String> expected =
                getExpectedResult(
                        sourceTables, TableInfo.RESULT_SINK, query, testName.getMethodName());
        List<String> actual = executeQuery(sourceTables, TableInfo.RESULT_SINK, query, true);

        assertThat(actual).containsExactlyInAnyOrderElementsOf(expected);
    }

    @Test
    public void testThreeWayLeftLeftJoinComplexConditionV2() throws Exception {
        TableInfo[] sourceTables = new TableInfo[3];
        sourceTables[0] = TableInfo.USERS;
        sourceTables[1] = TableInfo.ORDERS;
        sourceTables[2] = TableInfo.PAYMENTS;

        String query =
                "INSERT INTO ResultSink "
                        + "SELECT u.user_id, u.name, o.order_id, p.payment_id, p.price "
                        + "FROM Users u "
                        + "LEFT JOIN Orders o ON u.user_id = o.user_id AND u.cash >= 100 "
                        + "LEFT JOIN Payments p ON p.price = u.cash AND (o.user_id = p.user_id OR u.name = 'Lucas') ";

        List<String> expected =
                getExpectedResult(
                        sourceTables, TableInfo.RESULT_SINK, query, testName.getMethodName());
        List<String> actual = executeQuery(sourceTables, TableInfo.RESULT_SINK, query, true);

        assertThat(actual).containsExactlyInAnyOrderElementsOf(expected);
    }

    @Test
    public void testThreeWayInnerLeftJoinComplexConditionV2() throws Exception {
        TableInfo[] sourceTables = new TableInfo[3];
        sourceTables[0] = TableInfo.USERS;
        sourceTables[1] = TableInfo.ORDERS;
        sourceTables[2] = TableInfo.PAYMENTS;

        String query =
                "INSERT INTO ResultSink "
                        + "SELECT u.user_id, u.name, o.order_id, p.payment_id, p.price "
                        + "FROM Users u "
                        + "JOIN Orders o ON u.user_id = o.user_id AND u.cash >= 100 "
                        + "LEFT JOIN Payments p ON p.price = u.cash AND (o.user_id = p.user_id OR u.name = 'Lucas') ";

        List<String> expected =
                getExpectedResult(
                        sourceTables, TableInfo.RESULT_SINK, query, testName.getMethodName());
        List<String> actual = executeQuery(sourceTables, TableInfo.RESULT_SINK, query, true);

        assertThat(actual).containsExactlyInAnyOrderElementsOf(expected);
    }

    @Test
    public void testThreeWayInnerJoinConstantCondition() throws Exception {
        TableInfo[] sourceTables = new TableInfo[3];
        sourceTables[0] = TableInfo.USERS;
        sourceTables[1] = TableInfo.ORDERS;
        sourceTables[2] = TableInfo.PAYMENTS;

        String query =
                "INSERT INTO ResultSink "
                        + "SELECT u.user_id, u.name, o.order_id, p.payment_id, p.price "
                        + "FROM Users u "
                        + "JOIN Orders o ON TRUE "
                        + "JOIN Payments p ON o.user_id = p.user_id AND p.price = u.cash AND p.price < 200";

        List<String> expected =
                getExpectedResult(
                        sourceTables, TableInfo.RESULT_SINK, query, testName.getMethodName());
        List<String> actual = executeQuery(sourceTables, TableInfo.RESULT_SINK, query, true);

        assertThat(actual).containsExactlyInAnyOrderElementsOf(expected);
    }

    @Test
    public void testThreeWayLeftInnerJoinConstantCondition() throws Exception {
        TableInfo[] sourceTables = new TableInfo[3];
        sourceTables[0] = TableInfo.USERS;
        sourceTables[1] = TableInfo.ORDERS;
        sourceTables[2] = TableInfo.PAYMENTS;

        String query =
                "INSERT INTO ResultSink "
                        + "SELECT u.user_id, u.name, o.order_id, p.payment_id, p.price "
                        + "FROM Users u "
                        + "LEFT JOIN Orders o ON TRUE "
                        + "JOIN Payments p ON o.user_id = p.user_id AND p.price = u.cash AND p.price < 200";

        List<String> expected =
                getExpectedResult(
                        sourceTables, TableInfo.RESULT_SINK, query, testName.getMethodName());
        List<String> actual = executeQuery(sourceTables, TableInfo.RESULT_SINK, query, true);

        assertThat(actual).containsExactlyInAnyOrderElementsOf(expected);
    }

    @Test
    public void testThreeWayLeftLeftJoinConstantCondition() throws Exception {
        TableInfo[] sourceTables = new TableInfo[3];
        sourceTables[0] = TableInfo.USERS;
        sourceTables[1] = TableInfo.ORDERS;
        sourceTables[2] = TableInfo.PAYMENTS;

        String query =
                "INSERT INTO ResultSink "
                        + "SELECT u.user_id, u.name, o.order_id, p.payment_id, p.price "
                        + "FROM Users u "
                        + "LEFT JOIN Orders o ON TRUE "
                        + "LEFT JOIN Payments p ON o.user_id = p.user_id AND p.price = u.cash AND p.price < 200";

        List<String> expected =
                getExpectedResult(
                        sourceTables, TableInfo.RESULT_SINK, query, testName.getMethodName());
        List<String> actual = executeQuery(sourceTables, TableInfo.RESULT_SINK, query, true);

        assertThat(actual).containsExactlyInAnyOrderElementsOf(expected);
    }

    @Test
    public void testThreeWayInnerLeftJoinConstantCondition() throws Exception {
        TableInfo[] sourceTables = new TableInfo[3];
        sourceTables[0] = TableInfo.USERS;
        sourceTables[1] = TableInfo.ORDERS;
        sourceTables[2] = TableInfo.PAYMENTS;

        String query =
                "INSERT INTO ResultSink "
                        + "SELECT u.user_id, u.name, o.order_id, p.payment_id, p.price "
                        + "FROM Users u "
                        + "JOIN Orders o ON TRUE "
                        + "LEFT JOIN Payments p ON o.user_id = p.user_id AND p.price = u.cash AND p.price < 200";

        List<String> expected =
                getExpectedResult(
                        sourceTables, TableInfo.RESULT_SINK, query, testName.getMethodName());
        List<String> actual = executeQuery(sourceTables, TableInfo.RESULT_SINK, query, true);

        assertThat(actual).containsExactlyInAnyOrderElementsOf(expected);
    }

    public static class TableInfo {

        private final String name;
        private final List<Row> data;
        private final List<String> columnNames = new ArrayList<>();
        private final List<TypeInformation<?>> columnTypes = new ArrayList<>();
        private final List<String> primaryKeys = new ArrayList<>();

        public static final TableInfo USERS =
                TableInfo.of("Users", usersData)
                        .column("user_id", Types.STRING)
                        .column("name", Types.STRING)
                        .column("cash", Types.INT);

        public static final TableInfo ORDERS =
                TableInfo.of("Orders", ordersData)
                        .column("order_id", Types.STRING)
                        .column("user_id", Types.STRING)
                        .column("product", Types.STRING);

        public static final TableInfo PAYMENTS =
                TableInfo.of("Payments", paymentsData)
                        .column("payment_id", Types.STRING)
                        .column("price", Types.INT)
                        .column("user_id", Types.STRING);

        public static final TableInfo RESULT_SINK =
                TableInfo.of("ResultSink", null)
                        .column("user_id", Types.STRING)
                        .column("name", Types.STRING)
                        .column("order_id", Types.STRING)
                        .column("payment_id", Types.STRING)
                        .column("price", Types.INT);

        private TableInfo(String name, List<Row> data) {
            this.name = name;
            this.data = data;
        }

        public static TableInfo of(String name, List<Row> data) {
            return new TableInfo(name, data);
        }

        public TableInfo column(String name, TypeInformation<?> type) {
            this.columnNames.add(name);
            this.columnTypes.add(type);
            return this;
        }

        public TableInfo primaryKey(String name) {
            assertThat(this.columnNames).contains(name);
            this.primaryKeys.add(name);
            return this;
        }

        public String getName() {
            return name;
        }

        public List<Row> getData() {
            return data;
        }

        public String[] getColumnNames() {
            return columnNames.toArray(new String[0]);
        }

        public TypeInformation<?>[] getColumnTypes() {
            return columnTypes.toArray(new TypeInformation[0]);
        }

        public List<String> getPrimaryKeys() {
            return primaryKeys;
        }
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
