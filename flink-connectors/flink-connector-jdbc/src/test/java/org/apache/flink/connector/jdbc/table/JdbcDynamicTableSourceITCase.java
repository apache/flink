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

package org.apache.flink.connector.jdbc.table;

import org.apache.flink.connector.jdbc.JdbcTestBase;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.EnvironmentSettings;
import org.apache.flink.table.api.TableEnvironment;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.apache.flink.table.planner.runtime.utils.StreamTestSink;
import org.apache.flink.test.util.AbstractTestBase;
import org.apache.flink.types.Row;
import org.apache.flink.util.CollectionUtil;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Set;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

/** ITCase for {@link JdbcDynamicTableSource}. */
public class JdbcDynamicTableSourceITCase extends AbstractTestBase {

    public static final String DRIVER_CLASS = "org.apache.derby.jdbc.EmbeddedDriver";
    public static final String DB_URL = "jdbc:derby:memory:test";
    public static final String INPUT_TABLE = "jdbDynamicTableSource";

    public static StreamExecutionEnvironment env;
    public static TableEnvironment tEnv;

    @Before
    public void before() throws ClassNotFoundException, SQLException {
        env = StreamExecutionEnvironment.getExecutionEnvironment();
        EnvironmentSettings envSettings =
                EnvironmentSettings.newInstance().useBlinkPlanner().inStreamingMode().build();
        tEnv = StreamTableEnvironment.create(env, envSettings);

        System.setProperty(
                "derby.stream.error.field", JdbcTestBase.class.getCanonicalName() + ".DEV_NULL");
        Class.forName(DRIVER_CLASS);

        try (Connection conn = DriverManager.getConnection(DB_URL + ";create=true");
                Statement statement = conn.createStatement()) {
            statement.executeUpdate(
                    "CREATE TABLE "
                            + INPUT_TABLE
                            + " ("
                            + "id BIGINT NOT NULL,"
                            + "timestamp6_col TIMESTAMP, "
                            + "timestamp9_col TIMESTAMP, "
                            + "time_col TIME, "
                            + "real_col FLOAT(23), "
                            + // A precision of 23 or less makes FLOAT equivalent to REAL.
                            "double_col FLOAT(24),"
                            + // A precision of 24 or greater makes FLOAT equivalent to DOUBLE
                            // PRECISION.
                            "decimal_col DECIMAL(10, 4))");
            statement.executeUpdate(
                    "INSERT INTO "
                            + INPUT_TABLE
                            + " VALUES ("
                            + "1, TIMESTAMP('2020-01-01 15:35:00.123456'), TIMESTAMP('2020-01-01 15:35:00.123456789'), "
                            + "TIME('15:35:00'), 1.175E-37, 1.79769E+308, 100.1234)");
            statement.executeUpdate(
                    "INSERT INTO "
                            + INPUT_TABLE
                            + " VALUES ("
                            + "2, TIMESTAMP('2020-01-01 15:36:01.123456'), TIMESTAMP('2020-01-01 15:36:01.123456789'), "
                            + "TIME('15:36:01'), -1.175E-37, -1.79769E+308, 101.1234)");
        }
    }

    @After
    public void clearOutputTable() throws Exception {
        Class.forName(DRIVER_CLASS);
        try (Connection conn = DriverManager.getConnection(DB_URL);
                Statement stat = conn.createStatement()) {
            stat.executeUpdate("DROP TABLE " + INPUT_TABLE);
        }
        StreamTestSink.clear();
    }

    @Test
    public void testJdbcSource() throws Exception {
        tEnv.executeSql(
                "CREATE TABLE "
                        + INPUT_TABLE
                        + "("
                        + "id BIGINT,"
                        + "timestamp6_col TIMESTAMP(6),"
                        + "timestamp9_col TIMESTAMP(9),"
                        + "time_col TIME,"
                        + "real_col FLOAT,"
                        + "double_col DOUBLE,"
                        + "decimal_col DECIMAL(10, 4)"
                        + ") WITH ("
                        + "  'connector'='jdbc',"
                        + "  'url'='"
                        + DB_URL
                        + "',"
                        + "  'table-name'='"
                        + INPUT_TABLE
                        + "'"
                        + ")");

        Iterator<Row> collected = tEnv.executeSql("SELECT * FROM " + INPUT_TABLE).collect();
        List<String> result =
                CollectionUtil.iteratorToList(collected).stream()
                        .map(Row::toString)
                        .sorted()
                        .collect(Collectors.toList());
        List<String> expected =
                Stream.of(
                                "+I[1, 2020-01-01T15:35:00.123456, 2020-01-01T15:35:00.123456789, 15:35, 1.175E-37, 1.79769E308, 100.1234]",
                                "+I[2, 2020-01-01T15:36:01.123456, 2020-01-01T15:36:01.123456789, 15:36:01, -1.175E-37, -1.79769E308, 101.1234]")
                        .sorted()
                        .collect(Collectors.toList());
        assertEquals(expected, result);
    }

    @Test
    public void testProject() throws Exception {
        tEnv.executeSql(
                "CREATE TABLE "
                        + INPUT_TABLE
                        + "("
                        + "id BIGINT,"
                        + "timestamp6_col TIMESTAMP(6),"
                        + "timestamp9_col TIMESTAMP(9),"
                        + "time_col TIME,"
                        + "real_col FLOAT,"
                        + "double_col DOUBLE,"
                        + "decimal_col DECIMAL(10, 4)"
                        + ") WITH ("
                        + "  'connector'='jdbc',"
                        + "  'url'='"
                        + DB_URL
                        + "',"
                        + "  'table-name'='"
                        + INPUT_TABLE
                        + "',"
                        + "  'scan.partition.column'='id',"
                        + "  'scan.partition.num'='2',"
                        + "  'scan.partition.lower-bound'='0',"
                        + "  'scan.partition.upper-bound'='100'"
                        + ")");

        Iterator<Row> collected =
                tEnv.executeSql("SELECT id,timestamp6_col,decimal_col FROM " + INPUT_TABLE)
                        .collect();
        List<String> result =
                CollectionUtil.iteratorToList(collected).stream()
                        .map(Row::toString)
                        .sorted()
                        .collect(Collectors.toList());
        List<String> expected =
                Stream.of(
                                "+I[1, 2020-01-01T15:35:00.123456, 100.1234]",
                                "+I[2, 2020-01-01T15:36:01.123456, 101.1234]")
                        .sorted()
                        .collect(Collectors.toList());
        assertEquals(expected, result);
    }

    @Test
    public void testLimit() throws Exception {
        tEnv.executeSql(
                "CREATE TABLE "
                        + INPUT_TABLE
                        + "(\n"
                        + "id BIGINT,\n"
                        + "timestamp6_col TIMESTAMP(6),\n"
                        + "timestamp9_col TIMESTAMP(9),\n"
                        + "time_col TIME,\n"
                        + "real_col FLOAT,\n"
                        + "double_col DOUBLE,\n"
                        + "decimal_col DECIMAL(10, 4)\n"
                        + ") WITH (\n"
                        + "  'connector'='jdbc',\n"
                        + "  'url'='"
                        + DB_URL
                        + "',\n"
                        + "  'table-name'='"
                        + INPUT_TABLE
                        + "',\n"
                        + "  'scan.partition.column'='id',\n"
                        + "  'scan.partition.num'='2',\n"
                        + "  'scan.partition.lower-bound'='1',\n"
                        + "  'scan.partition.upper-bound'='2'\n"
                        + ")");

        Iterator<Row> collected =
                tEnv.executeSql("SELECT * FROM " + INPUT_TABLE + " LIMIT 1").collect();
        List<String> result =
                CollectionUtil.iteratorToList(collected).stream()
                        .map(Row::toString)
                        .sorted()
                        .collect(Collectors.toList());

        Set<String> expected = new HashSet<>();
        expected.add(
                "+I[1, 2020-01-01T15:35:00.123456, 2020-01-01T15:35:00.123456789, 15:35, 1.175E-37, 1.79769E308, 100.1234]");
        expected.add(
                "+I[2, 2020-01-01T15:36:01.123456, 2020-01-01T15:36:01.123456789, 15:36:01, -1.175E-37, -1.79769E308, 101.1234]");
        assertEquals(1, result.size());
        assertTrue(
                "The actual output is not a subset of the expected set.",
                expected.containsAll(result));
    }
}
