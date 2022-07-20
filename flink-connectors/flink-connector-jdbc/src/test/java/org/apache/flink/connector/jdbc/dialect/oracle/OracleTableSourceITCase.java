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

package org.apache.flink.connector.jdbc.dialect.oracle;

import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.TableEnvironment;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.apache.flink.test.util.AbstractTestBase;
import org.apache.flink.types.Row;
import org.apache.flink.util.CollectionUtil;

import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;
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

import static org.assertj.core.api.Assertions.assertThat;

/** The Table Source ITCase for {@link OracleDialect}. */
public class OracleTableSourceITCase extends AbstractTestBase {

    private static final OracleContainer container = new OracleContainer();
    private static String containerUrl;
    private static final String INPUT_TABLE = "oracle_test_table";

    private static StreamExecutionEnvironment env;
    private static TableEnvironment tEnv;

    @BeforeClass
    public static void beforeAll() throws ClassNotFoundException, SQLException {
        container.start();
        containerUrl = container.getJdbcUrl();
        Class.forName(container.getDriverClassName());
        try (Connection conn = DriverManager.getConnection(containerUrl);
                Statement statement = conn.createStatement()) {
            statement.executeUpdate(
                    "CREATE TABLE "
                            + INPUT_TABLE
                            + " ("
                            + "id INTEGER NOT NULL,"
                            + "float_col FLOAT,"
                            + "double_col DOUBLE PRECISION ,"
                            + "decimal_col NUMBER(10, 4) NOT NULL,"
                            + "binary_float_col BINARY_FLOAT NOT NULL,"
                            + "binary_double_col BINARY_DOUBLE NOT NULL,"
                            + "char_col CHAR NOT NULL,"
                            + "nchar_col NCHAR(3) NOT NULL,"
                            + "varchar2_col VARCHAR2(30) NOT NULL,"
                            + "date_col DATE NOT NULL,"
                            + "timestamp6_col TIMESTAMP(6),"
                            + "timestamp9_col TIMESTAMP(9),"
                            + "clob_col CLOB,"
                            + "blob_col BLOB"
                            + ")");
            statement.executeUpdate(
                    "INSERT INTO "
                            + INPUT_TABLE
                            + " VALUES ("
                            + "1, 1.12345, 2.12345678790, 100.1234, 1.175E-10, 1.79769E+40, 'a', 'abc', 'abcdef', "
                            + "TO_DATE('1997-01-01','yyyy-mm-dd'),TIMESTAMP '2020-01-01 15:35:00.123456',"
                            + " TIMESTAMP '2020-01-01 15:35:00.123456789', 'Hello World', hextoraw('453d7a34'))");
            statement.executeUpdate(
                    "INSERT INTO "
                            + INPUT_TABLE
                            + " VALUES ("
                            + "2, 1.12345, 2.12345678790, 101.1234, -1.175E-10, -1.79769E+40, 'a', 'abc', 'abcdef', "
                            + "TO_DATE('1997-01-02','yyyy-mm-dd'),  TIMESTAMP '2020-01-01 15:36:01.123456', "
                            + "TIMESTAMP '2020-01-01 15:36:01.123456789', 'Hey Leonard', hextoraw('453d7a34'))");
        }
    }

    @AfterClass
    public static void afterAll() throws Exception {
        Class.forName(container.getDriverClassName());
        try (Connection conn = DriverManager.getConnection(containerUrl);
                Statement statement = conn.createStatement()) {
            statement.executeUpdate("DROP TABLE " + INPUT_TABLE);
        }
        container.stop();
    }

    @Before
    public void before() throws Exception {
        env = StreamExecutionEnvironment.getExecutionEnvironment();
        tEnv = StreamTableEnvironment.create(env);
    }

    @Test
    public void testJdbcSource() throws Exception {
        tEnv.executeSql(
                "CREATE TABLE "
                        + INPUT_TABLE
                        + "("
                        + "id BIGINT,"
                        + "float_col DECIMAL(6, 5),"
                        + "double_col DECIMAL(11, 10),"
                        + "decimal_col DECIMAL(10, 4),"
                        + "binary_float_col FLOAT,"
                        + "binary_double_col DOUBLE,"
                        + "char_col CHAR(1),"
                        + "nchar_col VARCHAR(3),"
                        + "varchar2_col VARCHAR(30),"
                        + "date_col DATE,"
                        + "timestamp6_col TIMESTAMP(6),"
                        + "timestamp9_col TIMESTAMP(9),"
                        + "clob_col STRING,"
                        + "blob_col BYTES"
                        + ") WITH ("
                        + "  'connector'='jdbc',"
                        + "  'url'='"
                        + containerUrl
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
                                "+I[1, 1.12345, 2.1234567879, 100.1234, 1.175E-10, 1.79769E40, a, abc, abcdef, 1997-01-01, 2020-01-01T15:35:00.123456, 2020-01-01T15:35:00.123456789, Hello World, [69, 61, 122, 52]]",
                                "+I[2, 1.12345, 2.1234567879, 101.1234, -1.175E-10, -1.79769E40, a, abc, abcdef, 1997-01-02, 2020-01-01T15:36:01.123456, 2020-01-01T15:36:01.123456789, Hey Leonard, [69, 61, 122, 52]]")
                        .sorted()
                        .collect(Collectors.toList());
        assertThat(result).isEqualTo(expected);
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
                        + "binary_float_col FLOAT,"
                        + "binary_double_col DOUBLE,"
                        + "decimal_col DECIMAL(10, 4)"
                        + ") WITH ("
                        + "  'connector'='jdbc',"
                        + "  'url'='"
                        + containerUrl
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
        assertThat(result).isEqualTo(expected);
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
                        + "binary_float_col FLOAT,\n"
                        + "binary_double_col DOUBLE,\n"
                        + "decimal_col DECIMAL(10, 4)\n"
                        + ") WITH (\n"
                        + "  'connector'='jdbc',\n"
                        + "  'url'='"
                        + containerUrl
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
                "+I[1, 2020-01-01T15:35:00.123456, 2020-01-01T15:35:00.123456789, 1.175E-10, 1.79769E40, 100.1234]");
        expected.add(
                "+I[2, 2020-01-01T15:36:01.123456, 2020-01-01T15:36:01.123456789, -1.175E-10, -1.79769E40, 101.1234]");
        assertThat(result).hasSize(1);
        assertThat(expected)
                .as("The actual output is not a subset of the expected set.")
                .containsAll(result);
    }
}
