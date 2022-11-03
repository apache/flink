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

package org.apache.flink.connector.jdbc.dialect.trino;

import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.TableEnvironment;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.apache.flink.test.util.AbstractTestBase;
import org.apache.flink.types.Row;
import org.apache.flink.util.CollectionUtil;

import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.sql.Connection;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Set;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import static org.assertj.core.api.Assertions.assertThat;

/** The Table Source ITCase for {@link TrinoDialect}. */
public class TrinoTableSourceITCase extends AbstractTestBase {

    private static final TrinoContainer container = new TrinoContainer();
    private static final String INPUT_TABLE = "test_table";

    private static StreamExecutionEnvironment env;
    private static TableEnvironment tEnv;

    @BeforeAll
    public static void beforeAll() throws ClassNotFoundException, SQLException {
        container.start();
        try (Connection conn = container.getConnection();
                Statement statement = conn.createStatement()) {
            statement.executeUpdate(
                    "CREATE TABLE "
                            + INPUT_TABLE
                            + " ("
                            + "id INTEGER,"
                            + "double_col DOUBLE PRECISION ,"
                            + "decimal_col DECIMAL(10, 4),"
                            + "char_col CHAR,"
                            + "varchar2_col VARCHAR(30),"
                            + "date_col DATE,"
                            + "timestamp6_col TIMESTAMP(6),"
                            + "timestamp9_col TIMESTAMP(9)"
                            + ")");
            statement.executeUpdate(
                    "INSERT INTO "
                            + INPUT_TABLE
                            + " VALUES ("
                            + "1, "
                            + "2.12345678790, "
                            + "100.1234, "
                            + "'a', "
                            + "'abcdef', "
                            + "TO_DATE('1997-01-01','yyyy-mm-dd'), "
                            + "TIMESTAMP '2020-01-01 15:35:00.123456', "
                            + "TIMESTAMP '2020-01-01 15:35:00.123456789' "
                            + ")");
            statement.executeUpdate(
                    "INSERT INTO "
                            + INPUT_TABLE
                            + " VALUES ("
                            + "2, "
                            + "2.12345678790, "
                            + "101.1234, "
                            + "'a', "
                            + "'abcdef', "
                            + "TO_DATE('1997-01-02','yyyy-mm-dd'), "
                            + "TIMESTAMP '2020-01-01 15:36:01.123456', "
                            + "TIMESTAMP '2020-01-01 15:36:01.123456789' "
                            + ")");
        }
    }

    @AfterAll
    public static void afterAll() throws Exception {
        try (Connection conn = container.getConnection();
                Statement statement = conn.createStatement()) {
            statement.executeUpdate("DROP TABLE " + INPUT_TABLE);
        }
        container.stop();
    }

    @BeforeEach
    public void before() throws Exception {
        env = StreamExecutionEnvironment.getExecutionEnvironment();
        tEnv = StreamTableEnvironment.create(env);
    }

    @Test
    public void testJdbcSource() throws Exception {
        tEnv.executeSql(
                "CREATE TABLE "
                        + INPUT_TABLE
                        + " ("
                        + "id BIGINT,"
                        + "double_col DECIMAL(11, 10),"
                        + "decimal_col DECIMAL(10, 4),"
                        + "char_col CHAR(1),"
                        + "varchar2_col VARCHAR(30),"
                        + "date_col DATE,"
                        + "timestamp6_col TIMESTAMP(6),"
                        + "timestamp9_col TIMESTAMP(9)"
                        + ") WITH ("
                        + "  'connector'='jdbc',"
                        + "  'url'='"
                        + container.getJdbcUrl()
                        + "',"
                        + "  'username'='"
                        + container.getUsername()
                        + "',\n"
                        + "  'password'='"
                        + container.getPassword()
                        + "',\n"
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
                                "+I[1, "
                                        + "2.1234567879, "
                                        + "100.1234, "
                                        + "a, "
                                        + "abcdef, "
                                        + "1997-01-01, "
                                        + "2020-01-01T15:35:00.123456, "
                                        + "2020-01-01T15:35:00.123456789"
                                        + "]",
                                "+I[2, "
                                        + "2.1234567879, "
                                        + "101.1234, "
                                        + "a, "
                                        + "abcdef, "
                                        + "1997-01-02, "
                                        + "2020-01-01T15:36:01.123456, "
                                        + "2020-01-01T15:36:01.123456789"
                                        + "]")
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
                        + "binary_double_col DOUBLE,"
                        + "decimal_col DECIMAL(10, 4)"
                        + ") WITH ("
                        + "  'connector'='jdbc',"
                        + "  'url'='"
                        + container.getJdbcUrl()
                        + "',"
                        + " 'username'='"
                        + container.getUsername()
                        + "',\n"
                        + " 'password'='"
                        + container.getPassword()
                        + "',\n"
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
                        + " (\n"
                        + "id BIGINT,\n"
                        + "timestamp6_col TIMESTAMP(6),\n"
                        + "timestamp9_col TIMESTAMP(9),\n"
                        + "decimal_col DECIMAL(10, 4)\n"
                        + ") WITH (\n"
                        + "  'connector'='jdbc',\n"
                        + "  'url'='"
                        + container.getJdbcUrl()
                        + "',\n"
                        + " 'username'='"
                        + container.getUsername()
                        + "',\n"
                        + " 'password'='"
                        + container.getPassword()
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
        expected.add("+I[1, 2020-01-01T15:35:00.123456, 2020-01-01T15:35:00.123456789, 100.1234]");
        expected.add("+I[2, 2020-01-01T15:36:01.123456, 2020-01-01T15:36:01.123456789, 101.1234]");
        assertThat(result).hasSize(1);
        assertThat(expected)
                .as("The actual output is not a subset of the expected set.")
                .containsAll(result);
    }
}
