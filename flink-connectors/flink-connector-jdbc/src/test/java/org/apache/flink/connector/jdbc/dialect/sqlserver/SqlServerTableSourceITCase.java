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

package org.apache.flink.connector.jdbc.dialect.sqlserver;

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
import org.testcontainers.containers.MSSQLServerContainer;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.Iterator;
import java.util.List;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import static org.assertj.core.api.Assertions.assertThat;

/** The Table Source ITCase for {@link SqlServerDialect}. */
public class SqlServerTableSourceITCase extends AbstractTestBase {

    private static final MSSQLServerContainer container =
            new MSSQLServerContainer("mcr.microsoft.com/mssql/server:2019-GA-ubuntu-16.04")
                    .acceptLicense();
    private static String containerUrl;
    private static final String INPUT_TABLE = "sql_test_table";

    private static StreamExecutionEnvironment env;
    private static TableEnvironment tEnv;

    @BeforeClass
    public static void beforeAll() throws ClassNotFoundException, SQLException {
        container.start();
        containerUrl = container.getJdbcUrl();
        Class.forName(container.getDriverClassName());
        try (Connection conn =
                        DriverManager.getConnection(
                                containerUrl, container.getUsername(), container.getPassword());
                Statement statement = conn.createStatement()) {
            statement.executeUpdate(
                    "CREATE TABLE "
                            + INPUT_TABLE
                            + " ("
                            + "id INT NOT NULL,"
                            + "tiny_int TINYINT,"
                            + "small_int SMALLINT,"
                            + "big_int BIGINT,"
                            + "float_col REAL,"
                            + "double_col FLOAT ,"
                            + "decimal_col DECIMAL(10, 4) NOT NULL,"
                            + "bool BIT NOT NULL,"
                            + "date_col DATE NOT NULL,"
                            + "time_col TIME(5) NOT NULL,"
                            + "datetime_col DATETIME,"
                            + "datetime2_col DATETIME2,"
                            + "char_col CHAR NOT NULL,"
                            + "nchar_col NCHAR(3) NOT NULL,"
                            + "varchar2_col VARCHAR(30) NOT NULL,"
                            + "nvarchar2_col NVARCHAR(30) NOT NULL,"
                            + "text_col TEXT,"
                            + "ntext_col NTEXT,"
                            + "binary_col BINARY(10)"
                            + ")");
            statement.executeUpdate(
                    "INSERT INTO "
                            + INPUT_TABLE
                            + " VALUES ("
                            + "1, 2, 4, 10000000000, 1.12345, 2.12345678791, 100.1234, 0, "
                            + "'1997-01-01', '05:20:20.222','2020-01-01 15:35:00.123',"
                            + "'2020-01-01 15:35:00.1234567', 'a', 'abc', 'abcdef', 'xyz',"
                            + "'Hello World', 'World Hello', 1024)");
            statement.executeUpdate(
                    "INSERT INTO "
                            + INPUT_TABLE
                            + " VALUES ("
                            + "2, 2, 4, 10000000000, 1.12345, 2.12345678791, 101.1234, 1, "
                            + "'1997-01-02', '05:20:20.222','2020-01-01 15:36:01.123',"
                            + "'2020-01-01 15:36:01.1234567', 'a', 'abc', 'abcdef', 'xyz',"
                            + "'Hey Leonard', 'World Hello', 1024)");
        }
    }

    @AfterClass
    public static void afterAll() throws Exception {
        Class.forName(container.getDriverClassName());
        try (Connection conn =
                        DriverManager.getConnection(
                                containerUrl, container.getUsername(), container.getPassword());
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
        createFlinkTable();
        Iterator<Row> collected = tEnv.executeSql("SELECT * FROM " + INPUT_TABLE).collect();
        List<String> result =
                CollectionUtil.iteratorToList(collected).stream()
                        .map(Row::toString)
                        .sorted()
                        .collect(Collectors.toList());
        List<String> expected =
                Stream.of(
                                "+I[1, 2, 4, 10000000000, 1.12345, 2.12345678791, 100.1234, false, "
                                        + "1997-01-01, 05:20:20, 2020-01-01T15:35:00.123, "
                                        + "2020-01-01T15:35:00.123456700, a, abc, abcdef, xyz, "
                                        + "Hello World, World Hello, [0, 0, 0, 0, 0, 0, 0, 0, 4, 0]]",
                                "+I[2, 2, 4, 10000000000, 1.12345, 2.12345678791, 101.1234, true, "
                                        + "1997-01-02, 05:20:20, 2020-01-01T15:36:01.123, "
                                        + "2020-01-01T15:36:01.123456700, a, abc, abcdef, xyz, "
                                        + "Hey Leonard, World Hello, [0, 0, 0, 0, 0, 0, 0, 0, 4, 0]]")
                        .sorted()
                        .collect(Collectors.toList());
        assertThat(result).isEqualTo(expected);
    }

    @Test
    public void testProject() throws Exception {
        createFlinkTable();
        Iterator<Row> collected =
                tEnv.executeSql("SELECT id,datetime_col,decimal_col FROM " + INPUT_TABLE).collect();
        List<String> result =
                CollectionUtil.iteratorToList(collected).stream()
                        .map(Row::toString)
                        .sorted()
                        .collect(Collectors.toList());
        List<String> expected =
                Stream.of(
                                "+I[1, 2020-01-01T15:35:00.123, 100.1234]",
                                "+I[2, 2020-01-01T15:36:01.123, 101.1234]")
                        .sorted()
                        .collect(Collectors.toList());
        assertThat(result).isEqualTo(expected);
    }

    @Test
    public void testFilter() throws Exception {
        createFlinkTable();
        Iterator<Row> collected =
                tEnv.executeSql(
                                "SELECT id,datetime_col,decimal_col FROM "
                                        + INPUT_TABLE
                                        + " WHERE id = 1")
                        .collect();
        List<String> result =
                CollectionUtil.iteratorToList(collected).stream()
                        .map(Row::toString)
                        .sorted()
                        .collect(Collectors.toList());
        List<String> expected =
                Stream.of("+I[1, 2020-01-01T15:35:00.123, 100.1234]").collect(Collectors.toList());
        assertThat(result).isEqualTo(expected);
    }

    private void createFlinkTable() {
        tEnv.executeSql(
                "CREATE TABLE "
                        + INPUT_TABLE
                        + " ("
                        + "id INT NOT NULL,"
                        + "tiny_int TINYINT,"
                        + "small_int SMALLINT,"
                        + "big_int BIGINT,"
                        + "float_col FLOAT,"
                        + "double_col DOUBLE ,"
                        + "decimal_col DECIMAL(10, 4) NOT NULL,"
                        + "bool BOOLEAN NOT NULL,"
                        + "date_col DATE NOT NULL,"
                        + "time_col TIME(0) NOT NULL,"
                        + "datetime_col TIMESTAMP,"
                        + "datetime2_col TIMESTAMP WITHOUT TIME ZONE,"
                        + "char_col STRING NOT NULL,"
                        + "nchar_col STRING NOT NULL,"
                        + "varchar2_col STRING NOT NULL,"
                        + "nvarchar2_col STRING NOT NULL,"
                        + "text_col STRING,"
                        + "ntext_col STRING,"
                        + "binary_col BYTES"
                        + ") WITH ("
                        + "  'connector'='jdbc',"
                        + "  'url'='"
                        + containerUrl
                        + "',"
                        + "  'table-name'='"
                        + INPUT_TABLE
                        + "',"
                        + "  'username'='"
                        + container.getUsername()
                        + "',"
                        + "  'password'='"
                        + container.getPassword()
                        + "'"
                        + ")");
    }
}
