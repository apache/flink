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

package org.apache.flink.table.jdbc;


import org.junit.jupiter.api.Test;

import java.math.BigDecimal;
import java.net.URL;
import java.nio.charset.StandardCharsets;
import java.sql.Date;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.Statement;
import java.sql.Time;
import java.sql.Timestamp;
import java.util.Objects;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;

/** Tests for flink prepared statement. */
public class FlinkPreparedStatementTest extends FlinkJdbcDriverTestBase {

    @Test
    public void testExecuteQuery() throws Exception {
        try (FlinkConnection connection = new FlinkConnection(getDriverUri())) {
            try (Statement statement = connection.createStatement()) {
                final URL url = getClass().getClassLoader().getResource("test-data.csv");
                Objects.requireNonNull(url);

                String createTableDDL = String.format(
                        "CREATE TABLE test_table("
                                + "str1 string,"
                                + "smallint1 smallint,"
                                + "int1 int,"
                                + "bigint1 bigint,"
                                + "float1 float,"
                                + "double1 double,"
                                + "boolean1 boolean,"
                                + "tinyint1 tinyint,"
                                + "bytes1 bytes,"
                                + "date1 date,"
                                + "time1 time,"
                                + "timestamp1 timestamp,"
                                + "decimal1 decimal,"
                                + "`value` string"
                                + ") WITH ("
                                + "'connector'='filesystem',\n"
                                + "'format'='csv',\n"
                                + "'path'='%s',\n"
                                + "'csv.ignore-parse-errors' = 'true',\n"
                                + "'csv.allow-comments' = 'true'"
                                + ")",
                        url.getPath());
                statement.execute(createTableDDL);

                String query = "select `value` from test_table where "
                        + "str1 = ? "
                        + "and smallint1 = ? "
                        + "and int1 = ? "
                        + "and bigint1 = ? "
                        + "and float1 = ? "
                        + "and double1 = ? "
                        + "and boolean1 = ? "
                        + "and tinyint1 = ? "
                        + "and bytes1 = ? "
                        + "and date1 = ? "
                        + "and time1 = ? "
                        + "and timestamp1 = ? "
                        + "and decimal1 = ?";

                try (PreparedStatement preparedStatement = connection.prepareStatement(query)) {
                    // Test for setting various types of parameters
                    preparedStatement.setString(1, "abc");
                    preparedStatement.setShort(2, (short) 0);
                    preparedStatement.setInt(3, 0);
                    preparedStatement.setLong(4, 0);
                    preparedStatement.setFloat(5, 0.0f);
                    preparedStatement.setDouble(6, 0.0d);
                    preparedStatement.setBoolean(7, true);
                    preparedStatement.setByte(8, (byte) 0);
                    preparedStatement.setBytes(9, "abc".getBytes(StandardCharsets.UTF_8));
                    preparedStatement.setDate(10, Date.valueOf("2023-01-01"));
                    preparedStatement.setTime(11, Time.valueOf("00:00:00"));
                    preparedStatement.setTimestamp(12, Timestamp.valueOf("2023-01-01 00:00:00"));
                    preparedStatement.setBigDecimal(13, BigDecimal.valueOf(0));

                    try (ResultSet resultSet = preparedStatement.executeQuery()) {
                        resultSet.next();
                        assertEquals("value0", resultSet.getString(1));
                        assertFalse(resultSet.next());
                    }

                    preparedStatement.clearParameters();
                    // Test for setObject
                    preparedStatement.setObject(1, "def");
                    preparedStatement.setObject(2, (short) 1);
                    preparedStatement.setObject(3, 1);
                    preparedStatement.setObject(4, (long) 1);
                    preparedStatement.setObject(5, 1.0f);
                    preparedStatement.setObject(6, 1.0d);
                    preparedStatement.setObject(7, false);
                    preparedStatement.setObject(8, (byte) 1);
                    preparedStatement.setObject(9, "def".getBytes(StandardCharsets.UTF_8));
                    preparedStatement.setObject(10, Date.valueOf("2023-01-01"));
                    preparedStatement.setObject(11, Time.valueOf("00:00:01"));
                    preparedStatement.setObject(12, Timestamp.valueOf("2023-01-01 00:00:01"));
                    preparedStatement.setObject(13, BigDecimal.valueOf(1));

                    try (ResultSet resultSet = preparedStatement.executeQuery()) {
                        resultSet.next();
                        assertEquals("value1", resultSet.getString(1));
                        assertFalse(resultSet.next());
                    }
                }
            }
        }
    }
}
