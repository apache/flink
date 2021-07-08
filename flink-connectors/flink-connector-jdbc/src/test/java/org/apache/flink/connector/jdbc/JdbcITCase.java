/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.flink.connector.jdbc;

import org.apache.flink.api.common.restartstrategy.RestartStrategies;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.util.function.FunctionWithException;

import org.junit.Test;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.sql.Types;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import static org.apache.flink.connector.jdbc.JdbcConnectionOptions.JdbcConnectionOptionsBuilder;
import static org.apache.flink.connector.jdbc.JdbcTestFixture.DERBY_EBOOKSHOP_DB;
import static org.apache.flink.connector.jdbc.JdbcTestFixture.INPUT_TABLE;
import static org.apache.flink.connector.jdbc.JdbcTestFixture.INSERT_TEMPLATE;
import static org.apache.flink.connector.jdbc.JdbcTestFixture.TEST_DATA;
import static org.apache.flink.connector.jdbc.JdbcTestFixture.TestEntry;
import static org.junit.Assert.assertEquals;

/** Smoke tests for the {@link JdbcSink} and the underlying classes. */
public class JdbcITCase extends JdbcTestBase {

    public static final JdbcStatementBuilder<TestEntry> TEST_ENTRY_JDBC_STATEMENT_BUILDER =
            (ps, t) -> {
                ps.setInt(1, t.id);
                ps.setString(2, t.title);
                ps.setString(3, t.author);
                if (t.price == null) {
                    ps.setNull(4, Types.DOUBLE);
                } else {
                    ps.setDouble(4, t.price);
                }
                ps.setInt(5, t.qty);
            };

    @Test
    public void testInsert() throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setRestartStrategy(new RestartStrategies.NoRestartStrategyConfiguration());
        env.setParallelism(1);
        env.fromElements(TEST_DATA)
                .addSink(
                        JdbcSink.sink(
                                String.format(INSERT_TEMPLATE, INPUT_TABLE),
                                TEST_ENTRY_JDBC_STATEMENT_BUILDER,
                                new JdbcConnectionOptionsBuilder()
                                        .withUrl(getDbMetadata().getUrl())
                                        .withDriverName(getDbMetadata().getDriverClass())
                                        .build()));
        env.execute();

        assertEquals(Arrays.asList(TEST_DATA), selectBooks());
    }

    private List<TestEntry> selectBooks() throws SQLException {
        List<TestEntry> result = new ArrayList<>();
        try (Connection connection = DriverManager.getConnection(getDbMetadata().getUrl())) {
            connection.setTransactionIsolation(Connection.TRANSACTION_READ_COMMITTED);
            connection.setReadOnly(true);
            try (Statement st = connection.createStatement()) {
                try (ResultSet rs =
                        st.executeQuery(
                                "select id, title, author, price, qty from " + INPUT_TABLE)) {
                    while (rs.next()) {
                        result.add(
                                new TestEntry(
                                        getNullable(rs, r -> r.getInt(1)),
                                        getNullable(rs, r -> r.getString(2)),
                                        getNullable(rs, r -> r.getString(3)),
                                        getNullable(rs, r -> r.getDouble(4)),
                                        getNullable(rs, r -> r.getInt(5))));
                    }
                }
            }
        }
        return result;
    }

    @Override
    protected DbMetadata getDbMetadata() {
        return DERBY_EBOOKSHOP_DB;
    }

    private static <T> T getNullable(
            ResultSet rs, FunctionWithException<ResultSet, T, SQLException> get)
            throws SQLException {
        T value = get.apply(rs);
        return rs.wasNull() ? null : value;
    }
}
