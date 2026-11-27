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

import org.apache.flink.api.common.JobID;
import org.apache.flink.configuration.ReadableConfig;
import org.apache.flink.table.api.ResultKind;
import org.apache.flink.table.client.gateway.Executor;
import org.apache.flink.table.client.gateway.StatementResult;
import org.apache.flink.table.data.RowData;
import org.apache.flink.util.CloseableIterator;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;
import org.junit.jupiter.api.io.TempDir;

import javax.annotation.Nullable;

import java.net.URI;
import java.nio.file.Path;
import java.sql.ResultSet;
import java.sql.SQLFeatureNotSupportedException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/** Tests for flink statement. */
public class FlinkStatementTest extends FlinkJdbcDriverTestBase {
    @TempDir private Path tempDir;

    @Test
    @Timeout(value = 60)
    void testExecuteQuery() throws Exception {
        try (FlinkConnection connection = new FlinkConnection(getDriverUri())) {
            try (Statement statement = connection.createStatement()) {
                // CREATE TABLE is not a query and has no results
                assertThat(
                                statement.execute(
                                        String.format(
                                                "CREATE TABLE test_table(id bigint, val int, str string, timestamp1 timestamp(0), timestamp2 timestamp_ltz(3), time_data time, date_data date) "
                                                        + "with ("
                                                        + "'connector'='filesystem',\n"
                                                        + "'format'='csv',\n"
                                                        + "'path'='%s')",
                                                tempDir)))
                        .isFalse();
                assertThat(statement.getUpdateCount()).isEqualTo(0);

                // INSERT TABLE returns job id
                assertThat(
                                statement.execute(
                                        "INSERT INTO test_table VALUES "
                                                + "(1, 11, '111', TIMESTAMP '2021-04-15 23:18:36', TO_TIMESTAMP_LTZ(400000000000, 3), TIME '12:32:00', DATE '2023-11-02'), "
                                                + "(3, 33, '333', TIMESTAMP '2021-04-16 23:18:36', TO_TIMESTAMP_LTZ(500000000000, 3), TIME '13:32:00', DATE '2023-12-02'), "
                                                + "(2, 22, '222', TIMESTAMP '2021-04-17 23:18:36', TO_TIMESTAMP_LTZ(600000000000, 3), TIME '14:32:00', DATE '2023-01-02'), "
                                                + "(4, 44, '444', TIMESTAMP '2021-04-18 23:18:36', TO_TIMESTAMP_LTZ(700000000000, 3), TIME '15:32:00', DATE '2023-02-02')"))
                        .isTrue();
                assertThatThrownBy(statement::getUpdateCount)
                        .isInstanceOf(SQLFeatureNotSupportedException.class)
                        .hasMessage("FlinkStatement#getUpdateCount is not supported for query");

                String jobId;
                try (ResultSet resultSet = statement.getResultSet()) {
                    assertThat(resultSet.next()).isTrue();
                    assertThat(resultSet.getMetaData().getColumnCount()).isEqualTo(1);
                    jobId = resultSet.getString("job id");
                    assertThat(resultSet.getString(1)).isEqualTo(jobId);
                    assertThat(resultSet.next()).isFalse();
                }
                assertThat(jobId).isNotNull();
                // Wait job finished
                boolean jobFinished = false;
                while (!jobFinished) {
                    assertThat(statement.execute("SHOW JOBS")).isTrue();
                    try (ResultSet resultSet = statement.getResultSet()) {
                        while (resultSet.next()) {
                            if (resultSet.getString(1).equals(jobId)) {
                                if (resultSet.getString(3).equals("FINISHED")) {
                                    jobFinished = true;
                                    break;
                                }
                            }
                        }
                    }
                }

                // SELECT all data from test_table
                statement.execute("SET 'table.local-time-zone' = 'UTC'");
                try (ResultSet resultSet = statement.executeQuery("SELECT * FROM test_table")) {
                    assertThat(resultSet.getMetaData().getColumnCount()).isEqualTo(7);
                    List<String> resultList = new ArrayList<>();
                    while (resultSet.next()) {
                        assertThat(resultSet.getLong(1)).isEqualTo(resultSet.getLong("id"));
                        assertThat(resultSet.getInt(2)).isEqualTo(resultSet.getInt("val"));
                        assertThat(resultSet.getString(3)).isEqualTo(resultSet.getString("str"));
                        assertThat(resultSet.getObject(4))
                                .isEqualTo(resultSet.getTimestamp("timestamp1"));
                        assertThat(resultSet.getTimestamp(5))
                                .isEqualTo(resultSet.getObject("timestamp2"));
                        assertThat(resultSet.getTime(6))
                                .isEqualTo(resultSet.getObject("time_data"));
                        assertThat(resultSet.getDate(7))
                                .isEqualTo(resultSet.getObject("date_data"));
                        resultList.add(
                                String.format(
                                        "%s,%s,%s,%s,%s,%s,%s",
                                        resultSet.getLong("id"),
                                        resultSet.getInt("val"),
                                        resultSet.getString("str"),
                                        resultSet.getTimestamp("timestamp1"),
                                        resultSet.getTimestamp("timestamp2"),
                                        resultSet.getTime("time_data"),
                                        resultSet.getDate("date_data")));
                    }
                    assertThat(resultList)
                            .containsExactlyInAnyOrder(
                                    "1,11,111,2021-04-15 23:18:36.0,1982-09-04 15:06:40.0,12:32:00,2023-11-02",
                                    "3,33,333,2021-04-16 23:18:36.0,1985-11-05 00:53:20.0,13:32:00,2023-12-02",
                                    "2,22,222,2021-04-17 23:18:36.0,1989-01-05 10:40:00.0,14:32:00,2023-01-02",
                                    "4,44,444,2021-04-18 23:18:36.0,1992-03-07 20:26:40.0,15:32:00,2023-02-02");
                }

                // SELECT all data from test_table with local time zone
                statement.execute("SET 'table.local-time-zone' = 'Asia/Shanghai'");
                try (ResultSet resultSet = statement.executeQuery("SELECT * FROM test_table")) {
                    assertThat(resultSet.getMetaData().getColumnCount()).isEqualTo(7);
                    List<String> resultList = new ArrayList<>();
                    while (resultSet.next()) {
                        resultList.add(
                                String.format(
                                        "%s,%s",
                                        resultSet.getTimestamp("timestamp1"),
                                        resultSet.getTimestamp("timestamp2")));
                    }
                    assertThat(resultList)
                            .containsExactlyInAnyOrder(
                                    "2021-04-15 23:18:36.0,1982-09-04 23:06:40.0",
                                    "2021-04-16 23:18:36.0,1985-11-05 08:53:20.0",
                                    "2021-04-17 23:18:36.0,1989-01-05 18:40:00.0",
                                    "2021-04-18 23:18:36.0,1992-03-08 04:26:40.0");
                }

                assertThat(statement.execute("SHOW JOBS")).isTrue();
                try (ResultSet resultSet = statement.getResultSet()) {
                    // Check there are two finished jobs.
                    int count = 0;
                    while (resultSet.next()) {
                        assertThat(resultSet.getString(3)).isEqualTo("FINISHED");
                        count++;
                    }
                    assertThat(count).isEqualTo(3);
                }
            }
        }
    }

    @Test
    void testCloseAllStatements() throws Exception {
        FlinkConnection connection = new FlinkConnection(getDriverUri());
        Statement statement1 = connection.createStatement();
        Statement statement2 = connection.createStatement();
        Statement statement3 = connection.createStatement();

        statement1.close();
        connection.close();

        assertThat(statement1.isClosed()).isTrue();
        assertThat(statement2.isClosed()).isTrue();
        assertThat(statement3.isClosed()).isTrue();
    }

    @Test
    void testCloseNonQuery() throws Exception {
        CompletableFuture<Void> closedFuture = new CompletableFuture<>();
        try (FlinkConnection connection = new FlinkConnection(new TestingExecutor(closedFuture))) {
            try (Statement statement = connection.createStatement()) {
                assertThatThrownBy(() -> statement.executeQuery("INSERT"))
                        .hasMessage(String.format("Statement[%s] is not a query.", "INSERT"));
                closedFuture.get(10, TimeUnit.SECONDS);
            }
        }
    }

    /** Testing executor. */
    static class TestingExecutor implements Executor {
        private final CompletableFuture<Void> closedFuture;

        TestingExecutor(CompletableFuture<Void> closedFuture) {
            this.closedFuture = closedFuture;
        }

        @Override
        public void configureSession(String statement) {
            throw new UnsupportedOperationException();
        }

        @Override
        public ReadableConfig getSessionConfig() {
            throw new UnsupportedOperationException();
        }

        @Override
        public Map<String, String> getSessionConfigMap() {
            throw new UnsupportedOperationException();
        }

        @Override
        public StatementResult executeStatement(String statement) {
            return new StatementResult(
                    null,
                    new CloseableIterator<RowData>() {
                        @Override
                        public void close() throws Exception {
                            closedFuture.complete(null);
                        }

                        @Override
                        public boolean hasNext() {
                            return false;
                        }

                        @Override
                        public RowData next() {
                            throw new UnsupportedOperationException();
                        }
                    },
                    false,
                    ResultKind.SUCCESS_WITH_CONTENT,
                    JobID.generate());
        }

        @Override
        public List<String> completeStatement(String statement, int position) {
            throw new UnsupportedOperationException();
        }

        @Override
        public String deployScript(@Nullable String script, @Nullable URI uri) {
            throw new UnsupportedOperationException();
        }

        @Override
        public void close() {}
    }
}
