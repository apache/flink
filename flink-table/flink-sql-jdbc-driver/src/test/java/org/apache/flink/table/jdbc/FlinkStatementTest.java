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
import org.junit.jupiter.api.Timeout;
import org.junit.jupiter.api.io.TempDir;

import java.nio.file.Path;
import java.sql.ResultSet;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

/** Tests for flink statement. */
public class FlinkStatementTest extends FlinkJdbcDriverTestBase {
    @TempDir private Path tempDir;

    @Test
    @Timeout(value = 60)
    public void testExecuteQuery() throws Exception {
        try (FlinkConnection connection = new FlinkConnection(getDriverUri())) {
            try (Statement statement = connection.createStatement()) {
                // CREATE TABLE is not a query and has no results
                assertFalse(
                        statement.execute(
                                String.format(
                                        "CREATE TABLE test_table(id bigint, val int, str string) "
                                                + "with ("
                                                + "'connector'='filesystem',\n"
                                                + "'format'='csv',\n"
                                                + "'path'='%s')",
                                        tempDir)));

                // INSERT TABLE returns job id
                assertTrue(
                        statement.execute(
                                "INSERT INTO test_table VALUES "
                                        + "(1, 11, '111'), "
                                        + "(3, 33, '333'), "
                                        + "(2, 22, '222'), "
                                        + "(4, 44, '444')"));
                String jobId;
                try (ResultSet resultSet = statement.getResultSet()) {
                    assertTrue(resultSet.next());
                    assertEquals(1, resultSet.getMetaData().getColumnCount());
                    jobId = resultSet.getString("job id");
                    assertEquals(jobId, resultSet.getString(1));
                    assertFalse(resultSet.next());
                }
                assertNotNull(jobId);
                // Wait job finished
                boolean jobFinished = false;
                while (!jobFinished) {
                    assertTrue(statement.execute("SHOW JOBS"));
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
                try (ResultSet resultSet = statement.executeQuery("SELECT * FROM test_table")) {
                    assertEquals(3, resultSet.getMetaData().getColumnCount());
                    List<String> resultList = new ArrayList<>();
                    while (resultSet.next()) {
                        assertEquals(resultSet.getLong("id"), resultSet.getLong(1));
                        assertEquals(resultSet.getInt("val"), resultSet.getInt(2));
                        assertEquals(resultSet.getString("str"), resultSet.getString(3));
                        resultList.add(
                                String.format(
                                        "%s,%s,%s",
                                        resultSet.getLong("id"),
                                        resultSet.getInt("val"),
                                        resultSet.getString("str")));
                    }
                    assertThat(resultList)
                            .containsExactlyInAnyOrder(
                                    "1,11,111", "2,22,222", "3,33,333", "4,44,444");
                }

                assertTrue(statement.execute("SHOW JOBS"));
                try (ResultSet resultSet = statement.getResultSet()) {
                    // Check there are two finished jobs.
                    int count = 0;
                    while (resultSet.next()) {
                        assertEquals("FINISHED", resultSet.getString(3));
                        count++;
                    }
                    assertEquals(2, count);
                }
            }
        }
    }
}
