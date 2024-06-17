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

package org.apache.flink.table.file.testutils.catalog;

import org.apache.flink.table.api.EnvironmentSettings;
import org.apache.flink.table.api.TableEnvironment;
import org.apache.flink.table.planner.factories.TestValuesTableFactory;
import org.apache.flink.types.Row;
import org.apache.flink.util.CloseableIterator;

import org.apache.flink.shaded.guava31.com.google.common.collect.Lists;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

import java.util.Arrays;
import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;

/** Integration tests for {@link TestFileSystemCatalog}. */
public class TestFileSystemCatalogITCase extends TestFileSystemCatalogTestBase {

    @ParameterizedTest
    @ValueSource(booleans = {true, false})
    public void testReadAndWriteTestFileSystemTable(boolean isStreamingMode) throws Exception {
        TableEnvironment tEnv =
                TableEnvironment.create(
                        isStreamingMode
                                ? EnvironmentSettings.inStreamingMode()
                                : EnvironmentSettings.inBatchMode());
        tEnv.registerCatalog(TEST_CATALOG, catalog);
        tEnv.useCatalog(TEST_CATALOG);

        tEnv.executeSql(
                "CREATE TABLE CsvTable (\n"
                        + "  id BIGINT,\n"
                        + "  user_name STRING,\n"
                        + "  message STRING,\n"
                        + "  log_ts STRING\n"
                        + ") WITH (\n"
                        + "  'format' = 'csv'\n"
                        + ")");

        tEnv.getConfig().getConfiguration().setString("parallelism.default", "1");
        tEnv.executeSql(
                        String.format(
                                "INSERT INTO %s.%s.CsvTable VALUES\n"
                                        + "(1001, 'user1', 'hello world', '2021-06-10 10:00:00'),\n"
                                        + "(1002, 'user2', 'hi', '2021-06-10 10:01:00'),\n"
                                        + "(1003, 'user3', 'ciao', '2021-06-10 10:02:00'),\n"
                                        + "(1004, 'user4', '你好', '2021-06-10 10:03:00')",
                                TEST_CATALOG, TEST_DEFAULT_DATABASE))
                .await();

        CloseableIterator<Row> result =
                tEnv.executeSql(
                                String.format(
                                        "SELECT * FROM %s.%s.CsvTable",
                                        TEST_CATALOG, TEST_DEFAULT_DATABASE))
                        .collect();

        // assert query result
        List<Row> expected =
                Arrays.asList(
                        Row.of(1001L, "user1", "hello world", "2021-06-10 10:00:00"),
                        Row.of(1002L, "user2", "hi", "2021-06-10 10:01:00"),
                        Row.of(1003L, "user3", "ciao", "2021-06-10 10:02:00"),
                        Row.of(1004L, "user4", "你好", "2021-06-10 10:03:00"));

        assertThat(Lists.newArrayList(result)).isEqualTo(expected);
    }

    @ParameterizedTest
    @ValueSource(booleans = {true, false})
    public void testReadDatagenTable(boolean isStreamingMode) {
        TableEnvironment tEnv =
                TableEnvironment.create(
                        isStreamingMode
                                ? EnvironmentSettings.inStreamingMode()
                                : EnvironmentSettings.inBatchMode());
        tEnv.registerCatalog(TEST_CATALOG, catalog);
        tEnv.useCatalog(TEST_CATALOG);

        tEnv.executeSql(
                "CREATE TABLE datagenSource (\n"
                        + "  id BIGINT,\n"
                        + "  user_name STRING,\n"
                        + "  message STRING,\n"
                        + "  log_ts STRING\n"
                        + ") WITH (\n"
                        + "  'connector' = 'datagen',\n"
                        + "  'number-of-rows' = '10'\n"
                        + ")");

        tEnv.getConfig().getConfiguration().setString("parallelism.default", "1");
        CloseableIterator<Row> result =
                tEnv.executeSql(
                                String.format(
                                        "SELECT * FROM %s.%s.datagenSource",
                                        TEST_CATALOG, TEST_DEFAULT_DATABASE))
                        .collect();

        // assert query result size
        assertThat(Lists.newArrayList(result).size()).isEqualTo(10);
    }

    @Test
    public void testWriteTestValuesSinkTable() throws Exception {
        TableEnvironment tEnv = TableEnvironment.create(EnvironmentSettings.newInstance().build());
        tEnv.registerCatalog(TEST_CATALOG, catalog);
        tEnv.useCatalog(TEST_CATALOG);

        tEnv.executeSql(
                "CREATE TABLE valueSink (\n"
                        + "  id BIGINT,\n"
                        + "  user_name STRING,\n"
                        + "  message STRING,\n"
                        + "  log_ts STRING\n"
                        + ") WITH (\n"
                        + "  'connector' = 'values'\n"
                        + ")");

        tEnv.getConfig().getConfiguration().setString("parallelism.default", "1");
        tEnv.getConfig().getConfiguration().setString("parallelism.default", "1");
        tEnv.executeSql(
                        String.format(
                                "INSERT INTO %s.%s.valueSink VALUES\n"
                                        + "(1001, 'user1', 'hello world', '2021-06-10 10:00:00'),\n"
                                        + "(1002, 'user2', 'hi', '2021-06-10 10:01:00'),\n"
                                        + "(1003, 'user3', 'ciao', '2021-06-10 10:02:00'),\n"
                                        + "(1004, 'user4', '你好', '2021-06-10 10:03:00')",
                                TEST_CATALOG, TEST_DEFAULT_DATABASE))
                .await();

        // assert query result size
        List<Row> expected =
                Arrays.asList(
                        Row.of(1001L, "user1", "hello world", "2021-06-10 10:00:00"),
                        Row.of(1002L, "user2", "hi", "2021-06-10 10:01:00"),
                        Row.of(1003L, "user3", "ciao", "2021-06-10 10:02:00"),
                        Row.of(1004L, "user4", "你好", "2021-06-10 10:03:00"));

        List<Row> actual = TestValuesTableFactory.getRawResults("valueSink");
        assertThat(actual).isEqualTo(expected);
    }
}
