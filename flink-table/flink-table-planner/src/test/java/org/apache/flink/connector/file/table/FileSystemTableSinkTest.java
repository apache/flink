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

package org.apache.flink.connector.file.table;

import org.apache.flink.table.api.EnvironmentSettings;
import org.apache.flink.table.api.ExplainDetail;
import org.apache.flink.table.api.TableEnvironment;
import org.apache.flink.table.api.ValidationException;
import org.apache.flink.table.api.config.ExecutionConfigOptions;

import org.junit.Test;

import java.util.Arrays;
import java.util.HashSet;
import java.util.Set;

import static org.apache.flink.core.testutils.CommonTestUtils.assertThrows;
import static org.apache.flink.table.planner.utils.TableTestUtil.readFromResource;
import static org.apache.flink.table.planner.utils.TableTestUtil.replaceNodeIdInOperator;
import static org.apache.flink.table.planner.utils.TableTestUtil.replaceStageId;
import static org.apache.flink.table.planner.utils.TableTestUtil.replaceStreamNodeId;
import static org.junit.Assert.assertEquals;

/** Test for {@link FileSystemTableSink}. */
public class FileSystemTableSinkTest {

    @Test
    public void testExceptionWhenSettingParallelismWithUpdatingQuery() {
        final TableEnvironment tEnv =
                TableEnvironment.create(EnvironmentSettings.inStreamingMode());

        final String testSourceTableName = "test_source_table";
        tEnv.executeSql(buildSourceTableSql(testSourceTableName, false));

        final String testSinkTableName = "test_sink_table";
        tEnv.executeSql(buildSinkTableSql(testSinkTableName, 10, false));
        String sql =
                String.format(
                        "INSERT INTO %s SELECT DISTINCT * FROM %s",
                        testSinkTableName, testSourceTableName);

        assertThrows(
                "filesystem sink doesn't support setting parallelism (10) by 'sink.parallelism' when the input stream is not INSERT only.",
                ValidationException.class,
                () -> tEnv.explainSql(sql));
    }

    @Test
    public void testFileSystemTableSinkWithParallelismInStreaming() {
        final int parallelism = 5;
        final TableEnvironment tEnv =
                TableEnvironment.create(EnvironmentSettings.inStreamingMode());
        tEnv.getConfig().set(ExecutionConfigOptions.TABLE_EXEC_RESOURCE_DEFAULT_PARALLELISM, 8);

        final String testSourceTableName = "test_source_table";
        tEnv.executeSql(buildSourceTableSql(testSourceTableName, false));

        // verify operator parallelisms when compaction is not enabled
        final String testSinkTableName = "test_sink_table";
        tEnv.executeSql(buildSinkTableSql(testSinkTableName, parallelism, false));
        final String sql0 = buildInsertIntoSql(testSinkTableName, testSourceTableName);
        final String actualNormal = tEnv.explainSql(sql0, ExplainDetail.JSON_EXECUTION_PLAN);
        final String expectedNormal =
                readFromResource(
                        "/explain/filesystem/testFileSystemTableSinkWithParallelismInStreamingSql0.out");
        assertEquals(
                replaceNodeIdInOperator(replaceStreamNodeId(replaceStageId(expectedNormal))),
                replaceNodeIdInOperator(replaceStreamNodeId(replaceStageId(actualNormal))));

        // verify operator parallelisms when compaction is enabled
        final String testCompactSinkTableName = "test_compact_sink_table";
        tEnv.executeSql(buildSinkTableSql(testCompactSinkTableName, parallelism, true));
        final String sql1 = buildInsertIntoSql(testCompactSinkTableName, testSourceTableName);
        final String actualCompact = tEnv.explainSql(sql1, ExplainDetail.JSON_EXECUTION_PLAN);
        final String expectedCompact =
                readFromResource(
                        "/explain/filesystem/testFileSystemTableSinkWithParallelismInStreamingSql1.out");
        assertEquals(
                replaceNodeIdInOperator(replaceStreamNodeId(replaceStageId(expectedCompact))),
                replaceNodeIdInOperator(replaceStreamNodeId(replaceStageId(actualCompact))));
    }

    @Test
    public void testFileSystemTableSinkWithParallelismInBatch() {
        final int parallelism = 5;
        final TableEnvironment tEnv = TableEnvironment.create(EnvironmentSettings.inBatchMode());
        tEnv.getConfig().set(ExecutionConfigOptions.TABLE_EXEC_RESOURCE_DEFAULT_PARALLELISM, 8);

        final String testSourceTableName = "test_source_table";
        final String testSinkTableName = "test_sink_table";
        tEnv.executeSql(buildSourceTableSql(testSourceTableName, true));
        tEnv.executeSql(buildSinkTableSql(testSinkTableName, parallelism, false));

        final String sql = buildInsertIntoSql(testSinkTableName, testSourceTableName);
        final String actual = tEnv.explainSql(sql, ExplainDetail.JSON_EXECUTION_PLAN);
        final String expected =
                readFromResource(
                        "/explain/filesystem/testFileSystemTableSinkWithParallelismInBatch.out");

        assertEquals(
                replaceNodeIdInOperator(replaceStreamNodeId(replaceStageId(expected))),
                replaceNodeIdInOperator(replaceStreamNodeId(replaceStageId(actual))));
    }

    private static String buildSourceTableSql(String testSourceTableName, boolean bounded) {
        return String.format(
                "CREATE TABLE %s ("
                        + " id BIGINT,"
                        + " real_col FLOAT,"
                        + " double_col DOUBLE,"
                        + " decimal_col DECIMAL(10, 4)"
                        + ") WITH ("
                        + " 'connector' = 'values',"
                        + " 'bounded' = '%s'"
                        + ")",
                testSourceTableName, bounded);
    }

    private static String buildSinkTableSql(
            String tableName, int parallelism, boolean autoCompaction) {
        return String.format(
                "CREATE TABLE %s ("
                        + " id BIGINT,"
                        + " real_col FLOAT,"
                        + " double_col DOUBLE,"
                        + " decimal_col DECIMAL(10, 4)"
                        + ") WITH ("
                        + " 'connector' = 'filesystem',"
                        + " 'path' = '/tmp',"
                        + " 'auto-compaction' = '%s',"
                        + " 'format' = 'testcsv',"
                        + " 'sink.parallelism' = '%s'"
                        + ")",
                tableName, autoCompaction, parallelism);
    }

    private static String buildInsertIntoSql(String sinkTable, String sourceTable) {
        return String.format("INSERT INTO %s SELECT * FROM %s", sinkTable, sourceTable);
    }

    @Test
    public void testFileSystemTableSinkWithCustomCommitPolicy() throws Exception {
        final String outputTable = "outputTable";
        final String customPartitionCommitPolicyClassName = TestCustomCommitPolicy.class.getName();
        final TableEnvironment tEnv =
                TableEnvironment.create(EnvironmentSettings.inStreamingMode());
        String ddl =
                "CREATE TABLE %s ("
                        + "  a INT,"
                        + "  b STRING,"
                        + "  d STRING,"
                        + "  e STRING"
                        + ") PARTITIONED BY (d, e) WITH ("
                        + "'connector'='filesystem',"
                        + "'path'='/tmp',"
                        + "'format'='testcsv',"
                        + "'sink.partition-commit.delay'='0s',"
                        + "'sink.partition-commit.policy.kind'='custom',"
                        + "'sink.partition-commit.policy.class'='%s',"
                        + "'sink.partition-commit.policy.class.parameters'='test1;test2'"
                        + ")";
        ddl = String.format(ddl, outputTable, customPartitionCommitPolicyClassName);
        tEnv.executeSql(ddl);
        tEnv.executeSql(
                        "insert into outputTable select *"
                                + " from (values (1, 'a', '2020-05-03', '3'), "
                                + "(2, 'x', '2020-05-03', '4'))")
                .await();
        Set<String> actualCommittedPaths =
                TestCustomCommitPolicy.getCommittedPartitionPathsAndReset();
        Set<String> expectedCommittedPaths =
                new HashSet<>(
                        Arrays.asList(
                                "test1test2", "/tmp/d=2020-05-03/e=3", "/tmp/d=2020-05-03/e=4"));
        assertEquals(expectedCommittedPaths, actualCommittedPaths);
    }
}
