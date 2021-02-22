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

package org.apache.flink.table.filesystem;

import org.apache.flink.configuration.Configuration;
import org.apache.flink.table.api.DataTypes;
import org.apache.flink.table.api.EnvironmentSettings;
import org.apache.flink.table.api.ExplainDetail;
import org.apache.flink.table.api.TableConfig;
import org.apache.flink.table.api.TableSchema;
import org.apache.flink.table.api.config.ExecutionConfigOptions;
import org.apache.flink.table.catalog.CatalogTableImpl;
import org.apache.flink.table.catalog.ObjectIdentifier;
import org.apache.flink.table.connector.ChangelogMode;
import org.apache.flink.table.connector.sink.DataStreamSinkProvider;
import org.apache.flink.table.connector.sink.DynamicTableSink;
import org.apache.flink.table.descriptors.DescriptorProperties;
import org.apache.flink.table.factories.FactoryUtil;
import org.apache.flink.table.planner.utils.TableTestUtil;
import org.apache.flink.table.planner.utils.TestingTableEnvironment;
import org.apache.flink.table.runtime.connector.sink.SinkRuntimeProviderContext;
import org.apache.flink.types.RowKind;
import org.apache.flink.util.ExceptionUtils;

import org.junit.Test;

import scala.Option;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

/** Test for {@link FileSystemTableSink}. */
public class FileSystemTableSinkTest {

    private static final TableSchema TEST_SCHEMA =
            TableSchema.builder()
                    .field("f0", DataTypes.STRING())
                    .field("f1", DataTypes.BIGINT())
                    .field("f2", DataTypes.BIGINT())
                    .build();

    @Test
    public void testFileSystemTableSinkWithParallelismInChangeLogMode() {
        int parallelism = 2;
        DescriptorProperties descriptor = new DescriptorProperties();
        descriptor.putString(FactoryUtil.CONNECTOR.key(), "filesystem");
        descriptor.putString("path", "/tmp");
        descriptor.putString("format", "testcsv");
        descriptor.putString(FactoryUtil.SINK_PARALLELISM.key(), String.valueOf(parallelism));

        final DynamicTableSink tableSink =
                FactoryUtil.createTableSink(
                        null,
                        ObjectIdentifier.of("mycatalog", "mydb", "mytable"),
                        new CatalogTableImpl(TEST_SCHEMA, descriptor.asMap(), ""),
                        new Configuration(),
                        Thread.currentThread().getContextClassLoader(),
                        false);
        assertTrue(tableSink instanceof FileSystemTableSink);

        final DynamicTableSink.SinkRuntimeProvider provider =
                tableSink.getSinkRuntimeProvider(new SinkRuntimeProviderContext(false));
        assertTrue(provider instanceof DataStreamSinkProvider);

        try {
            tableSink.getChangelogMode(
                    ChangelogMode.newBuilder()
                            .addContainedKind(RowKind.INSERT)
                            .addContainedKind(RowKind.DELETE)
                            .build());
            fail();
        } catch (Exception e) {
            assertTrue(
                    ExceptionUtils.findThrowableWithMessage(
                                    e, "when the input stream is not INSERT only")
                            .isPresent());
        }
    }

    @Test
    public void testFileSystemTableSinkWithParallelismInStreaming() {
        final int parallelism = 5;
        final EnvironmentSettings settings =
                EnvironmentSettings.newInstance().useBlinkPlanner().inStreamingMode().build();
        final TableConfig tableConfig = new TableConfig();
        tableConfig
                .getConfiguration()
                .set(ExecutionConfigOptions.TABLE_EXEC_RESOURCE_DEFAULT_PARALLELISM, 8);
        final TestingTableEnvironment tEnv =
                TestingTableEnvironment.create(settings, Option.empty(), tableConfig);

        final String testSinkTableName = "test_sink_table";
        final String testCompactSinkTableName = "test_compact_sink_table";
        final String testSourceTableName = "test_source_table";
        final String sourceTableSql =
                "CREATE TABLE "
                        + testSourceTableName
                        + "("
                        + "id BIGINT,"
                        + "real_col FLOAT,"
                        + "double_col DOUBLE,"
                        + "decimal_col DECIMAL(10, 4)"
                        + ") WITH ("
                        + "  'connector'='values',"
                        + "  'bounded'='"
                        + false
                        + "'"
                        + ")";
        final String sinkTableSql = createSinkTableSql(testSinkTableName, parallelism, false);
        final String autoCompactSinkTableSql =
                createSinkTableSql(testCompactSinkTableName, parallelism, true);

        tEnv.executeSql(sinkTableSql);
        tEnv.executeSql(sourceTableSql);
        tEnv.executeSql(autoCompactSinkTableSql);

        final String sql1 =
                "insert into  "
                        + testSinkTableName
                        + " (select * from "
                        + testSourceTableName
                        + ")";
        final String sql2 =
                "insert into  "
                        + testCompactSinkTableName
                        + " (select * from "
                        + testSourceTableName
                        + ")";
        final String actualNormal = tEnv.explainSql(sql1, ExplainDetail.JSON_EXECUTION_PLAN);
        final String actualCompact = tEnv.explainSql(sql2, ExplainDetail.JSON_EXECUTION_PLAN);
        final String expectedNormal =
                TableTestUtil.readFromResourceAndRemoveLastLinkBreak(
                        "/explain/filesystem/testFileSystemTableSinkWithParallelismInStreamingSql0.out");
        final String expectedCompact =
                TableTestUtil.readFromResourceAndRemoveLastLinkBreak(
                        "/explain/filesystem/testFileSystemTableSinkWithParallelismInStreamingSql1.out");

        assertEquals(
                TableTestUtil.replaceStreamNodeId(TableTestUtil.replaceStageId(expectedNormal)),
                TableTestUtil.replaceStreamNodeId(TableTestUtil.replaceStageId(actualNormal)));
        assertEquals(
                TableTestUtil.replaceStreamNodeId(TableTestUtil.replaceStageId(expectedCompact)),
                TableTestUtil.replaceStreamNodeId(TableTestUtil.replaceStageId(actualCompact)));
    }

    @Test
    public void testFileSystemTableSinkWithParallelismInBatch() {
        final int parallelism = 5;
        final EnvironmentSettings settings =
                EnvironmentSettings.newInstance().useBlinkPlanner().inStreamingMode().build();
        final TableConfig tableConfig = new TableConfig();
        tableConfig
                .getConfiguration()
                .set(ExecutionConfigOptions.TABLE_EXEC_RESOURCE_DEFAULT_PARALLELISM, 8);
        final TestingTableEnvironment tEnv =
                TestingTableEnvironment.create(settings, Option.empty(), tableConfig);

        final String testSinkTableName = "test_sink_table";
        final String testSourceTableName = "test_source_table";
        final String sourceTableSql =
                "CREATE TABLE "
                        + testSourceTableName
                        + "("
                        + "id BIGINT,"
                        + "real_col FLOAT,"
                        + "double_col DOUBLE,"
                        + "decimal_col DECIMAL(10, 4)"
                        + ") WITH ("
                        + "  'connector'='values',"
                        + "  'bounded'='"
                        + true
                        + "'"
                        + ")";
        final String sinkTableSql = createSinkTableSql(testSinkTableName, parallelism, false);
        tEnv.executeSql(sinkTableSql);
        tEnv.executeSql(sourceTableSql);

        final String sql =
                "insert into  "
                        + testSinkTableName
                        + " (select * from "
                        + testSourceTableName
                        + ")";
        final String actual = tEnv.explainSql(sql, ExplainDetail.JSON_EXECUTION_PLAN);
        final String expected =
                TableTestUtil.readFromResourceAndRemoveLastLinkBreak(
                        "/explain/filesystem/testFileSystemTableSinkWithParallelismInBatch.out");

        assertEquals(
                TableTestUtil.replaceStreamNodeId(TableTestUtil.replaceStageId(expected)),
                TableTestUtil.replaceStreamNodeId(TableTestUtil.replaceStageId(actual)));
    }

    private String createSinkTableSql(String tableName, int parallelism, boolean autoCompaction) {
        return "CREATE TABLE "
                + tableName
                + "("
                + "id BIGINT,"
                + "real_col FLOAT,"
                + "double_col DOUBLE,"
                + "decimal_col DECIMAL(10, 4)"
                + ") WITH ("
                + "  'connector'='filesystem',"
                + "  'path'='/tmp',"
                + "  'auto-compaction'='"
                + autoCompaction
                + "',"
                + "  'format'='testcsv',"
                + "  '"
                + FactoryUtil.SINK_PARALLELISM.key()
                + "'='"
                + parallelism
                + "'"
                + ")";
    }
}
