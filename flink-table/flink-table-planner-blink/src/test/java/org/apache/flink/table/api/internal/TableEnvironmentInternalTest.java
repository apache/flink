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

package org.apache.flink.table.api.internal;

import org.apache.flink.table.api.EnvironmentSettings;
import org.apache.flink.table.api.ExplainDetail;
import org.apache.flink.table.api.TableEnvironment;
import org.apache.flink.table.api.TableException;
import org.apache.flink.table.api.TableResult;
import org.apache.flink.table.planner.utils.TableTestUtil;

import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;
import org.junit.rules.TemporaryFolder;

import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.ExecutionException;

import static org.apache.flink.util.Preconditions.checkNotNull;
import static org.junit.Assert.assertEquals;

/** Test for {@link TableEnvironmentInternal}. */
public class TableEnvironmentInternalTest {

    @Rule public ExpectedException exception = ExpectedException.none();

    @Rule public TemporaryFolder tmpFolder = new TemporaryFolder();

    private TableEnvironmentInternal tableEnv;

    @Before
    public void setup() {
        EnvironmentSettings settings =
                EnvironmentSettings.newInstance().useBlinkPlanner().inStreamingMode().build();
        tableEnv = (TableEnvironmentInternal) TableEnvironment.create(settings);

        String srcTableDdl =
                "CREATE TABLE MyTable (\n"
                        + "  a bigint,\n"
                        + "  b int,\n"
                        + "  c varchar\n"
                        + ") with (\n"
                        + "  'connector' = 'values',\n"
                        + "  'bounded' = 'false')";
        tableEnv.executeSql(srcTableDdl);

        String sinkTableDdl =
                "CREATE TABLE MySink (\n"
                        + "  a bigint,\n"
                        + "  b int,\n"
                        + "  c varchar\n"
                        + ") with (\n"
                        + "  'connector' = 'values',\n"
                        + "  'table-sink-class' = 'DEFAULT')";
        tableEnv.executeSql(sinkTableDdl);
    }

    @Test
    public void testGetJsonPlan() throws IOException {
        String jsonPlan = tableEnv.getJsonPlan("insert into MySink select * from MyTable");
        String actual = TableTestUtil.readFromResource("/jsonplan/testGetJsonPlan.out");
        assertEquals(
                TableTestUtil.replaceExecNodeId(
                        TableTestUtil.replaceFlinkVersion(
                                TableTestUtil.getFormattedJson(jsonPlan))),
                TableTestUtil.replaceExecNodeId(TableTestUtil.getFormattedJson(actual)));
    }

    @Test
    public void testExecuteJsonPlan() throws ExecutionException, InterruptedException, IOException {
        File sourceFile = tmpFolder.newFile();
        List<String> data = Arrays.asList("1,1,hi", "2,1,hello", "3,2,hello world");
        Collections.shuffle(data);
        Files.write(sourceFile.toPath(), String.join("\n", data).getBytes());

        String srcTableDdl =
                String.format(
                        "CREATE TABLE src (\n"
                                + "  a bigint,\n"
                                + "  b int,\n"
                                + "  c varchar\n"
                                + ") with (\n"
                                + "  'connector' = 'filesystem',\n"
                                + "  'path' = '%s',\n"
                                + "  'format' = 'testcsv')",
                        sourceFile.getAbsolutePath());
        tableEnv.executeSql(srcTableDdl);

        File sinkPath = tmpFolder.newFolder();
        String sinkTableDdl =
                String.format(
                        "CREATE TABLE sink (\n"
                                + "  a bigint,\n"
                                + "  b int,\n"
                                + "  c varchar\n"
                                + ") with (\n"
                                + "  'connector' = 'filesystem',\n"
                                + "  'path' = '%s',\n"
                                + "  'format' = 'testcsv')",
                        sinkPath.getAbsolutePath());
        tableEnv.executeSql(sinkTableDdl);

        String jsonPlan = tableEnv.getJsonPlan("insert into sink select * from src");
        TableResult tableResult = tableEnv.executeJsonPlan(jsonPlan);
        tableResult.await();

        // read result data
        List<String> result = new ArrayList<>();
        for (File file : checkNotNull(sinkPath.listFiles())) {
            if (file.isFile()) {
                String value = new String(Files.readAllBytes(file.toPath()));
                result.addAll(Arrays.asList(value.split("\n")));
            }
        }
        Collections.sort(data);
        Collections.sort(result);
        assertEquals(data, result);
    }

    @Test
    public void testExplainJsonPlan() {
        String jsonPlan = TableTestUtil.readFromResource("/jsonplan/testGetJsonPlan.out");
        String actual = tableEnv.explainJsonPlan(jsonPlan, ExplainDetail.JSON_EXECUTION_PLAN);
        String expected = TableTestUtil.readFromResource("/explain/testExplainJsonPlan.out");
        assertEquals(expected, TableTestUtil.replaceStreamNodeId(actual));
    }

    @Test
    public void testProjectPushDown() {
        String sinkTableDdl =
                "CREATE TABLE sink (\n"
                        + "  a bigint,\n"
                        + "  b int\n"
                        + ") with (\n"
                        + "  'connector' = 'values',\n"
                        + "  'table-sink-class' = 'DEFAULT')";
        tableEnv.executeSql(sinkTableDdl);
        exception.expect(TableException.class);
        exception.expectMessage(
                "DynamicTableSource with project push-down is not supported for JSON serialization now");
        tableEnv.getJsonPlan("insert into sink select a, b from MyTable");
    }

    @Test
    public void testFilterPushDown() {
        String srcTableDdl =
                "CREATE TABLE src (\n"
                        + "  a bigint,\n"
                        + "  b int,\n"
                        + "  c varchar\n"
                        + ") with (\n"
                        + "  'connector' = 'values',\n"
                        + "  'bounded' = 'false',"
                        + "  'filterable-fields' = 'a')";
        tableEnv.executeSql(srcTableDdl);
        exception.expect(TableException.class);
        exception.expectMessage(
                "DynamicTableSource with filter push-down is not supported for JSON serialization now");
        tableEnv.getJsonPlan("insert into MySink select * from src where a > 0");
    }

    @Test
    public void testLimitPushDown() {
        exception.expect(TableException.class);
        // currently, there is a StreamExecLimit in the plan, once StreamExecLimit does support
        // json serialization/deserialization, the following exception message should be updated.
        exception.expectMessage(
                "StreamExecLimit does not implement @JsonCreator annotation on constructor");
        tableEnv.getJsonPlan("insert into MySink select * from MyTable limit 3");
    }

    @Test
    public void testPartitionPushDown() {
        String srcTableDdl =
                "CREATE TABLE PartitionTable (\n"
                        + "  a bigint,\n"
                        + "  b int,\n"
                        + "  p varchar)\n"
                        + "partitioned by (p)\n"
                        + "with (\n"
                        + "  'connector' = 'values',\n"
                        + "  'bounded' = 'false',"
                        + "  'partition-list' = 'p:A')";
        tableEnv.executeSql(srcTableDdl);
        exception.expect(TableException.class);
        // currently, there is a StreamExecCalc in the plan, once StreamExecCalc does support
        // json serialization/deserialization, the following exception message should be updated.
        exception.expectMessage(
                "StreamExecCalc does not implement @JsonCreator annotation on constructor");
        tableEnv.getJsonPlan("insert into MySink select * from PartitionTable where p = 'A'");
    }

    @Test
    public void testWatermarkPushDown() {
        String srcTableDdl =
                "CREATE TABLE WatermarkTable (\n"
                        + "  a bigint,\n"
                        + "  b int,\n"
                        + "  c timestamp(3),\n"
                        + "  watermark for c as c - interval '5' second\n"
                        + ") with (\n"
                        + "  'connector' = 'values',\n"
                        + "  'bounded' = 'false',"
                        + "  'enable-watermark-push-down' = 'true',"
                        + "  'disable-lookup' = 'true')";
        tableEnv.executeSql(srcTableDdl);
        String sinkTableDdl =
                "CREATE TABLE sink (\n"
                        + "  a bigint,\n"
                        + "  b int,\n"
                        + "  c timestamp(3)\n"
                        + ") with (\n"
                        + "  'connector' = 'values',\n"
                        + "  'table-sink-class' = 'DEFAULT')";
        tableEnv.executeSql(sinkTableDdl);
        exception.expect(TableException.class);
        exception.expectMessage(
                "DynamicTableSource with watermark push-down is not supported for JSON serialization now");
        tableEnv.getJsonPlan("insert into sink select * from WatermarkTable");
    }

    @Test
    public void testUnsupportedNodes() {
        String srcTableDdl =
                "CREATE TABLE src (\n"
                        + "  a2 bigint,\n"
                        + "  b2 int,\n"
                        + "  c2 varchar\n"
                        + ") with (\n"
                        + "  'connector' = 'values',\n"
                        + "  'bounded' = 'false')";
        tableEnv.executeSql(srcTableDdl);

        exception.expect(TableException.class);
        exception.expectMessage(
                "StreamExecCalc does not implement @JsonCreator annotation on constructor");
        tableEnv.getJsonPlan(
                "insert into MySink select a, b2, c2 from MyTable, src where a = a2 and b2 > 10");
    }

    @Test
    public void testBatchMode() {
        EnvironmentSettings settings =
                EnvironmentSettings.newInstance().useBlinkPlanner().inBatchMode().build();
        tableEnv = (TableEnvironmentInternal) TableEnvironment.create(settings);

        String srcTableDdl =
                "CREATE TABLE src (\n"
                        + "  a bigint\n"
                        + ") with (\n"
                        + "  'connector' = 'values',\n"
                        + "  'bounded' = 'true')";
        tableEnv.executeSql(srcTableDdl);

        String sinkTableDdl =
                "CREATE TABLE sink (\n"
                        + "  a bigint\n"
                        + ") with (\n"
                        + "  'connector' = 'values',\n"
                        + "  'table-sink-class' = 'DEFAULT')";
        tableEnv.executeSql(sinkTableDdl);

        exception.expect(TableException.class);
        exception.expectMessage("Only streaming mode is supported now");
        tableEnv.getJsonPlan("insert into sink select * from src");
    }
}
