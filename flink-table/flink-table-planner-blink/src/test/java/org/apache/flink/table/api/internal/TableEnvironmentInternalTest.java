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
import org.apache.flink.table.planner.utils.JsonPlanTestBase;
import org.apache.flink.table.planner.utils.TableTestUtil;

import org.junit.Before;
import org.junit.Test;

import java.io.File;
import java.io.IOException;
import java.util.Arrays;
import java.util.List;

import static org.junit.Assert.assertEquals;

/** Test for {@link TableEnvironmentInternal}. */
public class TableEnvironmentInternalTest extends JsonPlanTestBase {

    @Before
    public void setup() throws Exception {
        super.setup();

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
        String expected = TableTestUtil.readFromResource("/jsonplan/testGetJsonPlan.out");
        assertEquals(
                TableTestUtil.replaceExecNodeId(
                        TableTestUtil.replaceFlinkVersion(
                                TableTestUtil.getFormattedJson(expected))),
                TableTestUtil.replaceExecNodeId(
                        TableTestUtil.replaceFlinkVersion(
                                TableTestUtil.getFormattedJson(jsonPlan))));
    }

    @Test
    public void testExecuteJsonPlan() throws Exception {
        List<String> data = Arrays.asList("1,1,hi", "2,1,hello", "3,2,hello world");
        createTestCsvSourceTable("src", data, "a bigint", "b int", "c varchar");
        File sinkPath = createTestCsvSinkTable("sink", "a bigint", "b int", "c varchar");

        String jsonPlan = tableEnv.getJsonPlan("insert into sink select * from src");
        tableEnv.executeJsonPlan(jsonPlan).await();

        assertResult(data, sinkPath);
    }

    @Test
    public void testExplainJsonPlan() {
        String jsonPlan = TableTestUtil.readFromResource("/jsonplan/testGetJsonPlan.out");
        String actual = tableEnv.explainJsonPlan(jsonPlan, ExplainDetail.JSON_EXECUTION_PLAN);
        String expected = TableTestUtil.readFromResource("/explain/testExplainJsonPlan.out");
        assertEquals(expected, TableTestUtil.replaceStreamNodeId(actual));
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
