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

import org.apache.flink.table.api.CompiledPlan;
import org.apache.flink.table.api.EnvironmentSettings;
import org.apache.flink.table.api.ExplainDetail;
import org.apache.flink.table.api.PlanReference;
import org.apache.flink.table.api.TableEnvironment;
import org.apache.flink.table.planner.utils.JsonPlanTestBase;
import org.apache.flink.table.planner.utils.TableTestUtil;

import org.junit.Before;
import org.junit.Test;

import java.io.File;
import java.io.IOException;
import java.util.Arrays;
import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

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
    public void testCompilePlanSql() throws IOException {
        CompiledPlan compiledPlan =
                tableEnv.compilePlanSql("insert into MySink select * from MyTable");
        String expected = TableTestUtil.readFromResource("/jsonplan/testGetJsonPlan.out");
        assertThat(
                        TableTestUtil.replaceExecNodeId(
                                TableTestUtil.replaceFlinkVersion(
                                        TableTestUtil.getFormattedJson(
                                                compiledPlan.asJsonString()))))
                .isEqualTo(
                        TableTestUtil.replaceExecNodeId(
                                TableTestUtil.replaceFlinkVersion(
                                        TableTestUtil.getFormattedJson(expected))));
    }

    @Test
    public void testExecutePlan() throws Exception {
        List<String> data = Arrays.asList("1,1,hi", "2,1,hello", "3,2,hello world");
        createTestCsvSourceTable("src", data, "a bigint", "b int", "c varchar");
        File sinkPath = createTestCsvSinkTable("sink", "a bigint", "b int", "c varchar");

        CompiledPlan plan = tableEnv.compilePlanSql("insert into sink select * from src");
        tableEnv.executePlan(plan).await();

        assertResult(data, sinkPath);
    }

    @Test
    public void testExplainPlan() throws IOException {
        String actual =
                tableEnv.explainPlan(
                        tableEnv.loadPlan(
                                PlanReference.fromResource("/jsonplan/testGetJsonPlan.out")),
                        ExplainDetail.JSON_EXECUTION_PLAN);
        String expected = TableTestUtil.readFromResource("/explain/testExplainJsonPlan.out");
        assertThat(TableTestUtil.replaceNodeIdInOperator(TableTestUtil.replaceStreamNodeId(actual)))
                .isEqualTo(expected);
    }

    @Test
    public void testBatchMode() {
        tableEnv =
                (TableEnvironmentInternal)
                        TableEnvironment.create(EnvironmentSettings.inBatchMode());

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

        assertThatThrownBy(() -> tableEnv.compilePlanSql("insert into sink select * from src"))
                .isInstanceOf(UnsupportedOperationException.class)
                .hasMessage("The batch planner doesn't support the persisted plan feature.");
    }
}
