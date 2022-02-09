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
import org.apache.flink.table.api.TableException;
import org.apache.flink.table.api.TableResult;
import org.apache.flink.table.api.config.TableConfigOptions;
import org.apache.flink.table.planner.utils.JsonPlanTestBase;
import org.apache.flink.table.planner.utils.TableTestUtil;

import org.apache.commons.io.FileUtils;
import org.junit.Before;
import org.junit.Test;

import java.io.File;
import java.io.IOException;
import java.net.URI;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.Arrays;
import java.util.List;

import static org.apache.flink.core.testutils.FlinkAssertions.anyCauseMatches;
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
    public void testExecutePlanSql() throws Exception {
        Path planPath = Paths.get(URI.create(getTempDirPath("plan")).getPath(), "plan.json");
        FileUtils.createParentDirectories(planPath.toFile());

        List<String> data = Arrays.asList("1,1,hi", "2,1,hello", "3,2,hello world");
        createTestCsvSourceTable("src", data, "a bigint", "b int", "c varchar");
        File sinkPath = createTestCsvSinkTable("sink", "a bigint", "b int", "c varchar");

        CompiledPlan plan = tableEnv.compilePlanSql("insert into sink select * from src");
        plan.writeToFile(planPath);

        tableEnv.executeSql(String.format("EXECUTE PLAN '%s'", planPath.toAbsolutePath())).await();

        assertResult(data, sinkPath);
    }

    @Test
    public void testCompilePlan() throws Exception {
        Path planPath =
                Paths.get(URI.create(getTempDirPath("plan")).getPath(), "plan.json")
                        .toAbsolutePath();
        FileUtils.createParentDirectories(planPath.toFile());

        List<String> data = Arrays.asList("1,1,hi", "2,1,hello", "3,2,hello world");
        createTestCsvSourceTable("src", data, "a bigint", "b int", "c varchar");
        File sinkPath = createTestCsvSinkTable("sink", "a bigint", "b int", "c varchar");

        TableResult tableResult =
                tableEnv.executeSql(
                        String.format(
                                "COMPILE PLAN '%s' FOR INSERT INTO sink SELECT * FROM src",
                                planPath));

        assertThat(tableResult).isEqualTo(TableResultInternal.TABLE_RESULT_OK);
        assertThat(planPath.toFile()).exists();

        assertThatThrownBy(
                        () ->
                                tableEnv.executeSql(
                                        String.format(
                                                "COMPILE PLAN '%s' FOR INSERT INTO sink SELECT * FROM src",
                                                planPath)))
                .satisfies(anyCauseMatches(TableException.class, "Cannot overwrite the plan file"));

        tableEnv.executeSql(String.format("EXECUTE PLAN '%s'", planPath)).await();

        assertResult(data, sinkPath);
    }

    @Test
    public void testCompilePlanWithStatementSet() throws Exception {
        Path planPath =
                Paths.get(URI.create(getTempDirPath("plan")).getPath(), "plan.json")
                        .toAbsolutePath();
        FileUtils.createParentDirectories(planPath.toFile());

        List<String> inputData = Arrays.asList("1,1,hi", "2,1,hello", "3,2,hello world");
        createTestCsvSourceTable("src", inputData, "a bigint", "b int", "c varchar");
        File sinkAPath = createTestCsvSinkTable("sinkA", "a bigint", "b int", "c varchar");
        File sinkBPath = createTestCsvSinkTable("sinkB", "a bigint", "b int", "c varchar");

        TableResult tableResult =
                tableEnv.executeSql(
                        String.format(
                                "COMPILE PLAN '%s' FOR STATEMENT SET BEGIN "
                                        + "INSERT INTO sinkA SELECT * FROM src;"
                                        + "INSERT INTO sinkB SELECT a + 1, b + 1, CONCAT(c, '-something') FROM src;"
                                        + "END",
                                planPath));

        assertThat(tableResult).isEqualTo(TableResultInternal.TABLE_RESULT_OK);
        assertThat(planPath.toFile()).exists();

        tableEnv.executeSql(String.format("EXECUTE PLAN '%s'", planPath)).await();

        assertResult(inputData, sinkAPath);
        assertResult(
                Arrays.asList(
                        "2,2,hi-something", "3,2,hello-something", "4,3,hello world-something"),
                sinkBPath);
    }

    @Test
    public void testCompilePlanIfNotExists() throws Exception {
        Path planPath =
                Paths.get(URI.create(getTempDirPath("plan")).getPath(), "plan.json")
                        .toAbsolutePath();
        FileUtils.createParentDirectories(planPath.toFile());

        List<String> data = Arrays.asList("1,1,hi", "2,1,hello", "3,2,hello world");
        createTestCsvSourceTable("src", data, "a bigint", "b int", "c varchar");
        File sinkPath = createTestCsvSinkTable("sink", "a bigint", "b int", "c varchar");

        TableResult tableResult =
                tableEnv.executeSql(
                        String.format(
                                "COMPILE PLAN '%s' IF NOT EXISTS FOR INSERT INTO sink SELECT * FROM src",
                                planPath));

        assertThat(tableResult).isEqualTo(TableResultInternal.TABLE_RESULT_OK);
        assertThat(planPath.toFile()).exists();

        // This should not mutate the plan, as it already exists
        assertThat(
                        tableEnv.executeSql(
                                String.format(
                                        "COMPILE PLAN '%s' IF NOT EXISTS FOR INSERT INTO sink SELECT a + 1, b + 1, CONCAT(c, '-something') FROM src",
                                        planPath)))
                .isEqualTo(TableResultInternal.TABLE_RESULT_OK);

        tableEnv.executeSql(String.format("EXECUTE PLAN '%s'", planPath)).await();

        assertResult(data, sinkPath);
    }

    @Test
    public void testCompilePlanOverwrite() throws Exception {
        tableEnv.getConfig().getConfiguration().set(TableConfigOptions.PLAN_FORCE_RECOMPILE, true);

        Path planPath =
                Paths.get(URI.create(getTempDirPath("plan")).getPath(), "plan.json")
                        .toAbsolutePath();
        FileUtils.createParentDirectories(planPath.toFile());

        List<String> inputData = Arrays.asList("1,1,hi", "2,1,hello", "3,2,hello world");
        List<String> expectedData =
                Arrays.asList(
                        "2,2,hi-something", "3,2,hello-something", "4,3,hello world-something");
        createTestCsvSourceTable("src", inputData, "a bigint", "b int", "c varchar");
        File sinkPath = createTestCsvSinkTable("sink", "a bigint", "b int", "c varchar");

        TableResult tableResult =
                tableEnv.executeSql(
                        String.format(
                                "COMPILE PLAN '%s' FOR INSERT INTO sink SELECT * FROM src",
                                planPath));

        assertThat(tableResult).isEqualTo(TableResultInternal.TABLE_RESULT_OK);
        assertThat(planPath.toFile()).exists();

        // This should overwrite the plan
        assertThat(
                        tableEnv.executeSql(
                                String.format(
                                        "COMPILE PLAN '%s' FOR INSERT INTO sink SELECT a + 1, b + 1, CONCAT(c, '-something') FROM src",
                                        planPath)))
                .isEqualTo(TableResultInternal.TABLE_RESULT_OK);

        tableEnv.executeSql(String.format("EXECUTE PLAN '%s'", planPath)).await();

        assertResult(expectedData, sinkPath);
    }

    @Test
    public void testCompileAndExecutePlan() throws Exception {
        Path planPath =
                Paths.get(URI.create(getTempDirPath("plan")).getPath(), "plan.json")
                        .toAbsolutePath();
        FileUtils.createParentDirectories(planPath.toFile());

        List<String> data = Arrays.asList("1,1,hi", "2,1,hello", "3,2,hello world");
        createTestCsvSourceTable("src", data, "a bigint", "b int", "c varchar");
        File sinkPath = createTestCsvSinkTable("sink", "a bigint", "b int", "c varchar");

        tableEnv.executeSql(
                        String.format(
                                "COMPILE AND EXECUTE PLAN '%s' FOR INSERT INTO sink SELECT * FROM src",
                                planPath))
                .await();

        assertThat(planPath.toFile()).exists();

        assertResult(data, sinkPath);
    }

    @Test
    public void testCompileAndExecutePlanWithStatementSet() throws Exception {
        Path planPath =
                Paths.get(URI.create(getTempDirPath("plan")).getPath(), "plan.json")
                        .toAbsolutePath();
        FileUtils.createParentDirectories(planPath.toFile());

        List<String> inputData = Arrays.asList("1,1,hi", "2,1,hello", "3,2,hello world");
        createTestCsvSourceTable("src", inputData, "a bigint", "b int", "c varchar");
        File sinkAPath = createTestCsvSinkTable("sinkA", "a bigint", "b int", "c varchar");
        File sinkBPath = createTestCsvSinkTable("sinkB", "a bigint", "b int", "c varchar");

        tableEnv.executeSql(
                        String.format(
                                "COMPILE AND EXECUTE PLAN '%s' FOR STATEMENT SET BEGIN "
                                        + "INSERT INTO sinkA SELECT * FROM src;"
                                        + "INSERT INTO sinkB SELECT a + 1, b + 1, CONCAT(c, '-something') FROM src;"
                                        + "END",
                                planPath))
                .await();

        assertThat(planPath.toFile()).exists();

        assertResult(inputData, sinkAPath);
        assertResult(
                Arrays.asList(
                        "2,2,hi-something", "3,2,hello-something", "4,3,hello world-something"),
                sinkBPath);
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
                .hasMessage("The compiled plan feature is not supported in batch mode.");
    }
}
