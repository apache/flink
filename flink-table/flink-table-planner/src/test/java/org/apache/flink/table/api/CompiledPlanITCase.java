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

package org.apache.flink.table.api;

import org.apache.flink.FlinkVersion;
import org.apache.flink.table.api.config.ExecutionConfigOptions;
import org.apache.flink.table.api.config.TableConfigOptions;
import org.apache.flink.table.api.internal.TableResultInternal;
import org.apache.flink.table.planner.utils.JsonPlanTestBase;
import org.apache.flink.table.planner.utils.JsonTestUtils;
import org.apache.flink.table.planner.utils.TableTestUtil;
import org.apache.flink.testutils.junit.utils.TempDirUtils;

import org.apache.commons.io.FileUtils;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.io.File;
import java.io.IOException;
import java.net.URI;
import java.nio.charset.StandardCharsets;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.Arrays;
import java.util.List;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import static org.apache.flink.core.testutils.FlinkAssertions.anyCauseMatches;
import static org.apache.flink.table.api.Expressions.$;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/** Test for {@link CompiledPlan} and related {@link TableEnvironment} methods. */
class CompiledPlanITCase extends JsonPlanTestBase {

    private static final List<String> DATA =
            Arrays.asList("1,1,hi", "2,1,hello", "3,2,hello world");
    private static final String[] COLUMNS_DEFINITION =
            new String[] {"a bigint", "b int", "c varchar"};

    @BeforeEach
    @Override
    protected void setup() throws Exception {
        super.setup();

        String srcTableDdl =
                "CREATE TABLE MyTable (\n"
                        + String.join(",", COLUMNS_DEFINITION)
                        + ") with (\n"
                        + "  'connector' = 'values',\n"
                        + "  'bounded' = 'false')";
        tableEnv.executeSql(srcTableDdl);

        String sinkTableDdl =
                "CREATE TABLE MySink (\n"
                        + String.join(",", COLUMNS_DEFINITION)
                        + ") with (\n"
                        + "  'connector' = 'values',\n"
                        + "  'table-sink-class' = 'DEFAULT')";
        tableEnv.executeSql(sinkTableDdl);
    }

    @Test
    void testCompilePlanSql() throws IOException {
        CompiledPlan compiledPlan =
                tableEnv.compilePlanSql("INSERT INTO MySink SELECT * FROM MyTable");
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
    void testExecutePlanSql() throws Exception {
        File sinkPath = createSourceSinkTables();

        tableEnv.compilePlanSql("INSERT INTO sink SELECT * FROM src").execute().await();

        assertResult(DATA, sinkPath);
    }

    @Test
    void testExecuteCtasPlanSql() throws Exception {
        createTestCsvSourceTable("src", DATA, COLUMNS_DEFINITION);

        File sinkPath = TempDirUtils.newFolder(tempFolder);
        assertThatThrownBy(
                        () ->
                                tableEnv.compilePlanSql(
                                                String.format(
                                                        "CREATE TABLE sink\n"
                                                                + "WITH (\n"
                                                                + "  'connector' = 'filesystem',\n"
                                                                + "  'format' = 'testcsv',\n"
                                                                + "  'path' = '%s'\n"
                                                                + ") AS SELECT * FROM src",
                                                        sinkPath.getAbsolutePath()))
                                        .execute())
                .satisfies(
                        anyCauseMatches(
                                TableException.class,
                                "Unsupported SQL query! compilePlanSql() only accepts a single SQL statement"
                                        + " of type INSERT"));
    }

    @Test
    void testExecutePlanTable() throws Exception {
        File sinkPath = createSourceSinkTables();

        tableEnv.from("src").select($("*")).insertInto("sink").compilePlan().execute().await();

        assertResult(DATA, sinkPath);
    }

    @Test
    void testCompileWriteToFileAndThenExecuteSql() throws Exception {
        Path planPath =
                Paths.get(TempDirUtils.newFolder(tempFolder, "plan").getPath(), "plan.json");

        File sinkPath = createSourceSinkTables();

        CompiledPlan plan = tableEnv.compilePlanSql("INSERT INTO sink SELECT * FROM src");
        plan.writeToFile(planPath);

        tableEnv.executeSql(String.format("EXECUTE PLAN '%s'", planPath.toAbsolutePath())).await();

        assertResult(DATA, sinkPath);
    }

    @Test
    void testCompileWriteToFilePathWithSchemeAndThenExecuteSql() throws Exception {
        Path planPath =
                Paths.get(TempDirUtils.newFolder(tempFolder, "plan").getPath(), "plan.json");

        File sinkPath = createSourceSinkTables();

        tableEnv.executeSql(
                String.format(
                        "COMPILE PLAN 'file://%s' FOR INSERT INTO sink SELECT * FROM src",
                        planPath.toAbsolutePath()));

        tableEnv.executeSql(String.format("EXECUTE PLAN 'file://%s'", planPath.toAbsolutePath()))
                .await();

        assertResult(DATA, sinkPath);
    }

    @Test
    void testCompilePlan() throws Exception {
        Path planPath =
                Paths.get(TempDirUtils.newFolder(tempFolder, "plan").getPath(), "plan.json")
                        .toAbsolutePath();

        File sinkPath = createSourceSinkTables();

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

        assertResult(DATA, sinkPath);
    }

    @Test
    void testCompilePlanWithStatementSet() throws Exception {
        Path planPath =
                Paths.get(TempDirUtils.newFolder(tempFolder, "plan").getPath(), "plan.json")
                        .toAbsolutePath();

        createTestCsvSourceTable("src", DATA, COLUMNS_DEFINITION);
        File sinkAPath = createTestCsvSinkTable("sinkA", COLUMNS_DEFINITION);
        File sinkBPath = createTestCsvSinkTable("sinkB", COLUMNS_DEFINITION);

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

        assertResult(DATA, sinkAPath);
        assertResult(
                Arrays.asList(
                        "2,2,hi-something", "3,2,hello-something", "4,3,hello world-something"),
                sinkBPath);
    }

    @Test
    void testCompilePlanIfNotExists() throws Exception {
        Path planPath =
                Paths.get(TempDirUtils.newFolder(tempFolder, "plan").getPath(), "plan.json")
                        .toAbsolutePath();

        File sinkPath = createSourceSinkTables();

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

        assertResult(DATA, sinkPath);
    }

    @Test
    void testCompilePlanOverwrite() throws Exception {
        tableEnv.getConfig().set(TableConfigOptions.PLAN_FORCE_RECOMPILE, true);

        Path planPath =
                Paths.get(
                                URI.create(TempDirUtils.newFolder(tempFolder, "plan").getPath())
                                        .getPath(),
                                "plan.json")
                        .toAbsolutePath();

        List<String> expectedData =
                Arrays.asList(
                        "2,2,hi-something", "3,2,hello-something", "4,3,hello world-something");
        File sinkPath = createSourceSinkTables();

        TableResult tableResult =
                tableEnv.executeSql(
                        String.format(
                                "COMPILE PLAN '%s' FOR INSERT INTO sink "
                                        + "SELECT IF(a > b, a, b) AS a, b + 1 AS b, SUBSTR(c, 1, 4) AS c FROM src WHERE a > 10",
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
        assertThat(
                        TableTestUtil.isValidJson(
                                FileUtils.readFileToString(
                                        planPath.toFile(), StandardCharsets.UTF_8)))
                .isTrue();

        tableEnv.executeSql(String.format("EXECUTE PLAN '%s'", planPath)).await();

        assertResult(expectedData, sinkPath);
    }

    @Test
    void testCompileAndExecutePlan() throws Exception {
        Path planPath =
                Paths.get(TempDirUtils.newFolder(tempFolder, "plan").getPath(), "plan.json")
                        .toAbsolutePath();

        File sinkPath = createSourceSinkTables();

        tableEnv.executeSql(
                        String.format(
                                "COMPILE AND EXECUTE PLAN '%s' FOR INSERT INTO sink SELECT * FROM src",
                                planPath))
                .await();

        assertThat(planPath.toFile()).exists();

        assertResult(DATA, sinkPath);
    }

    @Test
    void testCompileAndExecutePlanWithStatementSet() throws Exception {
        Path planPath =
                Paths.get(TempDirUtils.newFolder(tempFolder, "plan").getPath(), "plan.json")
                        .toAbsolutePath();

        createTestCsvSourceTable("src", DATA, COLUMNS_DEFINITION);
        File sinkAPath = createTestCsvSinkTable("sinkA", COLUMNS_DEFINITION);
        File sinkBPath = createTestCsvSinkTable("sinkB", COLUMNS_DEFINITION);

        tableEnv.executeSql(
                        String.format(
                                "COMPILE AND EXECUTE PLAN '%s' FOR STATEMENT SET BEGIN "
                                        + "INSERT INTO sinkA SELECT * FROM src;"
                                        + "INSERT INTO sinkB SELECT a + 1, b + 1, CONCAT(c, '-something') FROM src;"
                                        + "END",
                                planPath))
                .await();

        assertThat(planPath.toFile()).exists();

        assertResult(DATA, sinkAPath);
        assertResult(
                Arrays.asList(
                        "2,2,hi-something", "3,2,hello-something", "4,3,hello world-something"),
                sinkBPath);
    }

    @Test
    void testExplainPlan() throws IOException {
        String planFromResources =
                JsonTestUtils.setFlinkVersion(
                                JsonTestUtils.readFromResource("/jsonplan/testGetJsonPlan.out"),
                                FlinkVersion.current())
                        .toString();

        String actual =
                tableEnv.loadPlan(PlanReference.fromJsonString(planFromResources))
                        .explain(ExplainDetail.JSON_EXECUTION_PLAN);
        String expected = TableTestUtil.readFromResource("/explain/testExplainJsonPlan.out");
        assertThat(TableTestUtil.replaceNodeIdInOperator(TableTestUtil.replaceStreamNodeId(actual)))
                .isEqualTo(expected);
    }

    @Test
    void testPersistedConfigOption() throws Exception {
        List<String> data =
                Stream.concat(
                                DATA.stream(),
                                Stream.of(
                                        "4,2,This string is long",
                                        "5,3,This is an even longer string"))
                        .collect(Collectors.toList());
        String[] sinkColumnDefinitions = new String[] {"a bigint", "b int", "c varchar(11)"};

        createTestCsvSourceTable("src", data, COLUMNS_DEFINITION);
        File sinkPath = createTestCsvSinkTable("sink", sinkColumnDefinitions);

        // Set config option to trim the strings, so it's persisted in the json plan
        tableEnv.getConfig()
                .getConfiguration()
                .set(
                        ExecutionConfigOptions.TABLE_EXEC_SINK_TYPE_LENGTH_ENFORCER,
                        ExecutionConfigOptions.TypeLengthEnforcer.TRIM_PAD);
        CompiledPlan plan = tableEnv.compilePlanSql("INSERT INTO sink SELECT * FROM src");

        // Set config option to trim the strings to IGNORE, to validate that the persisted config
        // is overriding the environment setting.
        tableEnv.getConfig()
                .getConfiguration()
                .set(
                        ExecutionConfigOptions.TABLE_EXEC_SINK_TYPE_LENGTH_ENFORCER,
                        ExecutionConfigOptions.TypeLengthEnforcer.IGNORE);

        plan.execute().await();
        List<String> expected =
                Stream.concat(DATA.stream(), Stream.of("4,2,This string", "5,3,This is an "))
                        .collect(Collectors.toList());
        assertResult(expected, sinkPath);
    }

    @Test
    void testBatchMode() {
        tableEnv = TableEnvironment.create(EnvironmentSettings.inBatchMode());

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

        assertThatThrownBy(() -> tableEnv.compilePlanSql("INSERT INTO sink SELECT * FROM src"))
                .isInstanceOf(UnsupportedOperationException.class)
                .hasMessage("The compiled plan feature is not supported in batch mode.");
    }

    private File createSourceSinkTables() throws IOException {
        createTestCsvSourceTable("src", DATA, COLUMNS_DEFINITION);
        return createTestCsvSinkTable("sink", COLUMNS_DEFINITION);
    }
}
