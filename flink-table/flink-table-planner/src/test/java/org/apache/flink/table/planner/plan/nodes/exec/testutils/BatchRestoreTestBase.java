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

package org.apache.flink.table.planner.plan.nodes.exec.testutils;

import org.apache.flink.table.api.CompiledPlan;
import org.apache.flink.table.api.EnvironmentSettings;
import org.apache.flink.table.api.PlanReference;
import org.apache.flink.table.api.TableEnvironment;
import org.apache.flink.table.api.config.TableConfigOptions;
import org.apache.flink.table.planner.factories.TestValuesTableFactory;
import org.apache.flink.table.planner.plan.nodes.exec.ExecNode;
import org.apache.flink.table.planner.plan.nodes.exec.ExecNodeMetadata;
import org.apache.flink.table.planner.plan.nodes.exec.batch.BatchExecNode;
import org.apache.flink.table.planner.plan.utils.ExecNodeMetadataUtil;
import org.apache.flink.table.test.program.SinkTestStep;
import org.apache.flink.table.test.program.SourceTestStep;
import org.apache.flink.table.test.program.SqlTestStep;
import org.apache.flink.table.test.program.StatementSetTestStep;
import org.apache.flink.table.test.program.TableTestProgram;
import org.apache.flink.table.test.program.TableTestProgramRunner;
import org.apache.flink.table.test.program.TestStep.TestKind;
import org.apache.flink.test.junit5.MiniClusterExtension;
import org.apache.flink.types.Row;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.MethodOrderer.OrderAnnotation;
import org.junit.jupiter.api.Order;
import org.junit.jupiter.api.TestInstance;
import org.junit.jupiter.api.TestMethodOrder;
import org.junit.jupiter.api.extension.ExtendWith;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;

import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.EnumSet;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Stream;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Base class for implementing compiled plan tests for {@link BatchExecNode}. You can generate json
 * compiled plan for the latest node version by running {@link
 * BatchRestoreTestBase#generateCompiledPlans(TableTestProgram)}. This method does not recreate the
 * compiled plan if it already exists for the given version of the operator.
 *
 * <p><b>Note:</b> The test base uses {@link TableConfigOptions.CatalogPlanCompilation#SCHEMA}
 * because it needs to adjust source and sink properties. Therefore, the test base can not be used
 * for testing storing table options in the compiled plan.
 */
@ExtendWith(MiniClusterExtension.class)
@TestInstance(TestInstance.Lifecycle.PER_CLASS)
@TestMethodOrder(OrderAnnotation.class)
public abstract class BatchRestoreTestBase implements TableTestProgramRunner {

    private final Class<? extends ExecNode<?>> execNodeUnderTest;
    private final List<Class<? extends ExecNode<?>>> childExecNodesUnderTest;

    protected BatchRestoreTestBase(Class<? extends ExecNode<?>> execNodeUnderTest) {
        this(execNodeUnderTest, new ArrayList<>());
    }

    protected BatchRestoreTestBase(
            Class<? extends ExecNode<?>> execNodeUnderTest,
            List<Class<? extends ExecNode<?>>> childExecNodesUnderTest) {
        this.execNodeUnderTest = execNodeUnderTest;
        this.childExecNodesUnderTest = childExecNodesUnderTest;
    }

    // Used for testing Restore Test Completeness
    public Class<? extends ExecNode<?>> getExecNode() {
        return execNodeUnderTest;
    }

    // Used for testing Restore Test Completeness
    public List<Class<? extends ExecNode<?>>> getChildExecNodes() {
        return childExecNodesUnderTest;
    }

    @Override
    public EnumSet<TestKind> supportedSetupSteps() {
        return EnumSet.of(
                TestKind.CONFIG,
                TestKind.FUNCTION,
                TestKind.SOURCE_WITH_RESTORE_DATA,
                TestKind.SOURCE_WITH_DATA,
                TestKind.SINK_WITH_RESTORE_DATA,
                TestKind.SINK_WITH_DATA);
    }

    @Override
    public EnumSet<TestKind> supportedRunSteps() {
        return EnumSet.of(TestKind.SQL, TestKind.STATEMENT_SET);
    }

    @AfterEach
    public void clearData() {
        TestValuesTableFactory.clearAllData();
    }

    private List<ExecNodeMetadata> getAllMetadata() {
        return ExecNodeMetadataUtil.extractMetadataFromAnnotation(execNodeUnderTest);
    }

    private ExecNodeMetadata getLatestMetadata() {
        return ExecNodeMetadataUtil.latestAnnotation(execNodeUnderTest);
    }

    private Stream<Arguments> createSpecs() {
        return getAllMetadata().stream()
                .flatMap(
                        metadata ->
                                supportedPrograms().stream().map(p -> Arguments.of(p, metadata)));
    }

    /** Generates compiled plans for a given TableTestProgram. */
    @ParameterizedTest
    @MethodSource("supportedPrograms")
    @Order(0)
    public void generateCompiledPlans(TableTestProgram program) {
        Path path = getPlanPath(program, getLatestMetadata());
        if (path.toFile().exists()) {
            return;
        }

        final EnvironmentSettings settings = EnvironmentSettings.inBatchMode();
        final TableEnvironment tEnv = TableEnvironment.create(settings);
        program.getSetupConfigOptionTestSteps().forEach(s -> s.apply(tEnv));
        tEnv.getConfig()
                .set(
                        TableConfigOptions.PLAN_COMPILE_CATALOG_OBJECTS,
                        TableConfigOptions.CatalogPlanCompilation.SCHEMA);

        for (SourceTestStep sourceTestStep : program.getSetupSourceTestSteps()) {
            final String id = TestValuesTableFactory.registerData(sourceTestStep.dataBeforeRestore);
            final Map<String, String> options = new HashMap<>();
            options.put("connector", "values");
            options.put("data-id", id);
            options.put("bounded", "true");
            options.put("terminating", "true");
            options.put("runtime-source", "NewSource");
            sourceTestStep.apply(tEnv, options);
        }

        for (SinkTestStep sinkTestStep : program.getSetupSinkTestSteps()) {
            final Map<String, String> options = new HashMap<>();
            options.put("connector", "values");
            options.put("sink-insert-only", "false");
            sinkTestStep.apply(tEnv, options);
        }

        program.getSetupFunctionTestSteps().forEach(s -> s.apply(tEnv));

        final CompiledPlan compiledPlan;
        if (program.runSteps.get(0).getKind() == TestKind.STATEMENT_SET) {
            final StatementSetTestStep statementSetTestStep = program.getRunStatementSetTestStep();
            compiledPlan = statementSetTestStep.compiledPlan(tEnv);
        } else {
            final SqlTestStep sqlTestStep = program.getRunSqlTestStep();
            compiledPlan = tEnv.compilePlanSql(sqlTestStep.sql);
        }

        compiledPlan.writeToFile(path);
    }

    @ParameterizedTest
    @MethodSource("createSpecs")
    @Order(1)
    void loadAndRunCompiledPlan(TableTestProgram program, ExecNodeMetadata metadata)
            throws Exception {
        final EnvironmentSettings settings = EnvironmentSettings.inBatchMode();
        final TableEnvironment tEnv = TableEnvironment.create(settings);
        tEnv.getConfig()
                .set(
                        TableConfigOptions.PLAN_RESTORE_CATALOG_OBJECTS,
                        TableConfigOptions.CatalogPlanRestore.IDENTIFIER);

        program.getSetupConfigOptionTestSteps().forEach(s -> s.apply(tEnv));

        for (SourceTestStep sourceTestStep : program.getSetupSourceTestSteps()) {

            List<Row> data = new ArrayList<>();
            data.addAll(sourceTestStep.dataBeforeRestore);
            data.addAll(sourceTestStep.dataAfterRestore);
            final String id = TestValuesTableFactory.registerData(data);
            final Map<String, String> options = new HashMap<>();
            options.put("connector", "values");
            options.put("data-id", id);
            options.put("runtime-source", "NewSource");
            options.put("terminating", "true");
            options.put("bounded", "true");
            sourceTestStep.apply(tEnv, options);
        }

        for (SinkTestStep sinkTestStep : program.getSetupSinkTestSteps()) {
            final Map<String, String> options = new HashMap<>();
            options.put("connector", "values");
            options.put("disable-lookup", "true");
            options.put("sink-insert-only", "false");
            sinkTestStep.apply(tEnv, options);
        }

        program.getSetupFunctionTestSteps().forEach(s -> s.apply(tEnv));

        final CompiledPlan compiledPlan =
                tEnv.loadPlan(PlanReference.fromFile(getPlanPath(program, metadata)));

        compiledPlan.execute().await();
        for (SinkTestStep sinkTestStep : program.getSetupSinkTestSteps()) {
            List<String> expectedResults = getExpectedResults(sinkTestStep, sinkTestStep.name);
            assertThat(expectedResults)
                    .containsExactlyInAnyOrder(
                            sinkTestStep.getExpectedAsStrings().toArray(new String[0]));
        }
    }

    private Path getPlanPath(TableTestProgram program, ExecNodeMetadata metadata) {
        return Paths.get(
                getTestResourceDirectory(program, metadata) + "/plan/" + program.id + ".json");
    }

    private String getTestResourceDirectory(TableTestProgram program, ExecNodeMetadata metadata) {
        return String.format(
                "%s/src/test/resources/restore-tests/%s_%d/%s",
                System.getProperty("user.dir"), metadata.name(), metadata.version(), program.id);
    }

    private static List<String> getExpectedResults(SinkTestStep sinkTestStep, String tableName) {
        if (sinkTestStep.shouldTestChangelogData()) {
            return TestValuesTableFactory.getRawResultsAsStrings(tableName);
        } else {
            return TestValuesTableFactory.getResultsAsStrings(tableName);
        }
    }
}
