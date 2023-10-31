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

import org.apache.flink.api.common.JobStatus;
import org.apache.flink.core.execution.JobClient;
import org.apache.flink.core.execution.SavepointFormatType;
import org.apache.flink.runtime.jobgraph.RestoreMode;
import org.apache.flink.runtime.jobgraph.SavepointRestoreSettings;
import org.apache.flink.runtime.testutils.CommonTestUtils;
import org.apache.flink.table.api.CompiledPlan;
import org.apache.flink.table.api.EnvironmentSettings;
import org.apache.flink.table.api.PlanReference;
import org.apache.flink.table.api.TableEnvironment;
import org.apache.flink.table.api.TableResult;
import org.apache.flink.table.api.config.TableConfigOptions;
import org.apache.flink.table.planner.factories.TestValuesTableFactory;
import org.apache.flink.table.planner.plan.nodes.exec.ExecNode;
import org.apache.flink.table.planner.plan.nodes.exec.ExecNodeMetadata;
import org.apache.flink.table.planner.plan.utils.ExecNodeMetadataUtil;
import org.apache.flink.table.test.program.SinkTestStep;
import org.apache.flink.table.test.program.SourceTestStep;
import org.apache.flink.table.test.program.SqlTestStep;
import org.apache.flink.table.test.program.TableTestProgram;
import org.apache.flink.table.test.program.TableTestProgramRunner;
import org.apache.flink.table.test.program.TestStep.TestKind;
import org.apache.flink.test.junit5.MiniClusterExtension;

import org.apache.commons.collections.CollectionUtils;
import org.junit.jupiter.api.Disabled;
import org.junit.jupiter.api.TestInstance;
import org.junit.jupiter.api.extension.ExtendWith;
import org.junit.jupiter.api.io.TempDir;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;

import java.net.URI;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.nio.file.StandardCopyOption;
import java.util.ArrayList;
import java.util.Collections;
import java.util.EnumSet;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.stream.Stream;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Base class for implementing restore tests for {@link ExecNode}.You can generate json compiled
 * plan and a savepoint for the latest node version by running {@link
 * RestoreTestBase#generateTestSetupFiles(TableTestProgram)} which is disabled by default.
 *
 * <p><b>Note:</b> The test base uses {@link TableConfigOptions.CatalogPlanCompilation#SCHEMA}
 * because it needs to adjust source and sink properties before and after the restore. Therefore,
 * the test base can not be used for testing storing table options in the compiled plan.
 */
@ExtendWith(MiniClusterExtension.class)
@TestInstance(TestInstance.Lifecycle.PER_CLASS)
public abstract class RestoreTestBase implements TableTestProgramRunner {

    private final Class<? extends ExecNode> execNodeUnderTest;

    protected RestoreTestBase(Class<? extends ExecNode> execNodeUnderTest) {
        this.execNodeUnderTest = execNodeUnderTest;
    }

    @Override
    public EnumSet<TestKind> supportedSetupSteps() {
        return EnumSet.of(
                TestKind.FUNCTION,
                TestKind.SOURCE_WITH_RESTORE_DATA,
                TestKind.SINK_WITH_RESTORE_DATA);
    }

    @Override
    public EnumSet<TestKind> supportedRunSteps() {
        return EnumSet.of(TestKind.SQL);
    }

    private @TempDir Path tmpDir;

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

    /**
     * Execute this test to generate test files. Remember to be using the correct branch when
     * generating the test files.
     */
    @Disabled
    @ParameterizedTest
    @MethodSource("supportedPrograms")
    public void generateTestSetupFiles(TableTestProgram program) throws Exception {
        final TableEnvironment tEnv =
                TableEnvironment.create(EnvironmentSettings.inStreamingMode());
        tEnv.getConfig()
                .set(
                        TableConfigOptions.PLAN_COMPILE_CATALOG_OBJECTS,
                        TableConfigOptions.CatalogPlanCompilation.SCHEMA);
        for (SourceTestStep sourceTestStep : program.getSetupSourceTestSteps()) {
            final String id = TestValuesTableFactory.registerData(sourceTestStep.dataBeforeRestore);
            final Map<String, String> options = new HashMap<>();
            options.put("connector", "values");
            options.put("data-id", id);
            options.put("terminating", "false");
            options.put("disable-lookup", "true");
            options.put("runtime-source", "NewSource");
            sourceTestStep.apply(tEnv, options);
        }

        final List<CompletableFuture<?>> futures = new ArrayList<>();
        for (SinkTestStep sinkTestStep : program.getSetupSinkTestSteps()) {
            final CompletableFuture<Object> future = new CompletableFuture<>();
            futures.add(future);
            final String tableName = sinkTestStep.name;
            TestValuesTableFactory.registerLocalRawResultsObserver(
                    tableName,
                    (integer, strings) -> {
                        final boolean shouldTakeSavepoint =
                                CollectionUtils.isEqualCollection(
                                        TestValuesTableFactory.getRawResultsAsStrings(tableName),
                                        sinkTestStep.getExpectedBeforeRestoreAsStrings());
                        if (shouldTakeSavepoint) {
                            future.complete(null);
                        }
                    });
            final Map<String, String> options = new HashMap<>();
            options.put("connector", "values");
            options.put("disable-lookup", "true");
            options.put("sink-insert-only", "false");
            sinkTestStep.apply(tEnv, options);
        }

        program.getSetupFunctionTestSteps().forEach(s -> s.apply(tEnv));

        final SqlTestStep sqlTestStep = program.getRunSqlTestStep();

        final CompiledPlan compiledPlan = tEnv.compilePlanSql(sqlTestStep.sql);
        compiledPlan.writeToFile(getPlanPath(program, getLatestMetadata()));

        final TableResult tableResult = compiledPlan.execute();
        CompletableFuture.allOf(futures.toArray(new CompletableFuture[0])).get();
        final JobClient jobClient = tableResult.getJobClient().get();
        final String savepoint =
                jobClient
                        .stopWithSavepoint(false, tmpDir.toString(), SavepointFormatType.DEFAULT)
                        .get();
        CommonTestUtils.waitForJobStatus(jobClient, Collections.singletonList(JobStatus.FINISHED));
        final Path savepointPath = Paths.get(new URI(savepoint));
        final Path savepointDirPath = getSavepointPath(program, getLatestMetadata());
        Files.createDirectories(savepointDirPath);
        Files.move(savepointPath, savepointDirPath, StandardCopyOption.ATOMIC_MOVE);
    }

    @ParameterizedTest
    @MethodSource("createSpecs")
    void testRestore(TableTestProgram program, ExecNodeMetadata metadata) throws Exception {
        final EnvironmentSettings settings = EnvironmentSettings.inStreamingMode();
        final SavepointRestoreSettings restoreSettings =
                SavepointRestoreSettings.forPath(
                        getSavepointPath(program, metadata).toString(),
                        false,
                        RestoreMode.NO_CLAIM);
        SavepointRestoreSettings.toConfiguration(restoreSettings, settings.getConfiguration());
        final TableEnvironment tEnv = TableEnvironment.create(settings);
        tEnv.getConfig()
                .set(
                        TableConfigOptions.PLAN_RESTORE_CATALOG_OBJECTS,
                        TableConfigOptions.CatalogPlanRestore.IDENTIFIER);
        for (SourceTestStep sourceTestStep : program.getSetupSourceTestSteps()) {
            final String id = TestValuesTableFactory.registerData(sourceTestStep.dataAfterRestore);
            final Map<String, String> options = new HashMap<>();
            options.put("connector", "values");
            options.put("data-id", id);
            options.put("disable-lookup", "true");
            options.put("runtime-source", "NewSource");
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
            assertThat(TestValuesTableFactory.getRawResultsAsStrings(sinkTestStep.name))
                    .containsExactlyInAnyOrder(
                            Stream.concat(
                                            sinkTestStep.getExpectedBeforeRestoreAsStrings()
                                                    .stream(),
                                            sinkTestStep.getExpectedAfterRestoreAsStrings()
                                                    .stream())
                                    .toArray(String[]::new));
        }
    }

    private Path getPlanPath(TableTestProgram program, ExecNodeMetadata metadata) {
        return Paths.get(
                getTestResourceDirectory(program, metadata) + "/plan/" + program.id + ".json");
    }

    private Path getSavepointPath(TableTestProgram program, ExecNodeMetadata metadata) {
        return Paths.get(getTestResourceDirectory(program, metadata) + "/savepoint/");
    }

    private String getTestResourceDirectory(TableTestProgram program, ExecNodeMetadata metadata) {
        return String.format(
                "%s/src/test/resources/restore-tests/%s_%d/%s",
                System.getProperty("user.dir"), metadata.name(), metadata.version(), program.id);
    }
}
