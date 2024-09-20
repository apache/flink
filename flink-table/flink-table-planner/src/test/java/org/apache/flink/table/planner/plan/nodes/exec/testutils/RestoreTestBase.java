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

import org.apache.flink.FlinkVersion;
import org.apache.flink.api.common.JobStatus;
import org.apache.flink.configuration.StateBackendOptions;
import org.apache.flink.core.execution.JobClient;
import org.apache.flink.core.execution.RestoreMode;
import org.apache.flink.core.execution.SavepointFormatType;
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
import org.apache.flink.table.test.program.StatementSetTestStep;
import org.apache.flink.table.test.program.TableTestProgram;
import org.apache.flink.table.test.program.TableTestProgramRunner;
import org.apache.flink.table.test.program.TestStep.TestKind;
import org.apache.flink.test.junit5.MiniClusterExtension;
import org.apache.flink.types.Row;

import org.apache.commons.collections.CollectionUtils;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Disabled;
import org.junit.jupiter.api.MethodOrderer.OrderAnnotation;
import org.junit.jupiter.api.Order;
import org.junit.jupiter.api.TestInstance;
import org.junit.jupiter.api.TestMethodOrder;
import org.junit.jupiter.api.extension.ExtendWith;
import org.junit.jupiter.api.io.TempDir;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;

import javax.annotation.Nullable;

import java.net.URI;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.nio.file.StandardCopyOption;
import java.util.ArrayList;
import java.util.Collection;
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
@TestMethodOrder(OrderAnnotation.class)
public abstract class RestoreTestBase implements TableTestProgramRunner {

    // This version can be set to generate savepoints for a particular Flink version.
    // By default, the savepoint is generated for the current version in the default directory.
    private static final FlinkVersion FLINK_VERSION = null;
    private final Class<? extends ExecNode<?>> execNodeUnderTest;
    private final List<Class<? extends ExecNode<?>>> childExecNodesUnderTest;
    private final AfterRestoreSource afterRestoreSource;

    protected RestoreTestBase(Class<? extends ExecNode<?>> execNodeUnderTest) {
        this(execNodeUnderTest, new ArrayList<>(), AfterRestoreSource.FINITE);
    }

    protected RestoreTestBase(
            Class<? extends ExecNode<?>> execNodeUnderTest,
            List<Class<? extends ExecNode<?>>> childExecNodesUnderTest) {
        this(execNodeUnderTest, childExecNodesUnderTest, AfterRestoreSource.FINITE);
    }

    protected RestoreTestBase(
            Class<? extends ExecNode<?>> execNodeUnderTest, AfterRestoreSource state) {
        this(execNodeUnderTest, new ArrayList<>(), state);
    }

    protected RestoreTestBase(
            Class<? extends ExecNode<?>> execNodeUnderTest,
            List<Class<? extends ExecNode<?>>> childExecNodesUnderTest,
            AfterRestoreSource state) {
        this.execNodeUnderTest = execNodeUnderTest;
        this.childExecNodesUnderTest = childExecNodesUnderTest;
        this.afterRestoreSource = state;
    }

    /**
     * AfterRestoreSource defines the source behavior while running {@link
     * RestoreTestBase#testRestore}.
     */
    protected enum AfterRestoreSource {
        FINITE,
        INFINITE,
        NO_RESTORE
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
                TestKind.TEMPORAL_FUNCTION,
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
                                supportedPrograms().stream()
                                        .flatMap(
                                                program ->
                                                        getSavepointPaths(program, metadata)
                                                                .map(
                                                                        savepointPath ->
                                                                                Arguments.of(
                                                                                        program,
                                                                                        getPlanPath(
                                                                                                program,
                                                                                                metadata),
                                                                                        savepointPath))));
    }

    /**
     * The method can be overridden in a subclass to test multiple savepoint files for a given
     * program and a node in a particular version. This can be useful e.g. to test a node against
     * savepoint generated in different Flink versions.
     */
    protected Stream<String> getSavepointPaths(
            TableTestProgram program, ExecNodeMetadata metadata) {
        return Stream.of(getSavepointPath(program, metadata, null));
    }

    protected final String getSavepointPath(
            TableTestProgram program,
            ExecNodeMetadata metadata,
            @Nullable FlinkVersion flinkVersion) {
        StringBuilder builder = new StringBuilder();
        builder.append(getTestResourceDirectory(program, metadata));
        if (flinkVersion != null) {
            builder.append("/").append(flinkVersion);
        }
        builder.append("/savepoint/");

        return builder.toString();
    }

    private void registerSinkObserver(
            final List<CompletableFuture<?>> futures,
            final SinkTestStep sinkTestStep,
            final boolean ignoreAfter) {
        final CompletableFuture<Object> future = new CompletableFuture<>();
        futures.add(future);
        final String tableName = sinkTestStep.name;
        TestValuesTableFactory.registerLocalRawResultsObserver(
                tableName,
                (integer, strings) -> {
                    List<String> results =
                            new ArrayList<>(sinkTestStep.getExpectedBeforeRestoreAsStrings());
                    if (!ignoreAfter) {
                        results.addAll(sinkTestStep.getExpectedAfterRestoreAsStrings());
                    }
                    List<String> expectedResults = getExpectedResults(sinkTestStep, tableName);
                    final boolean shouldComplete =
                            CollectionUtils.isEqualCollection(expectedResults, results);
                    if (shouldComplete) {
                        future.complete(null);
                    }
                });
    }

    /**
     * Execute this test to generate test files. Remember to be using the correct branch when
     * generating the test files.
     */
    @Disabled
    @ParameterizedTest
    @MethodSource("supportedPrograms")
    @Order(0)
    public void generateTestSetupFiles(TableTestProgram program) throws Exception {
        final EnvironmentSettings settings = EnvironmentSettings.inStreamingMode();
        settings.getConfiguration().set(StateBackendOptions.STATE_BACKEND, "rocksdb");
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
            options.put("terminating", "false");
            options.put("runtime-source", "NewSource");
            sourceTestStep.apply(tEnv, options);
        }

        final List<CompletableFuture<?>> futures = new ArrayList<>();
        for (SinkTestStep sinkTestStep : program.getSetupSinkTestSteps()) {
            registerSinkObserver(futures, sinkTestStep, true);
            final Map<String, String> options = new HashMap<>();
            options.put("connector", "values");
            options.put("sink-insert-only", "false");
            sinkTestStep.apply(tEnv, options);
        }

        program.getSetupFunctionTestSteps().forEach(s -> s.apply(tEnv));
        program.getSetupTemporalFunctionTestSteps().forEach(s -> s.apply(tEnv));

        final CompiledPlan compiledPlan;
        if (program.runSteps.get(0).getKind() == TestKind.STATEMENT_SET) {
            final StatementSetTestStep statementSetTestStep = program.getRunStatementSetTestStep();
            compiledPlan = statementSetTestStep.compiledPlan(tEnv);
        } else {
            final SqlTestStep sqlTestStep = program.getRunSqlTestStep();
            compiledPlan = tEnv.compilePlanSql(sqlTestStep.sql);
        }

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
        final Path savepointDirPath =
                Paths.get(getSavepointPath(program, getLatestMetadata(), FLINK_VERSION));
        Files.createDirectories(savepointDirPath);
        Files.move(savepointPath, savepointDirPath, StandardCopyOption.ATOMIC_MOVE);
    }

    @ParameterizedTest
    @MethodSource("createSpecs")
    @Order(1)
    void testRestore(TableTestProgram program, Path planPath, String savepointPath)
            throws Exception {
        final EnvironmentSettings settings = EnvironmentSettings.inStreamingMode();
        final SavepointRestoreSettings restoreSettings;
        if (afterRestoreSource == AfterRestoreSource.NO_RESTORE) {
            restoreSettings = SavepointRestoreSettings.none();
        } else {
            restoreSettings =
                    SavepointRestoreSettings.forPath(savepointPath, false, RestoreMode.NO_CLAIM);
        }
        SavepointRestoreSettings.toConfiguration(restoreSettings, settings.getConfiguration());
        settings.getConfiguration().set(StateBackendOptions.STATE_BACKEND, "rocksdb");
        final TableEnvironment tEnv = TableEnvironment.create(settings);
        tEnv.getConfig()
                .set(
                        TableConfigOptions.PLAN_RESTORE_CATALOG_OBJECTS,
                        TableConfigOptions.CatalogPlanRestore.IDENTIFIER);

        program.getSetupConfigOptionTestSteps().forEach(s -> s.apply(tEnv));

        for (SourceTestStep sourceTestStep : program.getSetupSourceTestSteps()) {
            final Collection<Row> data =
                    afterRestoreSource == AfterRestoreSource.NO_RESTORE
                            ? sourceTestStep.dataBeforeRestore
                            : sourceTestStep.dataAfterRestore;
            final String id = TestValuesTableFactory.registerData(data);
            final Map<String, String> options = new HashMap<>();
            options.put("connector", "values");
            options.put("data-id", id);
            options.put("runtime-source", "NewSource");
            if (afterRestoreSource == AfterRestoreSource.INFINITE) {
                options.put("terminating", "false");
            }
            sourceTestStep.apply(tEnv, options);
        }

        final List<CompletableFuture<?>> futures = new ArrayList<>();

        for (SinkTestStep sinkTestStep : program.getSetupSinkTestSteps()) {
            if (afterRestoreSource == AfterRestoreSource.INFINITE) {
                registerSinkObserver(futures, sinkTestStep, false);
            }
            final Map<String, String> options = new HashMap<>();
            options.put("connector", "values");
            options.put("disable-lookup", "true");
            options.put("sink-insert-only", "false");
            sinkTestStep.apply(tEnv, options);
        }

        program.getSetupFunctionTestSteps().forEach(s -> s.apply(tEnv));
        program.getSetupTemporalFunctionTestSteps().forEach(s -> s.apply(tEnv));

        final CompiledPlan compiledPlan = tEnv.loadPlan(PlanReference.fromFile(planPath));

        if (afterRestoreSource == AfterRestoreSource.INFINITE) {
            final TableResult tableResult = compiledPlan.execute();
            CompletableFuture.allOf(futures.toArray(new CompletableFuture[0])).get();
            tableResult.getJobClient().get().cancel().get();
        } else {
            compiledPlan.execute().await();
            for (SinkTestStep sinkTestStep : program.getSetupSinkTestSteps()) {
                List<String> expectedResults = getExpectedResults(sinkTestStep, sinkTestStep.name);
                assertThat(expectedResults)
                        .containsExactlyInAnyOrder(
                                Stream.concat(
                                                sinkTestStep.getExpectedBeforeRestoreAsStrings()
                                                        .stream(),
                                                sinkTestStep.getExpectedAfterRestoreAsStrings()
                                                        .stream())
                                        .toArray(String[]::new));
            }
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
        if (sinkTestStep.getTestChangelogData()) {
            return TestValuesTableFactory.getRawResultsAsStrings(tableName);
        } else {
            return TestValuesTableFactory.getResultsAsStrings(tableName);
        }
    }
}
