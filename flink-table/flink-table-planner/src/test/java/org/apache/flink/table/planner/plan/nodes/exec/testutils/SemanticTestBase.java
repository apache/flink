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

import org.apache.flink.table.api.EnvironmentSettings;
import org.apache.flink.table.api.TableEnvironment;
import org.apache.flink.table.planner.factories.TestValuesTableFactory;
import org.apache.flink.table.test.program.ConfigOptionTestStep;
import org.apache.flink.table.test.program.FailingSqlTestStep;
import org.apache.flink.table.test.program.FunctionTestStep;
import org.apache.flink.table.test.program.SinkTestStep;
import org.apache.flink.table.test.program.SourceTestStep;
import org.apache.flink.table.test.program.SqlTestStep;
import org.apache.flink.table.test.program.TableApiTestStep;
import org.apache.flink.table.test.program.TableTestProgram;
import org.apache.flink.table.test.program.TableTestProgramRunner;
import org.apache.flink.table.test.program.TestStep;
import org.apache.flink.table.test.program.TestStep.TestKind;
import org.apache.flink.test.junit5.MiniClusterExtension;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.TestInstance;
import org.junit.jupiter.api.extension.ExtendWith;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.MethodSource;

import java.util.EnumSet;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Base for tests that do not fall into the {@link RestoreTestBase} category, but use the {@link
 * TableTestProgram} infrastructure for testing whether the execution result is semantically
 * correct.
 */
@ExtendWith(MiniClusterExtension.class)
@TestInstance(TestInstance.Lifecycle.PER_CLASS)
public abstract class SemanticTestBase implements TableTestProgramRunner {

    @Override
    public EnumSet<TestKind> supportedSetupSteps() {
        return EnumSet.of(
                TestKind.CONFIG,
                TestKind.SOURCE_WITH_DATA,
                TestKind.SINK_WITH_DATA,
                TestKind.FUNCTION,
                TestKind.SQL);
    }

    @Override
    public EnumSet<TestKind> supportedRunSteps() {
        return EnumSet.of(TestKind.SQL, TestKind.FAILING_SQL, TestKind.TABLE_API);
    }

    @AfterEach
    public void clearData() {
        TestValuesTableFactory.clearAllData();
    }

    @ParameterizedTest
    @MethodSource("supportedPrograms")
    void runSteps(TableTestProgram program) throws Exception {
        final TableEnvironment env = TableEnvironment.create(EnvironmentSettings.inStreamingMode());

        for (TestStep testStep : program.setupSteps) {
            runStep(testStep, env);
        }

        for (TestStep testStep : program.runSteps) {
            runStep(testStep, env);
        }

        for (SinkTestStep sinkTestStep : program.getSetupSinkTestSteps()) {
            List<String> actualResults = getActualResults(sinkTestStep, sinkTestStep.name);
            assertThat(actualResults)
                    .containsExactlyInAnyOrder(
                            sinkTestStep.getExpectedAsStrings().toArray(new String[0]));
        }
    }

    protected void runStep(TestStep testStep, TableEnvironment env) throws Exception {
        switch (testStep.getKind()) {
            case CONFIG:
                {
                    final ConfigOptionTestStep<?> configTestStep =
                            (ConfigOptionTestStep<?>) testStep;
                    configTestStep.apply(env);
                }
                break;
            case SOURCE_WITH_DATA:
                {
                    final SourceTestStep sourceTestStep = (SourceTestStep) testStep;
                    final String id =
                            TestValuesTableFactory.registerData(sourceTestStep.dataBeforeRestore);
                    final Map<String, String> options = createSourceOptions(sourceTestStep, id);
                    sourceTestStep.apply(env, options);
                }
                break;
            case SINK_WITH_DATA:
                {
                    final SinkTestStep sinkTestStep = (SinkTestStep) testStep;
                    final Map<String, String> options = createSinkOptions();
                    sinkTestStep.apply(env, options);
                }
                break;
            case FUNCTION:
                {
                    final FunctionTestStep functionTestStep = (FunctionTestStep) testStep;
                    functionTestStep.apply(env);
                    break;
                }
            case SQL:
                {
                    final SqlTestStep sqlTestStep = (SqlTestStep) testStep;
                    sqlTestStep.apply(env).await();
                }
                break;
            case FAILING_SQL:
                {
                    final FailingSqlTestStep sqlTestStep = (FailingSqlTestStep) testStep;
                    sqlTestStep.apply(env);
                }
                break;
            case TABLE_API:
                {
                    final TableApiTestStep apiTestStep = (TableApiTestStep) testStep;
                    apiTestStep.apply(env).await();
                }
                break;
        }
    }

    private static Map<String, String> createSourceOptions(
            SourceTestStep sourceTestStep, String id) {
        final Map<String, String> options = new HashMap<>();
        options.put("connector", "values");
        options.put("data-id", id);
        options.put("runtime-source", "NewSource");
        // Enforce per-record watermarks for testing
        if (sourceTestStep.schemaComponents.stream().anyMatch(c -> c.startsWith("WATERMARK FOR"))) {
            options.put("disable-lookup", "true");
            options.put("enable-watermark-push-down", "true");
            options.put("scan.watermark.emit.strategy", "on-event");
        }
        return options;
    }

    private static Map<String, String> createSinkOptions() {
        return Map.ofEntries(
                Map.entry("connector", "values"), Map.entry("sink-insert-only", "false"));
    }

    private static List<String> getActualResults(SinkTestStep sinkTestStep, String tableName) {
        if (sinkTestStep.shouldTestChangelogData()) {
            return TestValuesTableFactory.getRawResultsAsStrings(tableName);
        } else {
            return TestValuesTableFactory.getResultsAsStrings(tableName);
        }
    }
}
