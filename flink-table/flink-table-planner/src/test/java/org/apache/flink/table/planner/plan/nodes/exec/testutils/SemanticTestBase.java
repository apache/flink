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
import org.apache.flink.table.test.program.FunctionTestStep;
import org.apache.flink.table.test.program.SinkTestStep;
import org.apache.flink.table.test.program.SourceTestStep;
import org.apache.flink.table.test.program.SqlTestStep;
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
        return EnumSet.of(TestKind.SQL);
    }

    @AfterEach
    public void clearData() {
        TestValuesTableFactory.clearAllData();
    }

    @ParameterizedTest
    @MethodSource("supportedPrograms")
    void runTests(TableTestProgram program) throws Exception {
        final TableEnvironment env = TableEnvironment.create(EnvironmentSettings.inStreamingMode());

        for (TestStep testStep : program.setupSteps) {
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
                                TestValuesTableFactory.registerData(
                                        sourceTestStep.dataBeforeRestore);
                        final Map<String, String> options = new HashMap<>();
                        options.put("connector", "values");
                        options.put("data-id", id);
                        options.put("runtime-source", "NewSource");
                        sourceTestStep.apply(env, options);
                    }
                    break;
                case SINK_WITH_DATA:
                    {
                        final SinkTestStep sinkTestStep = (SinkTestStep) testStep;
                        final Map<String, String> options = new HashMap<>();
                        options.put("connector", "values");
                        options.put("sink-insert-only", "false");
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
            }
        }

        program.getRunSqlTestStep().apply(env).await();

        for (SinkTestStep sinkTestStep : program.getSetupSinkTestSteps()) {
            List<String> actualResults = getActualResults(sinkTestStep, sinkTestStep.name);
            assertThat(actualResults)
                    .containsExactlyInAnyOrder(
                            sinkTestStep.getExpectedAsStrings().toArray(new String[0]));
        }
    }

    private static List<String> getActualResults(SinkTestStep sinkTestStep, String tableName) {
        if (sinkTestStep.shouldTestChangelogData()) {
            return TestValuesTableFactory.getRawResultsAsStrings(tableName);
        } else {
            return TestValuesTableFactory.getResultsAsStrings(tableName);
        }
    }
}
