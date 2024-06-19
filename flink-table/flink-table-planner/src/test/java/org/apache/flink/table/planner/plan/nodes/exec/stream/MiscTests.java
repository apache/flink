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

package org.apache.flink.table.planner.plan.nodes.exec.stream;

import org.apache.flink.table.api.EnvironmentSettings;
import org.apache.flink.table.api.TableEnvironment;
import org.apache.flink.table.planner.factories.TestValuesTableFactory;
import org.apache.flink.table.planner.plan.nodes.exec.testutils.RestoreTestBase;
import org.apache.flink.table.test.program.SinkTestStep;
import org.apache.flink.table.test.program.SourceTestStep;
import org.apache.flink.table.test.program.TableTestProgram;
import org.apache.flink.table.test.program.TableTestProgramRunner;
import org.apache.flink.table.test.program.TestStep;
import org.apache.flink.test.junit5.MiniClusterExtension;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.TestInstance;
import org.junit.jupiter.api.extension.ExtendWith;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.MethodSource;

import java.util.Collections;
import java.util.EnumSet;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Miscellaneous tests that do not fall into {@link RestoreTestBase} category, but use the {@link
 * TableTestProgram} infrastructure.
 */
@ExtendWith(MiniClusterExtension.class)
@TestInstance(TestInstance.Lifecycle.PER_CLASS)
class MiscTests implements TableTestProgramRunner {
    @Override
    public List<TableTestProgram> programs() {
        return Collections.singletonList(
                WindowRankTestPrograms.WINDOW_RANK_HOP_TVF_NAMED_MIN_TOP_1);
    }

    @Override
    public EnumSet<TestStep.TestKind> supportedSetupSteps() {
        return EnumSet.of(
                TestStep.TestKind.CONFIG,
                TestStep.TestKind.SOURCE_WITH_DATA,
                TestStep.TestKind.SINK_WITH_DATA);
    }

    @Override
    public EnumSet<TestStep.TestKind> supportedRunSteps() {
        return EnumSet.of(TestStep.TestKind.SQL);
    }

    @AfterEach
    public void clearData() {
        TestValuesTableFactory.clearAllData();
    }

    @ParameterizedTest
    @MethodSource("supportedPrograms")
    void runTests(TableTestProgram program) throws Exception {
        final TableEnvironment tEnv =
                TableEnvironment.create(EnvironmentSettings.inStreamingMode());
        program.getSetupConfigOptionTestSteps().forEach(s -> s.apply(tEnv));

        for (SourceTestStep sourceTestStep : program.getSetupSourceTestSteps()) {
            final String id = TestValuesTableFactory.registerData(sourceTestStep.dataBeforeRestore);
            final Map<String, String> options = new HashMap<>();
            options.put("connector", "values");
            options.put("data-id", id);
            options.put("runtime-source", "NewSource");
            sourceTestStep.apply(tEnv, options);
        }

        for (SinkTestStep sinkTestStep : program.getSetupSinkTestSteps()) {
            final Map<String, String> options = new HashMap<>();
            options.put("connector", "values");
            options.put("sink-insert-only", "false");
            sinkTestStep.apply(tEnv, options);
        }

        program.getRunSqlTestStep().apply(tEnv).await();
        for (SinkTestStep sinkTestStep : program.getSetupSinkTestSteps()) {
            List<String> expectedResults = getExpectedResults(sinkTestStep, sinkTestStep.name);
            assertThat(expectedResults)
                    .containsExactlyInAnyOrder(
                            sinkTestStep.getExpectedAsStrings().toArray(new String[0]));
        }
    }

    private static List<String> getExpectedResults(SinkTestStep sinkTestStep, String tableName) {
        if (sinkTestStep.getTestChangelogData()) {
            return TestValuesTableFactory.getRawResultsAsStrings(tableName);
        } else {
            return TestValuesTableFactory.getResultsAsStrings(tableName);
        }
    }
}
