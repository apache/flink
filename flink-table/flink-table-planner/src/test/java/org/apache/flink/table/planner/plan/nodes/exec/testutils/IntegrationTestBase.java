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

import org.apache.flink.configuration.CheckpointingOptions;
import org.apache.flink.configuration.ConfigOption;
import org.apache.flink.configuration.CoreOptions;
import org.apache.flink.configuration.RestartStrategyOptions;
import org.apache.flink.core.execution.CheckpointingMode;
import org.apache.flink.runtime.testutils.MiniClusterResourceConfiguration;
import org.apache.flink.table.api.EnvironmentSettings;
import org.apache.flink.table.api.TableConfig;
import org.apache.flink.table.api.TableEnvironment;
import org.apache.flink.table.api.config.ExecutionConfigOptions;
import org.apache.flink.table.planner.factories.TestValuesTableFactory;
import org.apache.flink.table.planner.runtime.utils.FailingCollectionSource;
import org.apache.flink.table.test.program.SinkTestStep;
import org.apache.flink.table.test.program.SourceTestStep;
import org.apache.flink.table.test.program.TableTestProgram;
import org.apache.flink.table.test.program.TestStep;
import org.apache.flink.test.junit5.MiniClusterExtension;

import org.apache.flink.shaded.guava33.com.google.common.collect.ImmutableList;

import org.junit.jupiter.api.TestInstance;
import org.junit.jupiter.api.extension.ExtendWith;
import org.junit.jupiter.api.extension.RegisterExtension;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.MethodSource;

import java.time.Duration;
import java.util.EnumSet;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Common base for integration tests that use the {@link TableTestProgram} infrastructure to verify
 * execution correctness. Unlike {@link CommonSemanticTestBase}, this class runs tests with
 * parallelism 4 and uses {@link FailingCollectionSource} to ensure consistency under more realistic
 * conditions.
 *
 * <p>This base compares results before and after enabling the options provided by {@link
 * #testOptions()}. It allows easy validation of new functionality.
 */
@ExtendWith(MiniClusterExtension.class)
@TestInstance(TestInstance.Lifecycle.PER_CLASS)
public abstract class IntegrationTestBase extends CommonSemanticTestBase {

    private static final int DEFAULT_PARALLELISM = 4;

    @RegisterExtension
    private static final MiniClusterExtension MINI_CLUSTER_EXTENSION =
            new MiniClusterExtension(
                    new MiniClusterResourceConfiguration.Builder()
                            .setNumberTaskManagers(1)
                            .setNumberSlotsPerTaskManager(DEFAULT_PARALLELISM)
                            .build());

    @Override
    public EnumSet<TestStep.TestKind> supportedSetupSteps() {
        return EnumSet.of(
                TestStep.TestKind.CONFIG,
                TestStep.TestKind.MODEL,
                TestStep.TestKind.SOURCE_WITH_DATA,
                TestStep.TestKind.SINK_WITH_DATA,
                TestStep.TestKind.SINK_WITHOUT_DATA,
                TestStep.TestKind.FUNCTION,
                TestStep.TestKind.SQL);
    }

    @ParameterizedTest
    @MethodSource("supportedPrograms")
    void runSteps(TableTestProgram program) throws Exception {
        TableEnvironment env = TableEnvironment.create(EnvironmentSettings.inStreamingMode());

        setupTestOptions(env.getConfig(), false);

        List<String> expected = runSteps(program, env);

        env = TableEnvironment.create(EnvironmentSettings.inStreamingMode());

        setupTestOptions(env.getConfig(), true);

        List<String> actual = runSteps(program, env);

        assertThat(actual)
                .as("%s", program.id)
                .containsExactlyInAnyOrder(expected.toArray(new String[0]));
    }

    List<String> runSteps(TableTestProgram program, TableEnvironment env) throws Exception {
        TestValuesTableFactory.clearAllData();
        FailingCollectionSource.reset();

        applyDefaultEnvironmentOptions(env.getConfig());

        for (TestStep testStep : program.setupSteps) {
            runStep(testStep, env);
        }

        for (TestStep testStep : program.runSteps) {
            runStep(testStep, env);
        }

        assertThat(program.getSetupSinkTestSteps().size()).isEqualTo(1);
        SinkTestStep sinkTestStep = program.getSetupSinkTestSteps().get(0);

        return getActualResults(sinkTestStep, sinkTestStep.name);
    }

    @Override
    protected void applyDefaultEnvironmentOptions(TableConfig config) {
        super.applyDefaultEnvironmentOptions(config);
        config.set(
                        ExecutionConfigOptions.TABLE_EXEC_RESOURCE_DEFAULT_PARALLELISM,
                        DEFAULT_PARALLELISM)
                .set(CoreOptions.DEFAULT_PARALLELISM, DEFAULT_PARALLELISM)
                .set(CheckpointingOptions.CHECKPOINTING_INTERVAL, Duration.ofMillis(100))
                .set(
                        CheckpointingOptions.CHECKPOINTING_CONSISTENCY_MODE,
                        CheckpointingMode.EXACTLY_ONCE)
                .set(RestartStrategyOptions.RESTART_STRATEGY, "fixeddelay")
                .set(RestartStrategyOptions.RESTART_STRATEGY_FIXED_DELAY_ATTEMPTS, 1)
                .set(
                        RestartStrategyOptions.RESTART_STRATEGY_FIXED_DELAY_DELAY,
                        Duration.ofMillis(0));
    }

    @Override
    public Map<String, String> createSourceOptions(SourceTestStep sourceTestStep, String id) {
        final Map<String, String> options = new HashMap<>();
        options.put("connector", "values");
        options.put("data-id", id);
        options.put("runtime-source", "SourceFunction");
        options.put("failing-source", "true");

        // Enforce per-record watermarks for testing
        if (sourceTestStep.schemaComponents.stream().anyMatch(c -> c.startsWith("WATERMARK FOR"))) {
            options.put("disable-lookup", "true");
            options.put("enable-watermark-push-down", "true");
            options.put("scan.watermark.emit.strategy", "on-event");
        }
        return options;
    }

    @Override
    public boolean isBounded() {
        return false;
    }

    /**
     * Returns the configuration options that should be toggled when comparing results before and
     * after they are enabled.
     *
     * @return list of options to test
     */
    public abstract ImmutableList<ConfigOption<Boolean>> testOptions();

    private void setupTestOptions(TableConfig config, boolean value) {
        ImmutableList<ConfigOption<Boolean>> testOptions = testOptions();
        assertThat(testOptions).isNotEmpty();
        for (ConfigOption<Boolean> option : testOptions) {
            config.set(option, value);
        }
    }
}
