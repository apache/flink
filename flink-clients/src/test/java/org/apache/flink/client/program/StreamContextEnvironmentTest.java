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

package org.apache.flink.client.program;

import org.apache.flink.api.common.ExecutionConfig;
import org.apache.flink.api.common.RuntimeExecutionMode;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.configuration.DeploymentOptions;
import org.apache.flink.configuration.ExecutionOptions;
import org.apache.flink.configuration.PipelineOptions;
import org.apache.flink.core.execution.PipelineExecutorFactory;
import org.apache.flink.core.execution.PipelineExecutorServiceLoader;
import org.apache.flink.runtime.jobgraph.SavepointConfigOptions;
import org.apache.flink.streaming.api.CheckpointingMode;
import org.apache.flink.streaming.api.environment.CheckpointConfig;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.sink.DiscardingSink;
import org.apache.flink.util.function.ThrowingConsumer;

import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.MethodSource;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.stream.Stream;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

class StreamContextEnvironmentTest {

    @ParameterizedTest
    @MethodSource("provideExecutors")
    void testDisallowProgramConfigurationChanges(
            ThrowingConsumer<StreamExecutionEnvironment, Exception> executor) {
        final Configuration clusterConfig = new Configuration();
        clusterConfig.set(DeploymentOptions.PROGRAM_CONFIG_ENABLED, false);
        clusterConfig.set(DeploymentOptions.TARGET, "local");
        clusterConfig.set(SavepointConfigOptions.SAVEPOINT_PATH, "/flink/savepoints");
        clusterConfig.set(ExecutionOptions.RUNTIME_MODE, RuntimeExecutionMode.STREAMING);

        final Configuration programConfig = new Configuration();
        programConfig.set(DeploymentOptions.PROGRAM_CONFIG_ENABLED, false);
        programConfig.set(DeploymentOptions.TARGET, "local");
        programConfig.set(ExecutionOptions.RUNTIME_MODE, RuntimeExecutionMode.BATCH);
        programConfig.set(ExecutionOptions.SORT_INPUTS, true);

        final ClassLoader classLoader = Thread.currentThread().getContextClassLoader();
        final StreamContextEnvironment environment =
                new StreamContextEnvironment(
                        new MockExecutorServiceLoader(),
                        clusterConfig,
                        clusterConfig,
                        classLoader,
                        true,
                        true,
                        false,
                        new ArrayList<>());

        // Change the CheckpointConfig
        environment.enableCheckpointing(500, CheckpointingMode.EXACTLY_ONCE);
        // Change the ExecutionConfig
        environment.setParallelism(25);

        // Add/mutate values in the configuration
        environment.configure(programConfig);

        environment.fromCollection(Collections.singleton(1)).addSink(new DiscardingSink<>());
        assertThatThrownBy(() -> executor.accept(environment))
                .isInstanceOf(MutatedConfigurationException.class)
                .hasMessageContainingAll(
                        ExecutionOptions.RUNTIME_MODE.key(),
                        ExecutionOptions.SORT_INPUTS.key(),
                        CheckpointConfig.class.getSimpleName(),
                        ExecutionConfig.class.getSimpleName());
    }

    @ParameterizedTest
    @MethodSource("provideExecutors")
    void testAllowProgramConfigurationWildcards(
            ThrowingConsumer<StreamExecutionEnvironment, Exception> executor) {
        final Configuration clusterConfig = new Configuration();
        clusterConfig.set(DeploymentOptions.TARGET, "local");
        clusterConfig.set(ExecutionOptions.RUNTIME_MODE, RuntimeExecutionMode.STREAMING);
        // Test prefix map notation
        clusterConfig.setString(
                PipelineOptions.GLOBAL_JOB_PARAMETERS.key() + "." + "my-param", "my-value");

        final Configuration jobConfig = new Configuration();
        jobConfig.set(
                PipelineOptions.GLOBAL_JOB_PARAMETERS,
                Collections.singletonMap("my-other-param", "my-other-value"));

        final ClassLoader classLoader = Thread.currentThread().getContextClassLoader();
        final StreamContextEnvironment environment =
                new StreamContextEnvironment(
                        new MockExecutorServiceLoader(),
                        clusterConfig,
                        clusterConfig,
                        classLoader,
                        true,
                        true,
                        false,
                        Collections.singletonList(PipelineOptions.GLOBAL_JOB_PARAMETERS.key()));

        // Change ExecutionConfig
        environment.configure(jobConfig);

        environment.fromCollection(Collections.singleton(1)).addSink(new DiscardingSink<>());
        assertThatThrownBy(() -> executor.accept(environment))
                .isInstanceOf(ExecutorReachedException.class);
        assertThat(environment.getConfig().getGlobalJobParameters().toMap())
                .containsOnlyKeys("my-other-param");
    }

    private static List<ThrowingConsumer<StreamExecutionEnvironment, Exception>>
            provideExecutors() {
        return Arrays.asList(
                StreamExecutionEnvironment::execute, StreamExecutionEnvironment::executeAsync);
    }

    private static class MockExecutorServiceLoader implements PipelineExecutorServiceLoader {

        @Override
        public PipelineExecutorFactory getExecutorFactory(Configuration configuration) {
            throw new ExecutorReachedException();
        }

        @Override
        public Stream<String> getExecutorNames() {
            throw new ExecutorReachedException();
        }
    }

    private static class ExecutorReachedException extends RuntimeException {}
}
