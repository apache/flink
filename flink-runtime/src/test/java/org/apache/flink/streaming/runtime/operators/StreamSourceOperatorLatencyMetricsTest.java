/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.flink.streaming.runtime.operators;

import org.apache.flink.api.common.ExecutionConfig;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.state.OperatorStateStore;
import org.apache.flink.api.connector.source.ReaderOutput;
import org.apache.flink.api.connector.source.SourceReaderContext;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.configuration.MetricOptions;
import org.apache.flink.core.fs.CloseableRegistry;
import org.apache.flink.core.io.InputStatus;
import org.apache.flink.runtime.execution.Environment;
import org.apache.flink.runtime.jobgraph.OperatorID;
import org.apache.flink.runtime.operators.coordination.MockOperatorEventGateway;
import org.apache.flink.runtime.operators.testutils.MockEnvironment;
import org.apache.flink.runtime.state.AbstractStateBackend;
import org.apache.flink.runtime.state.OperatorStateBackendParametersImpl;
import org.apache.flink.runtime.state.StateInitializationContext;
import org.apache.flink.runtime.state.StateInitializationContextImpl;
import org.apache.flink.runtime.state.hashmap.HashMapStateBackend;
import org.apache.flink.runtime.util.TestingTaskManagerRuntimeInfo;
import org.apache.flink.streaming.api.graph.StreamConfig;
import org.apache.flink.streaming.api.operators.Output;
import org.apache.flink.streaming.api.operators.SourceOperator;
import org.apache.flink.streaming.api.operators.StreamOperatorParameters;
import org.apache.flink.streaming.api.operators.StreamSource;
import org.apache.flink.streaming.api.operators.source.CollectingDataOutput;
import org.apache.flink.streaming.runtime.io.DataInputStatus;
import org.apache.flink.streaming.runtime.streamrecord.StreamElement;
import org.apache.flink.streaming.runtime.streamrecord.StreamRecord;
import org.apache.flink.streaming.runtime.tasks.TestProcessingTimeService;
import org.apache.flink.streaming.runtime.tasks.TimerService;
import org.apache.flink.streaming.util.MockOutput;
import org.apache.flink.streaming.util.MockStreamConfig;
import org.apache.flink.streaming.util.MockStreamTask;
import org.apache.flink.streaming.util.MockStreamTaskBuilder;
import org.apache.flink.test.util.source.AbstractTestSource;
import org.apache.flink.test.util.source.TestSourceReader;
import org.apache.flink.test.util.source.TestSplit;

import org.junit.jupiter.api.Test;

import java.time.Duration;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;

import static org.apache.flink.streaming.api.operators.StreamOperatorUtils.setProcessingTimeService;
import static org.apache.flink.streaming.api.operators.StreamOperatorUtils.setupStreamOperator;
import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.Mockito.mock;

/** Tests for the emission of latency markers by {@link StreamSource} operators. */
class StreamSourceOperatorLatencyMetricsTest {

    private static final long maxProcessingTime = 100L;
    private static final long latencyMarkInterval = 10L;

    /** Verifies that by default no latency metrics are emitted. */
    @Test
    void testLatencyMarkEmissionDisabled() throws Exception {
        testLatencyMarkEmission(
                0,
                (operator, timeProvider) ->
                        setupSourceOperator(
                                operator,
                                new ExecutionConfig(),
                                MockEnvironment.builder().build(),
                                timeProvider));
    }

    /** Verifies that latency metrics can be enabled via the {@link ExecutionConfig}. */
    @Test
    void testLatencyMarkEmissionEnabledViaExecutionConfig() throws Exception {
        testLatencyMarkEmission(
                (int) (maxProcessingTime / latencyMarkInterval) + 1,
                (operator, timeProvider) -> {
                    ExecutionConfig executionConfig = new ExecutionConfig();
                    executionConfig.setLatencyTrackingInterval(latencyMarkInterval);

                    setupSourceOperator(
                            operator,
                            executionConfig,
                            MockEnvironment.builder().build(),
                            timeProvider);
                });
    }

    /** Verifies that latency metrics can be enabled via the configuration. */
    @Test
    void testLatencyMarkEmissionEnabledViaFlinkConfig() throws Exception {
        testLatencyMarkEmission(
                (int) (maxProcessingTime / latencyMarkInterval) + 1,
                (operator, timeProvider) -> {
                    Configuration tmConfig = new Configuration();
                    tmConfig.set(
                            MetricOptions.LATENCY_INTERVAL, Duration.ofMillis(latencyMarkInterval));

                    Environment env =
                            MockEnvironment.builder()
                                    .setTaskManagerRuntimeInfo(
                                            new TestingTaskManagerRuntimeInfo(tmConfig))
                                    .build();

                    setupSourceOperator(operator, new ExecutionConfig(), env, timeProvider);
                });
    }

    /**
     * Verifies that latency metrics can be enabled via the {@link ExecutionConfig} even if they are
     * disabled via the configuration.
     */
    @Test
    void testLatencyMarkEmissionEnabledOverrideViaExecutionConfig() throws Exception {
        testLatencyMarkEmission(
                (int) (maxProcessingTime / latencyMarkInterval) + 1,
                (operator, timeProvider) -> {
                    ExecutionConfig executionConfig = new ExecutionConfig();
                    executionConfig.setLatencyTrackingInterval(latencyMarkInterval);

                    Configuration tmConfig = new Configuration();
                    tmConfig.set(MetricOptions.LATENCY_INTERVAL, Duration.ofMillis(0L));

                    Environment env =
                            MockEnvironment.builder()
                                    .setTaskManagerRuntimeInfo(
                                            new TestingTaskManagerRuntimeInfo(tmConfig))
                                    .build();

                    setupSourceOperator(operator, executionConfig, env, timeProvider);
                });
    }

    /**
     * Verifies that latency metrics can be disabled via the {@link ExecutionConfig} even if they
     * are enabled via the configuration.
     */
    @Test
    void testLatencyMarkEmissionDisabledOverrideViaExecutionConfig() throws Exception {
        testLatencyMarkEmission(
                0,
                (operator, timeProvider) -> {
                    Configuration tmConfig = new Configuration();
                    tmConfig.set(
                            MetricOptions.LATENCY_INTERVAL, Duration.ofMillis(latencyMarkInterval));

                    Environment env =
                            MockEnvironment.builder()
                                    .setTaskManagerRuntimeInfo(
                                            new TestingTaskManagerRuntimeInfo(tmConfig))
                                    .build();

                    ExecutionConfig executionConfig = new ExecutionConfig();
                    executionConfig.setLatencyTrackingInterval(0);

                    setupSourceOperator(operator, executionConfig, env, timeProvider);
                });
    }

    private interface OperatorSetupOperation {
        void setupSourceOperator(
                SourceOperator<Long, TestSplit> operator,
                TestProcessingTimeService testProcessingTimeService)
                throws Exception;
    }

    private void testLatencyMarkEmission(
            int numberLatencyMarkers, OperatorSetupOperation operatorSetup) throws Exception {
        final List<StreamElement> output = new ArrayList<>();

        final TestProcessingTimeService testProcessingTimeService = new TestProcessingTimeService();
        testProcessingTimeService.setCurrentTime(0L);
        final List<Long> processingTimes = Arrays.asList(1L, 10L, 11L, 21L, maxProcessingTime);

        ProcessingTimeServiceSource source =
                new ProcessingTimeServiceSource(testProcessingTimeService, processingTimes);

        Environment env = MockEnvironment.builder().build();
        final SourceOperator<Long, TestSplit> operator =
                createTestLatencySourceOperator(env, source, testProcessingTimeService);

        operatorSetup.setupSourceOperator(operator, testProcessingTimeService);

        initState(operator, env);
        operator.open();

        // Use CollectingDataOutput to collect the results
        CollectingDataOutput<Long> dataOutput = new CollectingDataOutput<>();

        // Run operator until completion with timeout protection
        long deadline = System.currentTimeMillis() + 5000;
        while (operator.emitNext(dataOutput) != DataInputStatus.END_OF_DATA) {
            assertThat(System.currentTimeMillis())
                    .as("Test timed out waiting for END_OF_DATA")
                    .isLessThan(deadline);
        }

        operator.close();

        // Extract stream elements from the output
        for (Object event : dataOutput.getEvents()) {
            if (event instanceof StreamElement) {
                output.add((StreamElement) event);
            }
        }

        assertThat(output).hasSize(numberLatencyMarkers);

        long timestamp = 0L;
        int expectedLatencyIndex = 0;

        int i = 0;
        // verify that its only latency markers
        for (; i < numberLatencyMarkers; i++) {
            StreamElement se = output.get(i);
            assertThat(se.isLatencyMarker()).isTrue();
            assertThat(se.asLatencyMarker().getOperatorId()).isEqualTo(operator.getOperatorID());
            assertThat(se.asLatencyMarker().getSubtaskIndex()).isZero();

            // determines the next latency mark that should've been emitted
            // latency marks are emitted once per latencyMarkInterval,
            // as a result of which we never emit both 10 and 11
            while (timestamp > processingTimes.get(expectedLatencyIndex)) {
                expectedLatencyIndex++;
            }
            assertThat(se.asLatencyMarker().getMarkedTime())
                    .isEqualTo(processingTimes.get(expectedLatencyIndex));

            timestamp += latencyMarkInterval;
        }
    }

    private static void initState(SourceOperator<?, ?> op, Environment env) throws Exception {
        AbstractStateBackend backend = new HashMapStateBackend();
        CloseableRegistry reg = new CloseableRegistry();
        OperatorStateStore store =
                backend.createOperatorStateBackend(
                        new OperatorStateBackendParametersImpl(
                                env, "test-source-operator", Collections.emptyList(), reg));
        StateInitializationContext ctx =
                new StateInitializationContextImpl(null, store, null, null, null);
        op.initializeState(ctx);
    }

    private static SourceOperator<Long, TestSplit> createTestLatencySourceOperator(
            Environment env,
            ProcessingTimeServiceSource source,
            TestProcessingTimeService testProcessingTimeService)
            throws Exception {

        MockStreamTask mockTask = new MockStreamTaskBuilder(env).build();
        final SourceOperator<Long, TestSplit> operator =
                new SourceOperator<>(
                        new StreamOperatorParameters<>(
                                mockTask,
                                new MockStreamConfig(new Configuration(), 1),
                                new MockOutput<>(new ArrayList<>()),
                                () -> testProcessingTimeService,
                                null,
                                null),
                        (context) -> source.createReader(context),
                        new MockOperatorEventGateway(),
                        source.getSplitSerializer(),
                        WatermarkStrategy.noWatermarks(),
                        testProcessingTimeService,
                        new Configuration(),
                        "localhost",
                        false,
                        () -> false,
                        Collections.emptyMap());

        // The operator will be initialized via the setupSourceOperator method
        return operator;
    }

    // ------------------------------------------------------------------------

    @SuppressWarnings("unchecked")
    private static <T> void setupSourceOperator(
            SourceOperator<T, TestSplit> operator,
            ExecutionConfig executionConfig,
            Environment env,
            TimerService timerService)
            throws Exception {

        StreamConfig cfg = new StreamConfig(new Configuration());
        cfg.setStateBackend(new HashMapStateBackend());

        cfg.setOperatorID(new OperatorID());
        cfg.serializeAllConfigs();

        MockStreamTask mockTask =
                new MockStreamTaskBuilder(env)
                        .setConfig(cfg)
                        .setExecutionConfig(executionConfig)
                        .setTimerService(timerService)
                        .build();

        setProcessingTimeService(
                operator,
                mockTask.getProcessingTimeServiceFactory().createProcessingTimeService(null));
        setupStreamOperator(operator, mockTask, cfg, (Output<StreamRecord<T>>) mock(Output.class));
    }

    /**
     * This source does not emit any records; it only manipulates the processing time service to
     * trigger latency marker emissions at specific time intervals.
     */
    private static final class ProcessingTimeServiceSource extends AbstractTestSource<Long> {

        private final TestProcessingTimeService processingTimeService;
        private final List<Long> processingTimes;

        private ProcessingTimeServiceSource(
                TestProcessingTimeService processingTimeService, List<Long> processingTimes) {
            this.processingTimeService = processingTimeService;
            this.processingTimes = processingTimes;
        }

        @Override
        public TestSourceReader<Long> createReader(SourceReaderContext readerContext) {
            return new TestSourceReader<>(readerContext) {
                private int currentIndex = 0;

                @Override
                public InputStatus pollNext(ReaderOutput<Long> output) throws Exception {
                    if (currentIndex >= processingTimes.size()) {
                        return InputStatus.END_OF_INPUT;
                    }

                    // Does not emit real records or require splits.
                    processingTimeService.setCurrentTime(processingTimes.get(currentIndex++));

                    return currentIndex < processingTimes.size()
                            ? InputStatus.MORE_AVAILABLE
                            : InputStatus.END_OF_INPUT;
                }
            };
        }
    }
}
