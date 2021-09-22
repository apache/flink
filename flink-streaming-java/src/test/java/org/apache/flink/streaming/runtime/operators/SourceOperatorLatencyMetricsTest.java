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
import org.apache.flink.api.common.state.ListStateDescriptor;
import org.apache.flink.api.common.state.OperatorStateStore;
import org.apache.flink.api.common.typeutils.base.array.BytePrimitiveArraySerializer;
import org.apache.flink.api.connector.source.ReaderOutput;
import org.apache.flink.api.connector.source.mocks.MockSourceReader;
import org.apache.flink.api.connector.source.mocks.MockSourceSplit;
import org.apache.flink.api.connector.source.mocks.MockSourceSplitSerializer;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.configuration.MetricOptions;
import org.apache.flink.core.fs.CloseableRegistry;
import org.apache.flink.core.io.InputStatus;
import org.apache.flink.core.io.SimpleVersionedSerialization;
import org.apache.flink.runtime.execution.Environment;
import org.apache.flink.runtime.jobgraph.OperatorID;
import org.apache.flink.runtime.operators.coordination.MockOperatorEventGateway;
import org.apache.flink.runtime.operators.testutils.MockEnvironment;
import org.apache.flink.runtime.operators.testutils.MockEnvironmentBuilder;
import org.apache.flink.runtime.operators.testutils.MockInputSplitProvider;
import org.apache.flink.runtime.state.AbstractStateBackend;
import org.apache.flink.runtime.state.StateInitializationContext;
import org.apache.flink.runtime.state.StateInitializationContextImpl;
import org.apache.flink.runtime.state.TestTaskStateManager;
import org.apache.flink.runtime.state.hashmap.HashMapStateBackend;
import org.apache.flink.runtime.util.TestingTaskManagerRuntimeInfo;
import org.apache.flink.streaming.api.graph.StreamConfig;
import org.apache.flink.streaming.api.operators.SourceOperator;
import org.apache.flink.streaming.runtime.streamrecord.StreamElement;
import org.apache.flink.streaming.runtime.tasks.OperatorChain;
import org.apache.flink.streaming.runtime.tasks.RegularOperatorChain;
import org.apache.flink.streaming.runtime.tasks.SourceOperatorStreamTask;
import org.apache.flink.streaming.runtime.tasks.StreamMockEnvironment;
import org.apache.flink.streaming.runtime.tasks.StreamTask;
import org.apache.flink.streaming.runtime.tasks.TestProcessingTimeService;
import org.apache.flink.streaming.util.CollectorDataOutput;
import org.apache.flink.streaming.util.MockOutput;
import org.apache.flink.util.TestLogger;

import org.junit.Test;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

/** Tests for the emission of latency markers by {@link SourceOperator} operators. */
public class SourceOperatorLatencyMetricsTest extends TestLogger {

    private static final long maxProcessingTime = 100L;
    private static final long latencyMarkInterval = 10L;

    /** Verifies that by default no latency metrics are emitted. */
    @Test
    public void testLatencyMarkEmissionDisabled() throws Exception {
        testLatencyMarkEmission(
                0,
                operator ->
                        setupSourceOperator(
                                operator,
                                new ExecutionConfig(),
                                MockEnvironment.builder().build()));
    }

    /** Verifies that latency metrics can be enabled via the {@link ExecutionConfig}. */
    @Test
    public void testLatencyMarkEmissionEnabledViaExecutionConfig() throws Exception {
        testLatencyMarkEmission(
                (int) (maxProcessingTime / latencyMarkInterval) + 1,
                operator -> {
                    ExecutionConfig executionConfig = new ExecutionConfig();
                    executionConfig.setLatencyTrackingInterval(latencyMarkInterval);
                    setupSourceOperator(
                            operator, executionConfig, MockEnvironment.builder().build());
                });
    }

    /** Verifies that latency metrics can be enabled via the configuration. */
    @Test
    public void testLatencyMarkEmissionEnabledViaFlinkConfig() throws Exception {
        testLatencyMarkEmission(
                (int) (maxProcessingTime / latencyMarkInterval) + 1,
                operator -> {
                    Configuration tmConfig = new Configuration();
                    tmConfig.setLong(MetricOptions.LATENCY_INTERVAL, latencyMarkInterval);
                    Environment env =
                            MockEnvironment.builder()
                                    .setTaskManagerRuntimeInfo(
                                            new TestingTaskManagerRuntimeInfo(tmConfig))
                                    .build();
                    setupSourceOperator(operator, new ExecutionConfig(), env);
                });
    }

    /**
     * Verifies that latency metrics can be enabled via the {@link ExecutionConfig} even if they are
     * disabled via the configuration.
     */
    @Test
    public void testLatencyMarkEmissionEnabledOverrideViaExecutionConfig() throws Exception {
        testLatencyMarkEmission(
                (int) (maxProcessingTime / latencyMarkInterval) + 1,
                operator -> {
                    ExecutionConfig executionConfig = new ExecutionConfig();
                    executionConfig.setLatencyTrackingInterval(latencyMarkInterval);
                    Configuration tmConfig = new Configuration();
                    tmConfig.setLong(MetricOptions.LATENCY_INTERVAL, 0L);
                    Environment env =
                            MockEnvironment.builder()
                                    .setTaskManagerRuntimeInfo(
                                            new TestingTaskManagerRuntimeInfo(tmConfig))
                                    .build();
                    setupSourceOperator(operator, executionConfig, env);
                });
    }

    /**
     * Verifies that latency metrics can be disabled via the {@link ExecutionConfig} even if they
     * are enabled via the configuration.
     */
    @Test
    public void testLatencyMarkEmissionDisabledOverrideViaExecutionConfig() throws Exception {
        testLatencyMarkEmission(
                0,
                operator -> {
                    Configuration tmConfig = new Configuration();
                    tmConfig.setLong(MetricOptions.LATENCY_INTERVAL, latencyMarkInterval);
                    Environment env =
                            MockEnvironment.builder()
                                    .setTaskManagerRuntimeInfo(
                                            new TestingTaskManagerRuntimeInfo(tmConfig))
                                    .build();
                    ExecutionConfig executionConfig = new ExecutionConfig();
                    executionConfig.setLatencyTrackingInterval(0);
                    setupSourceOperator(operator, executionConfig, env);
                });
    }

    private interface OperatorSetupOperation {
        void setupSourceOperator(SourceOperator<Integer, ?> operator);
    }

    private void testLatencyMarkEmission(
            int numberLatencyMarkers, OperatorSetupOperation operatorSetup) throws Exception {
        final List<StreamElement> output = new ArrayList<>();

        final TestProcessingTimeService testProcessingTimeService = new TestProcessingTimeService();
        testProcessingTimeService.setCurrentTime(0L);
        final List<Long> processingTimes = Arrays.asList(1L, 10L, 11L, 21L, maxProcessingTime);

        // regular source operator
        final SourceOperator<Integer, MockSourceSplit> sourceOperator =
                new SourceOperator<>(
                        (context) ->
                                new ProcessingTimeServiceSourceReader(
                                        testProcessingTimeService, processingTimes),
                        new MockOperatorEventGateway(),
                        new MockSourceSplitSerializer(),
                        WatermarkStrategy.noWatermarks(),
                        testProcessingTimeService,
                        new Configuration(),
                        "localhost",
                        true /* emit progressive watermarks */);
        operatorSetup.setupSourceOperator(sourceOperator);

        // run and wait to be stopped
        OperatorChain<?, ?> operatorChain =
                new RegularOperatorChain<>(
                        sourceOperator.getContainingTask(),
                        StreamTask.createRecordWriterDelegate(
                                sourceOperator.getOperatorConfig(),
                                new MockEnvironmentBuilder().build()));
        try {
            sourceOperator.initializeState(getStateContext());
            sourceOperator.open();
            sourceOperator.emitNext(new CollectorDataOutput<>(output));
            sourceOperator.finish();
        } finally {
            operatorChain.close();
        }

        assertEquals(numberLatencyMarkers, output.size());

        long timestamp = 0L;
        int expectedLatencyIndex = 0;

        int index = 0;
        // verify that its only latency markers
        for (; index < numberLatencyMarkers; index++) {
            StreamElement streamElement = output.get(index);
            assertTrue(streamElement.isLatencyMarker());
            assertEquals(
                    sourceOperator.getOperatorID(),
                    streamElement.asLatencyMarker().getOperatorId());
            assertEquals(0, streamElement.asLatencyMarker().getSubtaskIndex());

            // determines the next latency mark that should've been emitted
            // latency marks are emitted once per latencyMarkInterval,
            // as a result of which we never emit both 10 and 11
            while (timestamp > processingTimes.get(expectedLatencyIndex)) {
                expectedLatencyIndex++;
            }
            assertEquals(
                    processingTimes.get(expectedLatencyIndex).longValue(),
                    streamElement.asLatencyMarker().getMarkedTime());

            timestamp += latencyMarkInterval;
        }
    }

    // ---------------- helper methods -------------------------

    private static <T> void setupSourceOperator(
            SourceOperator<T, ?> sourceOperator,
            ExecutionConfig executionConfig,
            Environment environment) {
        StreamConfig streamConfig = new StreamConfig(new Configuration());
        streamConfig.setOperatorID(new OperatorID());
        try {
            sourceOperator.setup(
                    new SourceOperatorStreamTask<Integer>(
                            getTestingEnvironment(executionConfig, environment)),
                    streamConfig,
                    new MockOutput<>(new ArrayList<>()));
        } catch (Exception e) {
            e.printStackTrace();
            fail(e.getMessage());
        }
    }

    private StateInitializationContext getStateContext() throws Exception {
        // Create a mock split.
        byte[] serializedSplitWithVersion =
                SimpleVersionedSerialization.writeVersionAndSerialize(
                        new MockSourceSplitSerializer(), new MockSourceSplit(1234, 10));

        // Crate the state context.
        OperatorStateStore operatorStateStore = createOperatorStateStore();
        StateInitializationContext stateContext =
                new StateInitializationContextImpl(null, operatorStateStore, null, null, null);

        // Update the context.
        stateContext
                .getOperatorStateStore()
                .getListState(
                        new ListStateDescriptor<>(
                                "SourceReaderState", BytePrimitiveArraySerializer.INSTANCE))
                .update(Collections.singletonList(serializedSplitWithVersion));

        return stateContext;
    }

    private OperatorStateStore createOperatorStateStore() throws Exception {
        MockEnvironment env = new MockEnvironmentBuilder().build();
        final AbstractStateBackend abstractStateBackend = new HashMapStateBackend();
        CloseableRegistry cancelStreamRegistry = new CloseableRegistry();
        return abstractStateBackend.createOperatorStateBackend(
                env, "test-operator", Collections.emptyList(), cancelStreamRegistry);
    }

    private static Environment getTestingEnvironment(
            ExecutionConfig executionConfig, Environment environment) {
        StreamMockEnvironment mockEnvironment =
                new StreamMockEnvironment(
                        new Configuration(),
                        new Configuration(),
                        executionConfig,
                        1L,
                        new MockInputSplitProvider(),
                        1,
                        new TestTaskStateManager());
        mockEnvironment.setTaskManagerInfo(environment.getTaskManagerInfo());
        return mockEnvironment;
    }

    // ------------------------------------------------------------------------

    private static final class ProcessingTimeServiceSourceReader extends MockSourceReader {

        private final TestProcessingTimeService processingTimeService;
        private final List<Long> processingTimes;

        private boolean closed = false;

        private ProcessingTimeServiceSourceReader(
                TestProcessingTimeService processingTimeService, List<Long> processingTimes) {
            this.processingTimeService = processingTimeService;
            this.processingTimes = processingTimes;
        }

        @Override
        public InputStatus pollNext(ReaderOutput<Integer> sourceOutput) throws Exception {
            for (Long processingTime : processingTimes) {
                if (closed) {
                    break;
                }
                processingTimeService.setCurrentTime(processingTime);
            }
            return super.pollNext(sourceOutput);
        }

        @Override
        public void close() throws Exception {
            closed = true;
            super.close();
        }
    }
}
