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

package org.apache.flink.streaming.api.operators.source;

import org.apache.flink.api.common.ExecutionConfig;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.state.OperatorStateStore;
import org.apache.flink.api.connector.source.SourceReader;
import org.apache.flink.api.connector.source.mocks.MockSourceSplit;
import org.apache.flink.api.connector.source.mocks.MockSourceSplitSerializer;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.core.fs.CloseableRegistry;
import org.apache.flink.runtime.metrics.groups.UnregisteredMetricGroups;
import org.apache.flink.runtime.operators.coordination.MockOperatorEventGateway;
import org.apache.flink.runtime.operators.coordination.OperatorEventGateway;
import org.apache.flink.runtime.operators.testutils.MockEnvironmentBuilder;
import org.apache.flink.runtime.operators.testutils.MockInputSplitProvider;
import org.apache.flink.runtime.state.StateInitializationContext;
import org.apache.flink.runtime.state.StateInitializationContextImpl;
import org.apache.flink.runtime.state.TestTaskStateManager;
import org.apache.flink.runtime.state.hashmap.HashMapStateBackend;
import org.apache.flink.streaming.api.operators.SourceOperator;
import org.apache.flink.streaming.api.operators.StreamingRuntimeContext;
import org.apache.flink.streaming.runtime.tasks.ProcessingTimeService;
import org.apache.flink.streaming.runtime.tasks.SourceOperatorStreamTask;
import org.apache.flink.streaming.runtime.tasks.StreamMockEnvironment;
import org.apache.flink.streaming.runtime.tasks.TestProcessingTimeService;
import org.apache.flink.streaming.util.MockOutput;
import org.apache.flink.streaming.util.MockStreamConfig;
import org.apache.flink.streaming.util.MockStreamingRuntimeContext;

import java.util.ArrayList;
import java.util.Collections;

/** A SourceOperator extension to simplify test setup. */
public class TestingSourceOperator<T> extends SourceOperator<T, MockSourceSplit> {

    private static final long serialVersionUID = 1L;

    private final int subtaskIndex;
    private final int parallelism;

    public TestingSourceOperator(
            SourceReader<T, MockSourceSplit> reader,
            WatermarkStrategy<T> watermarkStrategy,
            ProcessingTimeService timeService,
            boolean emitProgressiveWatermarks) {

        this(
                reader,
                watermarkStrategy,
                timeService,
                new MockOperatorEventGateway(),
                1,
                5,
                emitProgressiveWatermarks);
    }

    public TestingSourceOperator(
            SourceReader<T, MockSourceSplit> reader,
            OperatorEventGateway eventGateway,
            int subtaskIndex,
            boolean emitProgressiveWatermarks) {

        this(
                reader,
                WatermarkStrategy.noWatermarks(),
                new TestProcessingTimeService(),
                eventGateway,
                subtaskIndex,
                5,
                emitProgressiveWatermarks);
    }

    public TestingSourceOperator(
            SourceReader<T, MockSourceSplit> reader,
            WatermarkStrategy<T> watermarkStrategy,
            ProcessingTimeService timeService,
            OperatorEventGateway eventGateway,
            int subtaskIndex,
            int parallelism,
            boolean emitProgressiveWatermarks) {

        super(
                (context) -> reader,
                eventGateway,
                new MockSourceSplitSerializer(),
                watermarkStrategy,
                timeService,
                new Configuration(),
                "localhost",
                emitProgressiveWatermarks);

        this.subtaskIndex = subtaskIndex;
        this.parallelism = parallelism;
        this.metrics = UnregisteredMetricGroups.createUnregisteredOperatorMetricGroup();
        initSourceMetricGroup();

        // unchecked wrapping is okay to keep tests simpler
        try {
            initReader();
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    @Override
    public StreamingRuntimeContext getRuntimeContext() {
        return new MockStreamingRuntimeContext(false, parallelism, subtaskIndex);
    }

    // this is overridden to avoid complex mock injection through the "containingTask"
    @Override
    public ExecutionConfig getExecutionConfig() {
        ExecutionConfig cfg = new ExecutionConfig();
        cfg.setAutoWatermarkInterval(100);
        return cfg;
    }

    public static <T> SourceOperator<T, MockSourceSplit> createTestOperator(
            SourceReader<T, MockSourceSplit> reader,
            WatermarkStrategy<T> watermarkStrategy,
            boolean emitProgressiveWatermarks)
            throws Exception {

        final OperatorStateStore operatorStateStore =
                new HashMapStateBackend()
                        .createOperatorStateBackend(
                                new MockEnvironmentBuilder().build(),
                                "test-operator",
                                Collections.emptyList(),
                                new CloseableRegistry());

        final StateInitializationContext stateContext =
                new StateInitializationContextImpl(null, operatorStateStore, null, null, null);

        TestProcessingTimeService timeService = new TestProcessingTimeService();
        timeService.setCurrentTime(Integer.MAX_VALUE); // start somewhere that is not zero

        final SourceOperator<T, MockSourceSplit> sourceOperator =
                new TestingSourceOperator<>(
                        reader, watermarkStrategy, timeService, emitProgressiveWatermarks);

        sourceOperator.setup(
                new SourceOperatorStreamTask<Integer>(
                        new StreamMockEnvironment(
                                new Configuration(),
                                new Configuration(),
                                new ExecutionConfig(),
                                1L,
                                new MockInputSplitProvider(),
                                1,
                                new TestTaskStateManager())),
                new MockStreamConfig(new Configuration(), 1),
                new MockOutput<>(new ArrayList<>()));
        sourceOperator.initializeState(stateContext);
        sourceOperator.open();

        return sourceOperator;
    }
}
