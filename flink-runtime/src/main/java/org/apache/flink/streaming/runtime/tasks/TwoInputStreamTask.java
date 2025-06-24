/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.flink.streaming.runtime.tasks;

import org.apache.flink.annotation.Internal;
import org.apache.flink.runtime.execution.Environment;
import org.apache.flink.runtime.io.network.partition.consumer.IndexedInputGate;
import org.apache.flink.streaming.api.operators.TwoInputStreamOperator;
import org.apache.flink.streaming.runtime.io.StreamTwoInputProcessorFactory;
import org.apache.flink.streaming.runtime.io.checkpointing.CheckpointBarrierHandler;
import org.apache.flink.streaming.runtime.io.checkpointing.CheckpointedInputGate;
import org.apache.flink.streaming.runtime.io.checkpointing.InputProcessorUtil;
import org.apache.flink.streaming.runtime.partitioner.StreamPartitioner;

import javax.annotation.Nullable;

import java.util.Collections;
import java.util.List;
import java.util.Optional;
import java.util.function.Function;

import static org.apache.flink.util.Preconditions.checkState;

/**
 * A {@link StreamTask} for executing a {@link TwoInputStreamOperator} and supporting the {@link
 * TwoInputStreamOperator} to select input for reading.
 */
@Internal
public class TwoInputStreamTask<IN1, IN2, OUT> extends AbstractTwoInputStreamTask<IN1, IN2, OUT> {

    @Nullable private CheckpointBarrierHandler checkpointBarrierHandler;

    public TwoInputStreamTask(Environment env) throws Exception {
        super(env);
    }

    @Override
    protected Optional<CheckpointBarrierHandler> getCheckpointBarrierHandler() {
        return Optional.ofNullable(checkpointBarrierHandler);
    }

    @SuppressWarnings("unchecked")
    @Override
    protected void createInputProcessor(
            List<IndexedInputGate> inputGates1,
            List<IndexedInputGate> inputGates2,
            Function<Integer, StreamPartitioner<?>> gatePartitioners) {

        // create an input instance for each input
        checkpointBarrierHandler =
                InputProcessorUtil.createCheckpointBarrierHandler(
                        this,
                        configuration,
                        getCheckpointCoordinator(),
                        getTaskNameWithSubtaskAndId(),
                        new List[] {inputGates1, inputGates2},
                        Collections.emptyList(),
                        mainMailboxExecutor,
                        systemTimerService);

        CheckpointedInputGate[] checkpointedInputGates =
                InputProcessorUtil.createCheckpointedMultipleInputGate(
                        mainMailboxExecutor,
                        new List[] {inputGates1, inputGates2},
                        getEnvironment().getMetricGroup().getIOMetricGroup(),
                        checkpointBarrierHandler,
                        configuration);

        checkState(checkpointedInputGates.length == 2);

        inputProcessor =
                StreamTwoInputProcessorFactory.create(
                        this,
                        checkpointedInputGates,
                        getEnvironment().getIOManager(),
                        getEnvironment().getMemoryManager(),
                        getEnvironment().getMetricGroup().getIOMetricGroup(),
                        mainOperator,
                        input1WatermarkGauge,
                        input2WatermarkGauge,
                        operatorChain,
                        getConfiguration(),
                        getEnvironment().getTaskManagerInfo().getConfiguration(),
                        getJobConfiguration(),
                        getExecutionConfig(),
                        getUserCodeClassLoader(),
                        setupNumRecordsInCounter(mainOperator),
                        getEnvironment().getTaskStateManager().getInputRescalingDescriptor(),
                        gatePartitioners,
                        getEnvironment().getTaskInfo(),
                        getCanEmitBatchOfRecords());
    }

    // This is needed for StreamMultipleInputProcessor#processInput to preserve the existing
    // behavior of choosing an input every time a record is emitted. This behavior is good for
    // fairness between input consumption. But it can reduce throughput due to added control
    // flow cost on the per-record code path.
    @Override
    public CanEmitBatchOfRecordsChecker getCanEmitBatchOfRecords() {
        return () -> false;
    }
}
