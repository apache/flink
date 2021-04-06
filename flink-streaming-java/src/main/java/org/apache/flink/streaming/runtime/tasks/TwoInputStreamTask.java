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
import org.apache.flink.streaming.runtime.io.checkpointing.CheckpointedInputGate;
import org.apache.flink.streaming.runtime.io.checkpointing.InputProcessorUtil;
import org.apache.flink.streaming.runtime.partitioner.StreamPartitioner;

import java.util.Collections;
import java.util.List;
import java.util.function.Function;

import static org.apache.flink.util.Preconditions.checkState;

/**
 * A {@link StreamTask} for executing a {@link TwoInputStreamOperator} and supporting the {@link
 * TwoInputStreamOperator} to select input for reading.
 */
@Internal
public class TwoInputStreamTask<IN1, IN2, OUT> extends AbstractTwoInputStreamTask<IN1, IN2, OUT> {

    public TwoInputStreamTask(Environment env) throws Exception {
        super(env);
    }

    @Override
    protected void createInputProcessor(
            List<IndexedInputGate> inputGates1,
            List<IndexedInputGate> inputGates2,
            Function<Integer, StreamPartitioner<?>> gatePartitioners) {

        // create an input instance for each input
        CheckpointedInputGate[] checkpointedInputGates =
                InputProcessorUtil.createCheckpointedMultipleInputGate(
                        this,
                        getConfiguration(),
                        getCheckpointCoordinator(),
                        getEnvironment().getMetricGroup().getIOMetricGroup(),
                        getTaskNameWithSubtaskAndId(),
                        mainMailboxExecutor,
                        new List[] {inputGates1, inputGates2},
                        Collections.emptyList(),
                        systemTimerService);
        checkState(checkpointedInputGates.length == 2);

        inputProcessor =
                StreamTwoInputProcessorFactory.create(
                        this,
                        checkpointedInputGates,
                        getEnvironment().getIOManager(),
                        getEnvironment().getMemoryManager(),
                        getEnvironment().getMetricGroup().getIOMetricGroup(),
                        getStreamStatusMaintainer(),
                        mainOperator,
                        input1WatermarkGauge,
                        input2WatermarkGauge,
                        operatorChain,
                        getConfiguration(),
                        getTaskConfiguration(),
                        getJobConfiguration(),
                        getExecutionConfig(),
                        getUserCodeClassLoader(),
                        setupNumRecordsInCounter(mainOperator),
                        getEnvironment().getTaskStateManager().getInputRescalingDescriptor(),
                        gatePartitioners,
                        getEnvironment().getTaskInfo());
    }
}
