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

package org.apache.flink.streaming.runtime.io;

import org.apache.flink.api.common.ExecutionConfig;
import org.apache.flink.api.common.TaskInfo;
import org.apache.flink.api.common.typeutils.TypeSerializer;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.core.memory.ManagedMemoryUseCase;
import org.apache.flink.metrics.Counter;
import org.apache.flink.runtime.checkpoint.InflightDataRescalingDescriptor;
import org.apache.flink.runtime.io.disk.iomanager.IOManager;
import org.apache.flink.runtime.jobgraph.tasks.TaskInvokable;
import org.apache.flink.runtime.memory.MemoryManager;
import org.apache.flink.runtime.metrics.groups.TaskIOMetricGroup;
import org.apache.flink.streaming.api.graph.StreamConfig;
import org.apache.flink.streaming.api.operators.InputSelectable;
import org.apache.flink.streaming.api.operators.TwoInputStreamOperator;
import org.apache.flink.streaming.api.operators.sort.MultiInputSortingDataInput;
import org.apache.flink.streaming.api.operators.sort.MultiInputSortingDataInput.SelectableSortingInputs;
import org.apache.flink.streaming.api.watermark.Watermark;
import org.apache.flink.streaming.runtime.io.checkpointing.CheckpointedInputGate;
import org.apache.flink.streaming.runtime.metrics.WatermarkGauge;
import org.apache.flink.streaming.runtime.partitioner.StreamPartitioner;
import org.apache.flink.streaming.runtime.streamrecord.LatencyMarker;
import org.apache.flink.streaming.runtime.streamrecord.StreamRecord;
import org.apache.flink.streaming.runtime.tasks.OperatorChain;
import org.apache.flink.streaming.runtime.tasks.StreamTask.CanEmitBatchOfRecordsChecker;
import org.apache.flink.streaming.runtime.watermarkstatus.StatusWatermarkValve;
import org.apache.flink.streaming.runtime.watermarkstatus.WatermarkStatus;
import org.apache.flink.util.function.ThrowingConsumer;

import javax.annotation.Nullable;

import java.util.ArrayList;
import java.util.List;
import java.util.function.Function;

import static org.apache.flink.streaming.api.graph.StreamConfig.requiresSorting;
import static org.apache.flink.util.Preconditions.checkNotNull;

/** A factory to create {@link StreamMultipleInputProcessor} for two input case. */
public class StreamTwoInputProcessorFactory {
    public static <IN1, IN2> StreamMultipleInputProcessor create(
            TaskInvokable ownerTask,
            CheckpointedInputGate[] checkpointedInputGates,
            IOManager ioManager,
            MemoryManager memoryManager,
            TaskIOMetricGroup taskIOMetricGroup,
            TwoInputStreamOperator<IN1, IN2, ?> streamOperator,
            WatermarkGauge input1WatermarkGauge,
            WatermarkGauge input2WatermarkGauge,
            OperatorChain<?, ?> operatorChain,
            StreamConfig streamConfig,
            Configuration taskManagerConfig,
            Configuration jobConfig,
            ExecutionConfig executionConfig,
            ClassLoader userClassloader,
            Counter numRecordsIn,
            InflightDataRescalingDescriptor inflightDataRescalingDescriptor,
            Function<Integer, StreamPartitioner<?>> gatePartitioners,
            TaskInfo taskInfo,
            CanEmitBatchOfRecordsChecker canEmitBatchOfRecords) {

        checkNotNull(operatorChain);

        taskIOMetricGroup.reuseRecordsInputCounter(numRecordsIn);
        TypeSerializer<IN1> typeSerializer1 = streamConfig.getTypeSerializerIn(0, userClassloader);
        StreamTaskInput<IN1> input1 =
                StreamTaskNetworkInputFactory.create(
                        checkpointedInputGates[0],
                        typeSerializer1,
                        ioManager,
                        new StatusWatermarkValve(
                                checkpointedInputGates[0].getNumberOfInputChannels()),
                        0,
                        inflightDataRescalingDescriptor,
                        gatePartitioners,
                        taskInfo,
                        canEmitBatchOfRecords);
        TypeSerializer<IN2> typeSerializer2 = streamConfig.getTypeSerializerIn(1, userClassloader);
        StreamTaskInput<IN2> input2 =
                StreamTaskNetworkInputFactory.create(
                        checkpointedInputGates[1],
                        typeSerializer2,
                        ioManager,
                        new StatusWatermarkValve(
                                checkpointedInputGates[1].getNumberOfInputChannels()),
                        1,
                        inflightDataRescalingDescriptor,
                        gatePartitioners,
                        taskInfo,
                        canEmitBatchOfRecords);

        InputSelectable inputSelectable =
                streamOperator instanceof InputSelectable ? (InputSelectable) streamOperator : null;

        // this is a bit verbose because we're manually handling input1 and input2
        // TODO: extract method
        StreamConfig.InputConfig[] inputConfigs = streamConfig.getInputs(userClassloader);
        boolean input1IsSorted = requiresSorting(inputConfigs[0]);
        boolean input2IsSorted = requiresSorting(inputConfigs[1]);

        if (input1IsSorted || input2IsSorted) {
            // as soon as one input requires sorting we need to treat all inputs differently, to
            // make sure that pass-through inputs have precedence

            if (inputSelectable != null) {
                throw new IllegalStateException(
                        "The InputSelectable interface is not supported with sorting inputs");
            }

            List<StreamTaskInput<?>> sortedTaskInputs = new ArrayList<>();
            List<KeySelector<?, ?>> keySelectors = new ArrayList<>();
            List<StreamTaskInput<?>> passThroughTaskInputs = new ArrayList<>();
            if (input1IsSorted) {
                sortedTaskInputs.add(input1);
                keySelectors.add(streamConfig.getStatePartitioner(0, userClassloader));
            } else {
                passThroughTaskInputs.add(input1);
            }
            if (input2IsSorted) {
                sortedTaskInputs.add(input2);
                keySelectors.add(streamConfig.getStatePartitioner(1, userClassloader));
            } else {
                passThroughTaskInputs.add(input2);
            }

            @SuppressWarnings("unchecked")
            SelectableSortingInputs selectableSortingInputs =
                    MultiInputSortingDataInput.wrapInputs(
                            ownerTask,
                            sortedTaskInputs.toArray(new StreamTaskInput[0]),
                            keySelectors.toArray(new KeySelector[0]),
                            new TypeSerializer[] {typeSerializer1, typeSerializer2},
                            streamConfig.getStateKeySerializer(userClassloader),
                            passThroughTaskInputs.toArray(new StreamTaskInput[0]),
                            memoryManager,
                            ioManager,
                            executionConfig.isObjectReuseEnabled(),
                            streamConfig.getManagedMemoryFractionOperatorUseCaseOfSlot(
                                    ManagedMemoryUseCase.OPERATOR,
                                    taskManagerConfig,
                                    userClassloader),
                            taskManagerConfig,
                            executionConfig);
            inputSelectable = selectableSortingInputs.getInputSelectable();
            StreamTaskInput<?>[] sortedInputs = selectableSortingInputs.getSortedInputs();
            StreamTaskInput<?>[] passThroughInputs = selectableSortingInputs.getPassThroughInputs();
            if (input1IsSorted) {
                input1 = toTypedInput(sortedInputs[0]);
            } else {
                input1 = toTypedInput(passThroughInputs[0]);
            }
            if (input2IsSorted) {
                input2 = toTypedInput(sortedInputs[sortedInputs.length - 1]);
            } else {
                input2 = toTypedInput(passThroughInputs[passThroughInputs.length - 1]);
            }
        }

        @Nullable
        FinishedOnRestoreWatermarkBypass watermarkBypass =
                operatorChain.isTaskDeployedAsFinished()
                        ? new FinishedOnRestoreWatermarkBypass(operatorChain.getStreamOutputs())
                        : null;
        StreamTaskNetworkOutput<IN1> output1 =
                new StreamTaskNetworkOutput<>(
                        streamOperator,
                        RecordProcessorUtils.getRecordProcessor1(streamOperator),
                        input1WatermarkGauge,
                        0,
                        numRecordsIn,
                        watermarkBypass);
        StreamOneInputProcessor<IN1> processor1 =
                new StreamOneInputProcessor<>(input1, output1, operatorChain);

        StreamTaskNetworkOutput<IN2> output2 =
                new StreamTaskNetworkOutput<>(
                        streamOperator,
                        RecordProcessorUtils.getRecordProcessor2(streamOperator),
                        input2WatermarkGauge,
                        1,
                        numRecordsIn,
                        watermarkBypass);
        StreamOneInputProcessor<IN2> processor2 =
                new StreamOneInputProcessor<>(input2, output2, operatorChain);

        return new StreamMultipleInputProcessor(
                new MultipleInputSelectionHandler(inputSelectable, 2),
                new StreamOneInputProcessor[] {processor1, processor2});
    }

    @SuppressWarnings("unchecked")
    private static <IN1> StreamTaskInput<IN1> toTypedInput(StreamTaskInput<?> multiInput) {
        return (StreamTaskInput<IN1>) multiInput;
    }

    /**
     * The network data output implementation used for processing stream elements from {@link
     * StreamTaskNetworkInput} in two input selective processor.
     */
    private static class StreamTaskNetworkOutput<T> implements PushingAsyncDataInput.DataOutput<T> {

        private final TwoInputStreamOperator<?, ?, ?> operator;

        /** The function way is only used for frequent record processing as for JIT optimization. */
        private final ThrowingConsumer<StreamRecord<T>, Exception> recordConsumer;

        private final WatermarkGauge inputWatermarkGauge;

        /** The input index to indicate how to process elements by two input operator. */
        private final int inputIndex;

        private final Counter numRecordsIn;

        private final @Nullable FinishedOnRestoreWatermarkBypass watermarkBypass;

        private StreamTaskNetworkOutput(
                TwoInputStreamOperator<?, ?, ?> operator,
                ThrowingConsumer<StreamRecord<T>, Exception> recordConsumer,
                WatermarkGauge inputWatermarkGauge,
                int inputIndex,
                Counter numRecordsIn,
                @Nullable FinishedOnRestoreWatermarkBypass watermarkBypass) {
            this.operator = checkNotNull(operator);
            this.recordConsumer = checkNotNull(recordConsumer);
            this.inputWatermarkGauge = checkNotNull(inputWatermarkGauge);
            this.inputIndex = inputIndex;
            this.numRecordsIn = numRecordsIn;
            this.watermarkBypass = watermarkBypass;
        }

        @Override
        public void emitRecord(StreamRecord<T> record) throws Exception {
            numRecordsIn.inc();
            recordConsumer.accept(record);
        }

        @Override
        public void emitWatermark(Watermark watermark) throws Exception {
            inputWatermarkGauge.setCurrentWatermark(watermark.getTimestamp());
            if (inputIndex == 0) {
                if (watermarkBypass == null) {
                    operator.processWatermark1(watermark);
                } else {
                    watermarkBypass.processWatermark1(watermark);
                }
            } else {
                if (watermarkBypass == null) {
                    operator.processWatermark2(watermark);
                } else {
                    watermarkBypass.processWatermark2(watermark);
                }
            }
        }

        @Override
        public void emitWatermarkStatus(WatermarkStatus watermarkStatus) throws Exception {
            if (inputIndex == 0) {
                operator.processWatermarkStatus1(watermarkStatus);
            } else {
                operator.processWatermarkStatus2(watermarkStatus);
            }
        }

        @Override
        public void emitLatencyMarker(LatencyMarker latencyMarker) throws Exception {
            if (inputIndex == 0) {
                operator.processLatencyMarker1(latencyMarker);
            } else {
                operator.processLatencyMarker2(latencyMarker);
            }
        }
    }

    private static class FinishedOnRestoreWatermarkBypass {
        private final RecordWriterOutput<?>[] streamOutputs;

        private boolean receivedFirstMaxWatermark;
        private boolean receivedSecondMaxWatermark;

        public FinishedOnRestoreWatermarkBypass(RecordWriterOutput<?>[] streamOutputs) {
            this.streamOutputs = streamOutputs;
        }

        public void processWatermark1(Watermark watermark) {
            receivedFirstMaxWatermark = true;
            checkAndForward(watermark);
        }

        public void processWatermark2(Watermark watermark) {
            receivedSecondMaxWatermark = true;
            checkAndForward(watermark);
        }

        private void checkAndForward(Watermark watermark) {
            if (watermark.getTimestamp() != Watermark.MAX_WATERMARK.getTimestamp()) {
                throw new IllegalStateException(
                        String.format(
                                "We should not receive any watermarks [%s] other than the MAX_WATERMARK if finished on restore",
                                watermark));
            }
            if (receivedFirstMaxWatermark && receivedSecondMaxWatermark) {
                for (RecordWriterOutput<?> streamOutput : streamOutputs) {
                    streamOutput.emitWatermark(watermark);
                }
            }
        }
    }
}
