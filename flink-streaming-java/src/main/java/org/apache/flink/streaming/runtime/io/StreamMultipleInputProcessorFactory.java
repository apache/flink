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

import org.apache.flink.annotation.Internal;
import org.apache.flink.api.common.ExecutionConfig;
import org.apache.flink.api.common.typeutils.TypeSerializer;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.core.memory.ManagedMemoryUseCase;
import org.apache.flink.metrics.Counter;
import org.apache.flink.metrics.SimpleCounter;
import org.apache.flink.runtime.io.disk.iomanager.IOManager;
import org.apache.flink.runtime.jobgraph.tasks.AbstractInvokable;
import org.apache.flink.runtime.memory.MemoryManager;
import org.apache.flink.runtime.metrics.groups.TaskIOMetricGroup;
import org.apache.flink.streaming.api.graph.StreamConfig;
import org.apache.flink.streaming.api.operators.Input;
import org.apache.flink.streaming.api.operators.InputSelectable;
import org.apache.flink.streaming.api.operators.MultipleInputStreamOperator;
import org.apache.flink.streaming.api.operators.Output;
import org.apache.flink.streaming.api.operators.sort.MultiInputSortingDataInput;
import org.apache.flink.streaming.api.operators.sort.MultiInputSortingDataInput.SelectableSortingInputs;
import org.apache.flink.streaming.api.watermark.Watermark;
import org.apache.flink.streaming.runtime.metrics.WatermarkGauge;
import org.apache.flink.streaming.runtime.streamrecord.LatencyMarker;
import org.apache.flink.streaming.runtime.streamrecord.StreamRecord;
import org.apache.flink.streaming.runtime.streamstatus.StatusWatermarkValve;
import org.apache.flink.streaming.runtime.streamstatus.StreamStatus;
import org.apache.flink.streaming.runtime.streamstatus.StreamStatusMaintainer;
import org.apache.flink.streaming.runtime.tasks.OperatorChain;
import org.apache.flink.streaming.runtime.tasks.SourceOperatorStreamTask;

import java.util.Arrays;
import java.util.List;
import java.util.stream.IntStream;

import static org.apache.flink.util.Preconditions.checkNotNull;
import static org.apache.flink.util.Preconditions.checkState;

/**
 * A factory for {@link StreamMultipleInputProcessor}.
 */
@Internal
public class StreamMultipleInputProcessorFactory {

	@SuppressWarnings({"unchecked", "rawtypes"})
	public static StreamMultipleInputProcessor create(
			AbstractInvokable ownerTask,
			CheckpointedInputGate[] checkpointedInputGates,
			StreamConfig.InputConfig[] configuredInputs,
			IOManager ioManager,
			MemoryManager memoryManager,
			TaskIOMetricGroup ioMetricGroup,
			Counter mainOperatorRecordsIn,
			StreamStatusMaintainer streamStatusMaintainer,
			MultipleInputStreamOperator<?> mainOperator,
			WatermarkGauge[] inputWatermarkGauges,
			StreamConfig streamConfig,
			Configuration taskManagerConfig,
			Configuration jobConfig,
			ExecutionConfig executionConfig,
			ClassLoader userClassloader,
			OperatorChain<?, ?> operatorChain) {
		checkNotNull(operatorChain);

		List<Input> operatorInputs = mainOperator.getInputs();
		int inputsCount = operatorInputs.size();

		StreamOneInputProcessor<?>[] inputProcessors = new StreamOneInputProcessor[inputsCount];
		Counter networkRecordsIn = new SimpleCounter();
		ioMetricGroup.reuseRecordsInputCounter(networkRecordsIn);

		MultiStreamStreamStatusTracker streamStatusTracker = new MultiStreamStreamStatusTracker(inputsCount);
		checkState(
			configuredInputs.length == inputsCount,
			"Number of configured inputs in StreamConfig [%s] doesn't match the main operator's number of inputs [%s]",
			configuredInputs.length,
			inputsCount);
		StreamTaskInput[] inputs = new StreamTaskInput[inputsCount];
		for (int i = 0; i < inputsCount; i++) {
			StreamConfig.InputConfig configuredInput = configuredInputs[i];
			if (configuredInput instanceof StreamConfig.NetworkInputConfig) {
				StreamConfig.NetworkInputConfig networkInput = (StreamConfig.NetworkInputConfig) configuredInput;
				inputs[i] = new StreamTaskNetworkInput<>(
					checkpointedInputGates[networkInput.getInputGateIndex()],
					networkInput.getTypeSerializer(),
					ioManager,
					new StatusWatermarkValve(checkpointedInputGates[networkInput.getInputGateIndex()].getNumberOfInputChannels()),
					i);
			}
			else if (configuredInput instanceof StreamConfig.SourceInputConfig) {
				StreamConfig.SourceInputConfig sourceInput = (StreamConfig.SourceInputConfig) configuredInput;
				inputs[i] = operatorChain.getSourceTaskInput(sourceInput);
			}
			else {
				throw new UnsupportedOperationException("Unknown input type: " + configuredInput);
			}
		}

		InputSelectable inputSelectable =
			mainOperator instanceof InputSelectable ? (InputSelectable) mainOperator : null;
		if (streamConfig.shouldSortInputs()) {

			if (inputSelectable != null) {
				throw new IllegalStateException("The InputSelectable interface is not supported with sorting inputs");
			}

			SelectableSortingInputs selectableSortingInputs = MultiInputSortingDataInput.wrapInputs(
				ownerTask,
				inputs,
				IntStream.range(0, inputsCount)
					.mapToObj(idx -> streamConfig.getStatePartitioner(idx, userClassloader))
					.toArray(KeySelector[]::new),
				IntStream.range(0, inputsCount)
					.mapToObj(idx -> streamConfig.getTypeSerializerIn(idx, userClassloader))
					.toArray(TypeSerializer[]::new),
				streamConfig.getStateKeySerializer(userClassloader),
				memoryManager,
				ioManager,
				executionConfig.isObjectReuseEnabled(),
				streamConfig.getManagedMemoryFractionOperatorUseCaseOfSlot(
					ManagedMemoryUseCase.BATCH_OP,
					taskManagerConfig,
					userClassloader
				),
				jobConfig
			);

			inputs = selectableSortingInputs.getSortingInputs();
			inputSelectable = selectableSortingInputs.getInputSelectable();
		}

		for (int i = 0; i < inputsCount; i++) {
			StreamConfig.InputConfig configuredInput = configuredInputs[i];
			if (configuredInput instanceof StreamConfig.NetworkInputConfig) {
				StreamTaskNetworkOutput dataOutput = new StreamTaskNetworkOutput<>(
					operatorInputs.get(i),
					streamStatusMaintainer,
					inputWatermarkGauges[i],
					streamStatusTracker,
					i,
					mainOperatorRecordsIn,
					networkRecordsIn);

				inputProcessors[i] = new StreamOneInputProcessor(
					inputs[i],
					dataOutput,
					operatorChain);
			}
			else if (configuredInput instanceof StreamConfig.SourceInputConfig) {
				StreamConfig.SourceInputConfig sourceInput = (StreamConfig.SourceInputConfig) configuredInput;
				Output<StreamRecord<?>> chainedSourceOutput = operatorChain.getChainedSourceOutput(sourceInput);

				inputProcessors[i] = new StreamOneInputProcessor(
					inputs[i],
					new StreamTaskSourceOutput(chainedSourceOutput, streamStatusMaintainer, inputWatermarkGauges[i],
						streamStatusTracker,
						i),
					operatorChain);
			}
			else {
				throw new UnsupportedOperationException("Unknown input type: " + configuredInput);
			}
		}

		return new StreamMultipleInputProcessor(
			new MultipleInputSelectionHandler(inputSelectable, inputsCount),
			inputProcessors
		);
	}


	/**
	 * Stream status tracker for the inputs. We need to keep track for determining when
	 * to forward stream status changes downstream.
	 */
	private static class MultiStreamStreamStatusTracker {
		private final StreamStatus[] streamStatuses;

		private MultiStreamStreamStatusTracker(int numberOfInputs) {
			this.streamStatuses = new StreamStatus[numberOfInputs];
			Arrays.fill(streamStatuses, StreamStatus.ACTIVE);
		}

		public void setStreamStatus(int index, StreamStatus streamStatus) {
			streamStatuses[index] = streamStatus;
		}

		public StreamStatus getStreamStatus(int index) {
			return streamStatuses[index];
		}

		public boolean allStreamStatusesAreIdle() {
			for (StreamStatus streamStatus : streamStatuses) {
				if (streamStatus.isActive()) {
					return false;
				}
			}
			return true;
		}
	}

	/**
	 * The network data output implementation used for processing stream elements
	 * from {@link StreamTaskNetworkInput} in two input selective processor.
	 */
	private static class StreamTaskNetworkOutput<T> extends AbstractDataOutput<T> {
		private final Input<T> input;

		private final WatermarkGauge inputWatermarkGauge;

		/** The input index to indicate how to process elements by two input operator. */
		private final int inputIndex;

		private final MultiStreamStreamStatusTracker streamStatusTracker;

		private final Counter mainOperatorRecordsIn;

		private final Counter networkRecordsIn;

		private StreamTaskNetworkOutput(
				Input<T> input,
				StreamStatusMaintainer streamStatusMaintainer,
				WatermarkGauge inputWatermarkGauge,
				MultiStreamStreamStatusTracker streamStatusTracker,
				int inputIndex,
				Counter mainOperatorRecordsIn,
				Counter networkRecordsIn) {
			super(streamStatusMaintainer);

			this.input = checkNotNull(input);
			this.inputWatermarkGauge = checkNotNull(inputWatermarkGauge);
			this.streamStatusTracker = streamStatusTracker;
			this.inputIndex = inputIndex;
			this.mainOperatorRecordsIn = mainOperatorRecordsIn;
			this.networkRecordsIn = networkRecordsIn;
		}

		@Override
		public void emitRecord(StreamRecord<T> record) throws Exception {
			input.setKeyContextElement(record);
			input.processElement(record);
			mainOperatorRecordsIn.inc();
			networkRecordsIn.inc();
		}

		@Override
		public void emitWatermark(Watermark watermark) throws Exception {
			inputWatermarkGauge.setCurrentWatermark(watermark.getTimestamp());
			input.processWatermark(watermark);
		}

		@Override
		public void emitStreamStatus(StreamStatus streamStatus) {
			streamStatusTracker.setStreamStatus(inputIndex, streamStatus);

			// check if we need to toggle the task's stream status
			if (!streamStatus.equals(streamStatusMaintainer.getStreamStatus())) {
				if (streamStatus.isActive()) {
					// we're no longer idle if at least one input has become active
					streamStatusMaintainer.toggleStreamStatus(StreamStatus.ACTIVE);
				} else if (streamStatusTracker.allStreamStatusesAreIdle()) {
					streamStatusMaintainer.toggleStreamStatus(StreamStatus.IDLE);
				}
			}
		}

		@Override
		public void emitLatencyMarker(LatencyMarker latencyMarker) throws Exception {
			input.processLatencyMarker(latencyMarker);
		}
	}

	@SuppressWarnings({"unchecked", "rawtypes"})
	private static class StreamTaskSourceOutput extends SourceOperatorStreamTask.AsyncDataOutputToOutput {
		private final int inputIndex;
		private final MultiStreamStreamStatusTracker streamStatusTracker;

		public StreamTaskSourceOutput(
				Output<StreamRecord<?>> chainedSourceOutput,
				StreamStatusMaintainer streamStatusMaintainer,
				WatermarkGauge inputWatermarkGauge,
				MultiStreamStreamStatusTracker streamStatusTracker,
				int inputIndex) {
			super(chainedSourceOutput, streamStatusMaintainer, inputWatermarkGauge);
			this.streamStatusTracker = streamStatusTracker;
			this.inputIndex = inputIndex;
		}

		@Override
		public void emitStreamStatus(StreamStatus streamStatus) {
			streamStatusTracker.setStreamStatus(inputIndex, streamStatus);

			// check if we need to toggle the task's stream status
			if (!streamStatus.equals(streamStatusMaintainer.getStreamStatus())) {
				if (streamStatus.isActive()) {
					// we're no longer idle if at least one input has become active
					streamStatusMaintainer.toggleStreamStatus(StreamStatus.ACTIVE);
				} else if (streamStatusTracker.allStreamStatusesAreIdle()) {
					streamStatusMaintainer.toggleStreamStatus(StreamStatus.IDLE);
				}
			}
		}
	}
}
