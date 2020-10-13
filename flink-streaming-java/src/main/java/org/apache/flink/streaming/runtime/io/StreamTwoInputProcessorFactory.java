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
import org.apache.flink.api.common.typeutils.TypeSerializer;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.core.memory.ManagedMemoryUseCase;
import org.apache.flink.metrics.Counter;
import org.apache.flink.runtime.io.disk.iomanager.IOManager;
import org.apache.flink.runtime.jobgraph.tasks.AbstractInvokable;
import org.apache.flink.runtime.memory.MemoryManager;
import org.apache.flink.runtime.metrics.groups.TaskIOMetricGroup;
import org.apache.flink.streaming.api.graph.StreamConfig;
import org.apache.flink.streaming.api.operators.BoundedMultiInput;
import org.apache.flink.streaming.api.operators.InputSelectable;
import org.apache.flink.streaming.api.operators.TwoInputStreamOperator;
import org.apache.flink.streaming.api.operators.sort.MultiInputSortingDataInput;
import org.apache.flink.streaming.api.operators.sort.MultiInputSortingDataInput.SelectableSortingInputs;
import org.apache.flink.streaming.api.watermark.Watermark;
import org.apache.flink.streaming.runtime.metrics.WatermarkGauge;
import org.apache.flink.streaming.runtime.streamrecord.LatencyMarker;
import org.apache.flink.streaming.runtime.streamrecord.StreamRecord;
import org.apache.flink.streaming.runtime.streamstatus.StatusWatermarkValve;
import org.apache.flink.streaming.runtime.streamstatus.StreamStatus;
import org.apache.flink.streaming.runtime.streamstatus.StreamStatusMaintainer;
import org.apache.flink.util.function.ThrowingConsumer;

import static org.apache.flink.util.Preconditions.checkNotNull;

/**
 * A factory for {@link StreamTwoInputProcessor}.
 */
public class StreamTwoInputProcessorFactory {
	public static <IN1, IN2> StreamTwoInputProcessor<IN1, IN2> create(
			AbstractInvokable ownerTask,
			CheckpointedInputGate[] checkpointedInputGates,
			IOManager ioManager,
			MemoryManager memoryManager,
			TaskIOMetricGroup taskIOMetricGroup,
			StreamStatusMaintainer streamStatusMaintainer,
			TwoInputStreamOperator<IN1, IN2, ?> streamOperator,
			WatermarkGauge input1WatermarkGauge,
			WatermarkGauge input2WatermarkGauge,
			BoundedMultiInput endOfInputAware,
			StreamConfig streamConfig,
			Configuration taskManagerConfig,
			Configuration jobConfig,
			ExecutionConfig executionConfig,
			ClassLoader userClassloader,
			Counter numRecordsIn) {

		checkNotNull(endOfInputAware);

		StreamStatusTracker statusTracker = new StreamStatusTracker();
		taskIOMetricGroup.reuseRecordsInputCounter(numRecordsIn);
		TypeSerializer<IN1> typeSerializer1 = streamConfig.getTypeSerializerIn(0, userClassloader);
		StreamTaskInput<IN1> input1 = new StreamTaskNetworkInput<>(
			checkpointedInputGates[0],
			typeSerializer1,
			ioManager,
			new StatusWatermarkValve(checkpointedInputGates[0].getNumberOfInputChannels()),
			0);
		TypeSerializer<IN2> typeSerializer2 = streamConfig.getTypeSerializerIn(1, userClassloader);
		StreamTaskInput<IN2> input2 = new StreamTaskNetworkInput<>(
			checkpointedInputGates[1],
			typeSerializer2,
			ioManager,
			new StatusWatermarkValve(checkpointedInputGates[1].getNumberOfInputChannels()),
			1);

		InputSelectable inputSelectable =
			streamOperator instanceof InputSelectable ? (InputSelectable) streamOperator : null;
		if (streamConfig.shouldSortInputs()) {

			if (inputSelectable != null) {
				throw new IllegalStateException("The InputSelectable interface is not supported with sorting inputs");
			}

			@SuppressWarnings("unchecked")
			SelectableSortingInputs selectableSortingInputs = MultiInputSortingDataInput.wrapInputs(
				ownerTask,
				new StreamTaskInput[]{input1, input2},
				new KeySelector[]{
					streamConfig.getStatePartitioner(0, userClassloader),
					streamConfig.getStatePartitioner(1, userClassloader)},
				new TypeSerializer[]{typeSerializer1, typeSerializer2},
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
			inputSelectable = selectableSortingInputs.getInputSelectable();
			input1 = getSortedInput(selectableSortingInputs.getSortingInputs()[0]);
			input2 = getSortedInput(selectableSortingInputs.getSortingInputs()[1]);
		}

		StreamTaskNetworkOutput<IN1> output1 = new StreamTaskNetworkOutput<>(
			streamOperator,
			record -> processRecord1(record, streamOperator),
			streamStatusMaintainer,
			input1WatermarkGauge,
			statusTracker,
			0,
			numRecordsIn);
		StreamOneInputProcessor<IN1> processor1 = new StreamOneInputProcessor<>(
			input1,
			output1,
			endOfInputAware
		);

		StreamTaskNetworkOutput<IN2> output2 = new StreamTaskNetworkOutput<>(
			streamOperator,
			record -> processRecord2(record, streamOperator),
			streamStatusMaintainer,
			input2WatermarkGauge,
			statusTracker,
			1,
			numRecordsIn);
		StreamOneInputProcessor<IN2> processor2 = new StreamOneInputProcessor<>(
			input2,
			output2,
			endOfInputAware
		);

		return new StreamTwoInputProcessor<>(
			new TwoInputSelectionHandler(inputSelectable),
			processor1,
			processor2
		);
	}

	@SuppressWarnings("unchecked")
	private static <IN1> StreamTaskInput<IN1> getSortedInput(StreamTaskInput<?> multiInput) {
		return (StreamTaskInput<IN1>) multiInput;
	}

	private static <T> void processRecord1(
		StreamRecord<T> record,
		TwoInputStreamOperator<T, ?, ?> streamOperator) throws Exception {

		streamOperator.setKeyContextElement1(record);
		streamOperator.processElement1(record);
	}

	private static <T> void processRecord2(
		StreamRecord<T> record,
		TwoInputStreamOperator<?, T, ?> streamOperator) throws Exception {

		streamOperator.setKeyContextElement2(record);
		streamOperator.processElement2(record);
	}

	private static class StreamStatusTracker {
		/**
		 * Stream status for the two inputs. We need to keep track for determining when
		 * to forward stream status changes downstream.
		 */
		private StreamStatus firstStatus = StreamStatus.ACTIVE;
		private StreamStatus secondStatus = StreamStatus.ACTIVE;

		public StreamStatus getFirstStatus() {
			return firstStatus;
		}

		public void setFirstStatus(StreamStatus firstStatus) {
			this.firstStatus = firstStatus;
		}

		public StreamStatus getSecondStatus() {
			return secondStatus;
		}

		public void setSecondStatus(StreamStatus secondStatus) {
			this.secondStatus = secondStatus;
		}
	}

	/**
	 * The network data output implementation used for processing stream elements
	 * from {@link StreamTaskNetworkInput} in two input selective processor.
	 */
	private static class StreamTaskNetworkOutput<T> extends AbstractDataOutput<T> {

		private final TwoInputStreamOperator<?, ?, ?> operator;

		/** The function way is only used for frequent record processing as for JIT optimization. */
		private final ThrowingConsumer<StreamRecord<T>, Exception> recordConsumer;

		private final WatermarkGauge inputWatermarkGauge;

		/** The input index to indicate how to process elements by two input operator. */
		private final int inputIndex;

		private final Counter numRecordsIn;

		private final StreamStatusTracker statusTracker;

		private StreamTaskNetworkOutput(
				TwoInputStreamOperator<?, ?, ?> operator,
				ThrowingConsumer<StreamRecord<T>, Exception> recordConsumer,
				StreamStatusMaintainer streamStatusMaintainer,
				WatermarkGauge inputWatermarkGauge,
				StreamStatusTracker statusTracker,
				int inputIndex,
				Counter numRecordsIn) {
			super(streamStatusMaintainer);

			this.operator = checkNotNull(operator);
			this.recordConsumer = checkNotNull(recordConsumer);
			this.inputWatermarkGauge = checkNotNull(inputWatermarkGauge);
			this.statusTracker = statusTracker;
			this.inputIndex = inputIndex;
			this.numRecordsIn = numRecordsIn;
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
				operator.processWatermark1(watermark);
			} else {
				operator.processWatermark2(watermark);
			}
		}

		@Override
		public void emitStreamStatus(StreamStatus streamStatus) {
			final StreamStatus anotherStreamStatus;
			if (inputIndex == 0) {
				statusTracker.setFirstStatus(streamStatus);
				anotherStreamStatus = statusTracker.getSecondStatus();
			} else {
				statusTracker.setSecondStatus(streamStatus);
				anotherStreamStatus = statusTracker.getFirstStatus();
			}

			// check if we need to toggle the task's stream status
			if (!streamStatus.equals(streamStatusMaintainer.getStreamStatus())) {
				if (streamStatus.isActive()) {
					// we're no longer idle if at least one input has become active
					streamStatusMaintainer.toggleStreamStatus(StreamStatus.ACTIVE);
				} else if (anotherStreamStatus.isIdle()) {
					// we're idle once both inputs are idle
					streamStatusMaintainer.toggleStreamStatus(StreamStatus.IDLE);
				}
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
}
