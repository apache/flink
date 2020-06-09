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
import org.apache.flink.api.common.typeutils.TypeSerializer;
import org.apache.flink.core.io.InputStatus;
import org.apache.flink.metrics.Counter;
import org.apache.flink.runtime.checkpoint.channel.ChannelStateWriter;
import org.apache.flink.runtime.io.disk.iomanager.IOManager;
import org.apache.flink.streaming.api.operators.InputSelection;
import org.apache.flink.streaming.api.operators.TwoInputStreamOperator;
import org.apache.flink.streaming.api.watermark.Watermark;
import org.apache.flink.streaming.runtime.io.PushingAsyncDataInput.DataOutput;
import org.apache.flink.streaming.runtime.metrics.WatermarkGauge;
import org.apache.flink.streaming.runtime.streamrecord.LatencyMarker;
import org.apache.flink.streaming.runtime.streamrecord.StreamRecord;
import org.apache.flink.streaming.runtime.streamstatus.StatusWatermarkValve;
import org.apache.flink.streaming.runtime.streamstatus.StreamStatus;
import org.apache.flink.streaming.runtime.streamstatus.StreamStatusMaintainer;
import org.apache.flink.streaming.runtime.tasks.OperatorChain;
import org.apache.flink.streaming.runtime.tasks.TwoInputStreamTask;
import org.apache.flink.util.ExceptionUtils;
import org.apache.flink.util.function.ThrowingConsumer;

import java.io.IOException;
import java.util.concurrent.CompletableFuture;

import static org.apache.flink.util.Preconditions.checkNotNull;

/**
 * Input reader for {@link TwoInputStreamTask}.
 *
 * @param <IN1> The type of the records that arrive on the first input
 * @param <IN2> The type of the records that arrive on the second input
 */
@Internal
public final class StreamTwoInputProcessor<IN1, IN2> implements StreamInputProcessor {

	private final TwoInputSelectionHandler inputSelectionHandler;

	private final StreamTaskInput<IN1> input1;
	private final StreamTaskInput<IN2> input2;

	private final OperatorChain<?, ?> operatorChain;

	private final DataOutput<IN1> output1;
	private final DataOutput<IN2> output2;

	/** Input status to keep track for determining whether the input is finished or not. */
	private InputStatus firstInputStatus = InputStatus.MORE_AVAILABLE;
	private InputStatus secondInputStatus = InputStatus.MORE_AVAILABLE;

	/**
	 * Stream status for the two inputs. We need to keep track for determining when
	 * to forward stream status changes downstream.
	 */
	private StreamStatus firstStatus = StreamStatus.ACTIVE;
	private StreamStatus secondStatus = StreamStatus.ACTIVE;

	/** Always try to read from the first input. */
	private int lastReadInputIndex = 1;

	private boolean isPrepared;

	public StreamTwoInputProcessor(
			CheckpointedInputGate[] checkpointedInputGates,
			TypeSerializer<IN1> inputSerializer1,
			TypeSerializer<IN2> inputSerializer2,
			IOManager ioManager,
			StreamStatusMaintainer streamStatusMaintainer,
			TwoInputStreamOperator<IN1, IN2, ?> streamOperator,
			TwoInputSelectionHandler inputSelectionHandler,
			WatermarkGauge input1WatermarkGauge,
			WatermarkGauge input2WatermarkGauge,
			OperatorChain<?, ?> operatorChain,
			Counter numRecordsIn) {

		this.inputSelectionHandler = checkNotNull(inputSelectionHandler);

		this.output1 = new StreamTaskNetworkOutput<>(
			streamOperator,
			record -> processRecord1(record, streamOperator, numRecordsIn),
			streamStatusMaintainer,
			input1WatermarkGauge,
			0);
		this.output2 = new StreamTaskNetworkOutput<>(
			streamOperator,
			record -> processRecord2(record, streamOperator, numRecordsIn),
			streamStatusMaintainer,
			input2WatermarkGauge,
			1);

		this.input1 = new StreamTaskNetworkInput<>(
			checkpointedInputGates[0],
			inputSerializer1,
			ioManager,
			new StatusWatermarkValve(checkpointedInputGates[0].getNumberOfInputChannels(), output1),
			0);
		this.input2 = new StreamTaskNetworkInput<>(
			checkpointedInputGates[1],
			inputSerializer2,
			ioManager,
			new StatusWatermarkValve(checkpointedInputGates[1].getNumberOfInputChannels(), output2),
			1);

		this.operatorChain = checkNotNull(operatorChain);
	}

	private void processRecord1(
			StreamRecord<IN1> record,
			TwoInputStreamOperator<IN1, IN2, ?> streamOperator,
			Counter numRecordsIn) throws Exception {

		streamOperator.setKeyContextElement1(record);
		streamOperator.processElement1(record);
		postProcessRecord(numRecordsIn);
	}

	private void processRecord2(
			StreamRecord<IN2> record,
			TwoInputStreamOperator<IN1, IN2, ?> streamOperator,
			Counter numRecordsIn) throws Exception {

		streamOperator.setKeyContextElement2(record);
		streamOperator.processElement2(record);
		postProcessRecord(numRecordsIn);
	}

	private void postProcessRecord(Counter numRecordsIn) {
		numRecordsIn.inc();
		inputSelectionHandler.nextSelection();
	}

	@Override
	public CompletableFuture<?> getAvailableFuture() {
		if (inputSelectionHandler.areAllInputsSelected()) {
			return isAnyInputAvailable();
		} else {
			StreamTaskInput input = (inputSelectionHandler.isFirstInputSelected()) ? input1 : input2;
			return input.getAvailableFuture();
		}
	}

	@Override
	public InputStatus processInput() throws Exception {
		int readingInputIndex;
		if (isPrepared) {
			readingInputIndex = selectNextReadingInputIndex();
			assert readingInputIndex != InputSelection.NONE_AVAILABLE;
		} else {
			// the preparations here are not placed in the constructor because all work in it
			// must be executed after all operators are opened.
			readingInputIndex = selectFirstReadingInputIndex();
			if (readingInputIndex == InputSelection.NONE_AVAILABLE) {
				return InputStatus.NOTHING_AVAILABLE;
			}
		}

		lastReadInputIndex = readingInputIndex;

		if (readingInputIndex == 0) {
			firstInputStatus = input1.emitNext(output1);
			checkFinished(firstInputStatus, lastReadInputIndex);
		} else {
			secondInputStatus = input2.emitNext(output2);
			checkFinished(secondInputStatus, lastReadInputIndex);
		}

		return getInputStatus();
	}

	@Override
	public CompletableFuture<Void> prepareSnapshot(
			ChannelStateWriter channelStateWriter,
			long checkpointId) throws IOException {
		return CompletableFuture.allOf(
			input1.prepareSnapshot(channelStateWriter, checkpointId),
			input2.prepareSnapshot(channelStateWriter, checkpointId));
	}

	private int selectFirstReadingInputIndex() throws IOException {
		// Note: the first call to nextSelection () on the operator must be made after this operator
		// is opened to ensure that any changes about the input selection in its open()
		// method take effect.
		inputSelectionHandler.nextSelection();

		isPrepared = true;

		return selectNextReadingInputIndex();
	}

	private void checkFinished(InputStatus status, int inputIndex) throws Exception {
		if (status == InputStatus.END_OF_INPUT) {
			operatorChain.endHeadOperatorInput(getInputId(inputIndex));
			inputSelectionHandler.nextSelection();
		}
	}

	private InputStatus getInputStatus() {
		if (firstInputStatus == InputStatus.END_OF_INPUT && secondInputStatus == InputStatus.END_OF_INPUT) {
			return InputStatus.END_OF_INPUT;
		}

		if (inputSelectionHandler.areAllInputsSelected()) {
			if (firstInputStatus == InputStatus.MORE_AVAILABLE || secondInputStatus == InputStatus.MORE_AVAILABLE) {
				return InputStatus.MORE_AVAILABLE;
			} else {
				return InputStatus.NOTHING_AVAILABLE;
			}
		}

		InputStatus selectedStatus = inputSelectionHandler.isFirstInputSelected() ? firstInputStatus : secondInputStatus;
		InputStatus otherStatus = inputSelectionHandler.isFirstInputSelected() ? secondInputStatus : firstInputStatus;
		return selectedStatus == InputStatus.END_OF_INPUT ? otherStatus : selectedStatus;
	}

	@Override
	public void close() throws IOException {
		IOException ex = null;
		try {
			input1.close();
		} catch (IOException e) {
			ex = ExceptionUtils.firstOrSuppressed(e, ex);
		}

		try {
			input2.close();
		} catch (IOException e) {
			ex = ExceptionUtils.firstOrSuppressed(e, ex);
		}

		if (ex != null) {
			throw ex;
		}
	}

	private int selectNextReadingInputIndex() throws IOException {
		updateAvailability();
		checkInputSelectionAgainstIsFinished();

		int readingInputIndex = inputSelectionHandler.selectNextInputIndex(lastReadInputIndex);
		if (readingInputIndex == InputSelection.NONE_AVAILABLE) {
			return InputSelection.NONE_AVAILABLE;
		}

		// to avoid starvation, if the input selection is ALL and availableInputsMask is not ALL,
		// always try to check and set the availability of another input
		if (inputSelectionHandler.shouldSetAvailableForAnotherInput()) {
			checkAndSetAvailable(1 - readingInputIndex);
		}

		return readingInputIndex;
	}

	private void checkInputSelectionAgainstIsFinished() throws IOException {
		if (inputSelectionHandler.areAllInputsSelected()) {
			return;
		}
		if (inputSelectionHandler.isFirstInputSelected() && firstInputStatus == InputStatus.END_OF_INPUT) {
			throw new IOException("Can not make a progress: only first input is selected but it is already finished");
		}
		if (inputSelectionHandler.isSecondInputSelected() && secondInputStatus == InputStatus.END_OF_INPUT) {
			throw new IOException("Can not make a progress: only second input is selected but it is already finished");
		}
	}

	private void updateAvailability() {
		updateAvailability(firstInputStatus, input1);
		updateAvailability(secondInputStatus, input2);
	}

	private void updateAvailability(InputStatus status, StreamTaskInput input) {
		if (status == InputStatus.MORE_AVAILABLE || (status != InputStatus.END_OF_INPUT && input.isApproximatelyAvailable())) {
			inputSelectionHandler.setAvailableInput(input.getInputIndex());
		} else {
			inputSelectionHandler.setUnavailableInput(input.getInputIndex());
		}
	}

	private void checkAndSetAvailable(int inputIndex) {
		InputStatus status = (inputIndex == 0 ? firstInputStatus : secondInputStatus);
		if (status == InputStatus.END_OF_INPUT) {
			return;
		}

		// TODO: isAvailable() can be a costly operation (checking volatile). If one of
		// the input is constantly available and another is not, we will be checking this volatile
		// once per every record. This might be optimized to only check once per processed NetworkBuffer
		if (getInput(inputIndex).isAvailable()) {
			inputSelectionHandler.setAvailableInput(inputIndex);
		}
	}

	private CompletableFuture<?> isAnyInputAvailable() {
		if (firstInputStatus == InputStatus.END_OF_INPUT) {
			return input2.getAvailableFuture();
		}

		if (secondInputStatus == InputStatus.END_OF_INPUT) {
			return input1.getAvailableFuture();
		}

		return (input1.isApproximatelyAvailable() || input2.isApproximatelyAvailable()) ?
			AVAILABLE : CompletableFuture.anyOf(input1.getAvailableFuture(), input2.getAvailableFuture());
	}

	private StreamTaskInput getInput(int inputIndex) {
		return inputIndex == 0 ? input1 : input2;
	}

	private int getInputId(int inputIndex) {
		return inputIndex + 1;
	}

	/**
	 * The network data output implementation used for processing stream elements
	 * from {@link StreamTaskNetworkInput} in two input selective processor.
	 */
	private class StreamTaskNetworkOutput<T> extends AbstractDataOutput<T> {

		private final TwoInputStreamOperator<IN1, IN2, ?> operator;

		/** The function way is only used for frequent record processing as for JIT optimization. */
		private final ThrowingConsumer<StreamRecord<T>, Exception> recordConsumer;

		private final WatermarkGauge inputWatermarkGauge;

		/** The input index to indicate how to process elements by two input operator. */
		private final int inputIndex;

		private StreamTaskNetworkOutput(
				TwoInputStreamOperator<IN1, IN2, ?> operator,
				ThrowingConsumer<StreamRecord<T>, Exception> recordConsumer,
				StreamStatusMaintainer streamStatusMaintainer,
				WatermarkGauge inputWatermarkGauge,
				int inputIndex) {
			super(streamStatusMaintainer);

			this.operator = checkNotNull(operator);
			this.recordConsumer = checkNotNull(recordConsumer);
			this.inputWatermarkGauge = checkNotNull(inputWatermarkGauge);
			this.inputIndex = inputIndex;
		}

		@Override
		public void emitRecord(StreamRecord<T> record) throws Exception {
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
				firstStatus = streamStatus;
				anotherStreamStatus = secondStatus;
			} else {
				secondStatus = streamStatus;
				anotherStreamStatus = firstStatus;
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
