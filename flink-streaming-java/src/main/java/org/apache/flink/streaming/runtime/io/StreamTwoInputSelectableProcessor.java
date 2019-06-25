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
import org.apache.flink.metrics.Counter;
import org.apache.flink.metrics.SimpleCounter;
import org.apache.flink.runtime.io.disk.iomanager.IOManager;
import org.apache.flink.runtime.io.network.partition.consumer.InputGate;
import org.apache.flink.runtime.metrics.groups.OperatorMetricGroup;
import org.apache.flink.streaming.api.operators.InputSelectable;
import org.apache.flink.streaming.api.operators.InputSelection;
import org.apache.flink.streaming.api.operators.TwoInputStreamOperator;
import org.apache.flink.streaming.api.watermark.Watermark;
import org.apache.flink.streaming.runtime.metrics.WatermarkGauge;
import org.apache.flink.streaming.runtime.streamrecord.StreamElement;
import org.apache.flink.streaming.runtime.streamrecord.StreamRecord;
import org.apache.flink.streaming.runtime.streamstatus.StatusWatermarkValve;
import org.apache.flink.streaming.runtime.streamstatus.StreamStatus;
import org.apache.flink.streaming.runtime.streamstatus.StreamStatusMaintainer;
import org.apache.flink.streaming.runtime.tasks.OperatorChain;
import org.apache.flink.util.ExceptionUtils;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.Collection;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;

import static org.apache.flink.util.Preconditions.checkNotNull;
import static org.apache.flink.util.Preconditions.checkState;

/**
 * Input reader for {@link org.apache.flink.streaming.runtime.tasks.TwoInputSelectableStreamTask}
 * in the case that the operator is InputSelectable.
 *
 * @param <IN1> The type of the records that arrive on the first input
 * @param <IN2> The type of the records that arrive on the second input
 */
@Internal
public class StreamTwoInputSelectableProcessor<IN1, IN2> {

	private static final Logger LOG = LoggerFactory.getLogger(StreamTwoInputSelectableProcessor.class);

	private static final CompletableFuture<?> UNAVAILABLE = new CompletableFuture<>();

	private final TwoInputStreamOperator<IN1, IN2, ?> streamOperator;
	private final InputSelectable inputSelector;

	private final Object lock;

	private final StreamTaskInput input1;
	private final StreamTaskInput input2;

	private final OperatorChain<?, ?> operatorChain;

	/**
	 * Valves that control how watermarks and stream statuses from the 2 inputs are forwarded.
	 */
	private final StatusWatermarkValve statusWatermarkValve1;
	private final StatusWatermarkValve statusWatermarkValve2;

	/**
	 * Stream status for the two inputs. We need to keep track for determining when
	 * to forward stream status changes downstream.
	 */
	private StreamStatus firstStatus;
	private StreamStatus secondStatus;

	private int availableInputsMask;

	private int lastReadInputIndex;

	private InputSelection inputSelection;

	private Counter numRecordsIn;

	private boolean isPrepared;

	public StreamTwoInputSelectableProcessor(
		Collection<InputGate> inputGates1,
		Collection<InputGate> inputGates2,
		TypeSerializer<IN1> inputSerializer1,
		TypeSerializer<IN2> inputSerializer2,
		Object lock,
		IOManager ioManager,
		StreamStatusMaintainer streamStatusMaintainer,
		TwoInputStreamOperator<IN1, IN2, ?> streamOperator,
		WatermarkGauge input1WatermarkGauge,
		WatermarkGauge input2WatermarkGauge,
		OperatorChain<?, ?> operatorChain) {

		checkState(streamOperator instanceof InputSelectable);

		this.streamOperator = checkNotNull(streamOperator);
		this.inputSelector = (InputSelectable) streamOperator;

		this.lock = checkNotNull(lock);

		InputGate unionedInputGate1 = InputGateUtil.createInputGate(inputGates1.toArray(new InputGate[0]));
		InputGate unionedInputGate2 = InputGateUtil.createInputGate(inputGates2.toArray(new InputGate[0]));

		// create a Input instance for each input
		this.input1 = new StreamTaskNetworkInput(new BarrierDiscarder(unionedInputGate1), inputSerializer1, ioManager, 0);
		this.input2 = new StreamTaskNetworkInput(new BarrierDiscarder(unionedInputGate2), inputSerializer2, ioManager, 1);

		this.statusWatermarkValve1 = new StatusWatermarkValve(
			unionedInputGate1.getNumberOfInputChannels(),
			new ForwardingValveOutputHandler(streamOperator, lock, streamStatusMaintainer, input1WatermarkGauge, 0));
		this.statusWatermarkValve2 = new StatusWatermarkValve(
			unionedInputGate2.getNumberOfInputChannels(),
			new ForwardingValveOutputHandler(streamOperator, lock, streamStatusMaintainer, input2WatermarkGauge, 1));

		this.operatorChain = checkNotNull(operatorChain);

		this.firstStatus = StreamStatus.ACTIVE;
		this.secondStatus = StreamStatus.ACTIVE;

		this.availableInputsMask = (int) new InputSelection.Builder().select(1).select(2).build().getInputMask();

		this.lastReadInputIndex = 1; // always try to read from the first input

		this.isPrepared = false;

	}

	public boolean processInput() throws Exception {
		if (!isPrepared) {
			// the preparations here are not placed in the constructor because all work in it
			// must be executed after all operators are opened.
			prepareForProcessing();
		}

		int readingInputIndex = selectNextReadingInputIndex();
		if (readingInputIndex == -1) {
			return false;
		}
		lastReadInputIndex = readingInputIndex;

		StreamElement recordOrMark;
		if (readingInputIndex == 0) {
			recordOrMark = input1.pollNextNullable();
			if (recordOrMark != null) {
				processElement1(recordOrMark, input1.getLastChannel());
			}
		} else {
			recordOrMark = input2.pollNextNullable();
			if (recordOrMark != null) {
				processElement2(recordOrMark, input2.getLastChannel());
			}
		}

		if (recordOrMark == null) {
			setUnavailableInput(readingInputIndex);
		}

		return !checkFinished();
	}

	public void cleanup() throws Exception {
		Exception ex = null;
		try {
			input1.close();
		} catch (Exception e) {
			ex = ExceptionUtils.firstOrSuppressed(e, ex);
		}

		try {
			input2.close();
		} catch (Exception e) {
			ex = ExceptionUtils.firstOrSuppressed(e, ex);
		}

		if (ex != null) {
			throw ex;
		}
	}

	private int selectNextReadingInputIndex()
		throws InterruptedException, ExecutionException, IOException {

		int readingInputIndex;
		while ((readingInputIndex = inputSelection.fairSelectNextIndexOutOf2(availableInputsMask, lastReadInputIndex)) == -1) {
			if (!waitForAvailableInput(inputSelection)) {
				return -1;
			}
		}

		// to avoid starvation, if the input selection is ALL and availableInputsMask is not ALL,
		// always try to check and set the availability of another input
		// TODO: because this can be a costly operation (checking volatile inside CompletableFuture`
		//  this might be optimized to only check once per processed NetworkBuffer
		if (availableInputsMask < 3 && inputSelection.isALLMaskOf2()) {
			checkAndSetAvailable(1 - readingInputIndex);
		}

		return readingInputIndex;
	}

	private void processElement1(StreamElement recordOrMark, int channel) throws Exception {
		if (recordOrMark.isRecord()) {
			StreamRecord<IN1> record = recordOrMark.asRecord();
			synchronized (lock) {
				numRecordsIn.inc();
				streamOperator.setKeyContextElement1(record);
				streamOperator.processElement1(record);
				inputSelection = inputSelector.nextSelection();
			}
		}
		else if (recordOrMark.isWatermark()) {
			statusWatermarkValve1.inputWatermark(recordOrMark.asWatermark(), channel);
		} else if (recordOrMark.isStreamStatus()) {
			statusWatermarkValve1.inputStreamStatus(recordOrMark.asStreamStatus(), channel);
		} else if (recordOrMark.isLatencyMarker()) {
			synchronized (lock) {
				streamOperator.processLatencyMarker1(recordOrMark.asLatencyMarker());
			}
		} else {
			throw new UnsupportedOperationException("Unknown type of StreamElement on input1");
		}
	}

	private void processElement2(StreamElement recordOrMark, int channel) throws Exception {
		if (recordOrMark.isRecord()) {
			StreamRecord<IN2> record = recordOrMark.asRecord();
			synchronized (lock) {
				numRecordsIn.inc();
				streamOperator.setKeyContextElement2(record);
				streamOperator.processElement2(record);
				inputSelection = inputSelector.nextSelection();
			}
		}
		else if (recordOrMark.isWatermark()) {
			statusWatermarkValve2.inputWatermark(recordOrMark.asWatermark(), channel);
		} else if (recordOrMark.isStreamStatus()) {
			statusWatermarkValve2.inputStreamStatus(recordOrMark.asStreamStatus(), channel);
		} else if (recordOrMark.isLatencyMarker()) {
			synchronized (lock) {
				streamOperator.processLatencyMarker2(recordOrMark.asLatencyMarker());
			}
		} else {
			throw new UnsupportedOperationException("Unknown type of StreamElement on input2");
		}
	}

	private void prepareForProcessing() {
		// Note: the first call to nextSelection () on the operator must be made after this operator
		// is opened to ensure that any changes about the input selection in its open()
		// method take effect.
		inputSelection = inputSelector.nextSelection();

		try {
			numRecordsIn = ((OperatorMetricGroup) streamOperator
				.getMetricGroup()).getIOMetricGroup().getNumRecordsInCounter();
		} catch (Exception e) {
			LOG.warn("An exception occurred during the metrics setup.", e);
			numRecordsIn = new SimpleCounter();
		}

		isPrepared = true;
	}

	private void checkAndSetAvailable(int inputIndex) {
		StreamTaskInput input = getInput(inputIndex);
		if (!input.isFinished() && input.isAvailable().isDone()) {
			setAvailableInput(inputIndex);
		}
	}

	/**
	 * @return false if both of the inputs are finished, true otherwise.
	 */
	private boolean waitForAvailableInput(InputSelection inputSelection)
		throws ExecutionException, InterruptedException, IOException {

		if (inputSelection.isALLMaskOf2()) {
			return waitForAvailableEitherInput();
		} else {
			waitForOneInput(
				(inputSelection.getInputMask() == InputSelection.FIRST.getInputMask()) ? input1 : input2);
			return true;
		}
	}

	private boolean waitForAvailableEitherInput()
		throws ExecutionException, InterruptedException {

		CompletableFuture<?> future1 = input1.isFinished() ? UNAVAILABLE : input1.isAvailable();
		CompletableFuture<?> future2 = input2.isFinished() ? UNAVAILABLE : input2.isAvailable();

		if (future1 == UNAVAILABLE && future2 == UNAVAILABLE) {
			return false;
		}

		// block to wait for a available input
		CompletableFuture.anyOf(future1, future2).get();

		if (future1.isDone()) {
			setAvailableInput(input1.getInputIndex());
		}
		if (future2.isDone()) {
			setAvailableInput(input2.getInputIndex());
		}

		return true;
	}

	private void waitForOneInput(StreamTaskInput input)
		throws IOException, ExecutionException, InterruptedException {

		if (input.isFinished()) {
			throw new IOException("Could not read the finished input: input" + (input.getInputIndex() + 1) +  ".");
		}

		input.isAvailable().get();
		setAvailableInput(input.getInputIndex());
	}

	private boolean checkFinished() throws Exception {
		if (getInput(lastReadInputIndex).isFinished()) {
			synchronized (lock) {
				operatorChain.endInput(getInputId(lastReadInputIndex));
				inputSelection = inputSelector.nextSelection();
			}
		}

		return input1.isFinished() && input2.isFinished();
	}

	private void setAvailableInput(int inputIndex) {
		availableInputsMask |= 1 << inputIndex;
	}

	private void setUnavailableInput(int inputIndex) {
		availableInputsMask &= ~(1 << inputIndex);
	}

	private StreamTaskInput getInput(int inputIndex) {
		return inputIndex == 0 ? input1 : input2;
	}

	private int getInputId(int inputIndex) {
		return inputIndex + 1;
	}

	private class ForwardingValveOutputHandler implements StatusWatermarkValve.ValveOutputHandler {

		private final TwoInputStreamOperator<IN1, IN2, ?> operator;

		private final Object lock;

		private final StreamStatusMaintainer streamStatusMaintainer;

		private final WatermarkGauge inputWatermarkGauge;

		private final int inputIndex;

		private ForwardingValveOutputHandler(
			TwoInputStreamOperator<IN1, IN2, ?> operator,
			Object lock,
			StreamStatusMaintainer streamStatusMaintainer,
			WatermarkGauge inputWatermarkGauge,
			int inputIndex) {

			this.operator = checkNotNull(operator);
			this.lock = checkNotNull(lock);

			this.streamStatusMaintainer = checkNotNull(streamStatusMaintainer);

			this.inputWatermarkGauge = inputWatermarkGauge;

			this.inputIndex = inputIndex;
		}

		@Override
		public void handleWatermark(Watermark watermark) {
			try {
				synchronized (lock) {
					inputWatermarkGauge.setCurrentWatermark(watermark.getTimestamp());
					if (inputIndex == 0) {
						operator.processWatermark1(watermark);
					} else {
						operator.processWatermark2(watermark);
					}
				}
			} catch (Exception e) {
				throw new RuntimeException("Exception occurred while processing valve output watermark of input"
					+ (inputIndex + 1) + ": ", e);
			}
		}

		@Override
		public void handleStreamStatus(StreamStatus streamStatus) {
			try {
				synchronized (lock) {
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
			} catch (Exception e) {
				throw new RuntimeException("Exception occurred while processing valve output stream status of input"
					+ (inputIndex + 1) + ": ", e);
			}
		}
	}
}
