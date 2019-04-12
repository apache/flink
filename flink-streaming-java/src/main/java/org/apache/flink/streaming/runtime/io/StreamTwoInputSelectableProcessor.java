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
import org.apache.flink.annotation.VisibleForTesting;
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
import org.apache.flink.streaming.runtime.streamstatus.StreamStatus;
import org.apache.flink.streaming.runtime.streamstatus.StreamStatusMaintainer;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.atomic.AtomicInteger;

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

	private volatile boolean continuousProcessing = true;

	private final NetworkInput input1;
	private final NetworkInput input2;

	private final Object lock;

	private final TwoInputStreamOperator<IN1, IN2, ?> streamOperator;

	private final InputSelectable inputSelector;

	private final AuxiliaryHandler auxiliaryHandler;

	private final CompletableFuture<Integer>[] listenFutures;

	private final boolean[] isFinished;

	private InputSelection inputSelection;

	private AtomicInteger availableInputsMask = new AtomicInteger();

	private int lastReadingInputMask;

	private static final int TWO_INPUT_ANY_MASK = (int) new InputSelection.Builder()
		.select(1)
		.select(2)
		.build()
		.getInputMask();

	private static final int INPUT1_ID = 1;
	private static final int INPUT2_ID = 2;

	// ---------------- Metrics ------------------

	private final WatermarkGauge input1WatermarkGauge;
	private final WatermarkGauge input2WatermarkGauge;

	private Counter numRecordsIn;

	@SuppressWarnings("unchecked")
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
		WatermarkGauge input2WatermarkGauge) {

		checkState(streamOperator instanceof InputSelectable);

		// create a NetworkInput instance for each input
		StreamTwoInputStreamStatusHandler streamStatusHandler = new StreamTwoInputStreamStatusHandler(
			streamStatusMaintainer, lock);
		this.input1 = new NetworkInput(INPUT1_ID, inputGates1, inputSerializer1, streamStatusHandler, ioManager);
		this.input2 = new NetworkInput(INPUT2_ID, inputGates2, inputSerializer2, streamStatusHandler, ioManager);

		this.lock = checkNotNull(lock);

		this.streamOperator = checkNotNull(streamOperator);
		this.inputSelector = (InputSelectable) streamOperator;

		this.input1WatermarkGauge = input1WatermarkGauge;
		this.input2WatermarkGauge = input2WatermarkGauge;

		this.auxiliaryHandler = new AuxiliaryHandler();

		this.listenFutures = new CompletableFuture[]{null, null};
		this.isFinished = new boolean[]{false, false};
	}

	/**
	 * Notes that it must be called after calling all operator's open(). This ensures that
	 * the first input selection determined by the operator at and before opening is effective.
	 */
	public void init() {
		inputSelection = inputSelector.nextSelection();

		availableInputsMask.set(TWO_INPUT_ANY_MASK);

		lastReadingInputMask = (int) InputSelection.SECOND.getInputMask();

		if (numRecordsIn == null) {
			try {
				numRecordsIn = ((OperatorMetricGroup) streamOperator
					.getMetricGroup()).getIOMetricGroup().getNumRecordsInCounter();
			} catch (Exception e) {
				LOG.warn("An exception occurred during the metrics setup.", e);
				numRecordsIn = new SimpleCounter();
			}
		}
	}

	public boolean processInput() throws Exception {
		// cache inputs reference on the stack, to make the code more JIT friendly
		final NetworkInput input1 = this.input1;
		final NetworkInput input2 = this.input2;

		while (continuousProcessing) {
			final int selectionMask = (int) inputSelection.getInputMask();
			final int availableMask = availableInputsMask.get();

			int readingInputMask = selectionMask & availableMask;
			if (readingInputMask == TWO_INPUT_ANY_MASK) {
				// the input selection is `ALL` and both inputs are available, we read two inputs in turn
				readingInputMask -= lastReadingInputMask;
			} else if (readingInputMask == 0) {
				// block to wait for a available target input
				waitForAvailableInput(selectionMask);
				continue;
			}

			lastReadingInputMask = readingInputMask;
			int readingInputIndex = readingInputMask - 1;

			final StreamElement element;
			if (readingInputIndex == 0) {
				element = input1.pollNextElement();
				if (element != null) {
					if (element.isRecord()) {
						StreamRecord<IN1> record = element.asRecord();
						synchronized (lock) {
							numRecordsIn.inc();
							streamOperator.setKeyContextElement1(record);
							streamOperator.processElement1(record);
							inputSelection = inputSelector.nextSelection();
						}

						continue;
					} else if (element.isWatermark()) {
						Watermark watermark = element.asWatermark();
						synchronized (lock) {
							input1WatermarkGauge.setCurrentWatermark(watermark.getTimestamp());
							streamOperator.processWatermark1(watermark);
						}
						continue;
					} else {
						auxiliaryHandler.processOtherElement1(element);
						continue;
					}
				}
			} else {
				element = input2.pollNextElement();
				if (element != null) {
					if (element.isRecord()) {
						StreamRecord<IN2> record = element.asRecord();
						synchronized (lock) {
							numRecordsIn.inc();
							streamOperator.setKeyContextElement2(record);
							streamOperator.processElement2(record);
							inputSelection = inputSelector.nextSelection();
						}

						continue;
					} else if (element.isWatermark()) {
						Watermark watermark = element.asWatermark();
						synchronized (lock) {
							input2WatermarkGauge.setCurrentWatermark(watermark.getTimestamp());
							streamOperator.processWatermark2(watermark);
						}
						continue;
					} else {
						auxiliaryHandler.processOtherElement2(element);
						continue;
					}
				}
			}

			/* no data or the input is finished */
			if (!setUnavailableAndListenInput(readingInputIndex)) {
				// if the input is no longer listenable, then it is finished
				if (finishInput(readingInputIndex)) {
					// all inputs have been finished, exit
					return false;
				}
			}
		}

		return true;
	}

	@VisibleForTesting
	public final boolean isContinuousProcessing() {
		return continuousProcessing;
	}

	@VisibleForTesting
	public final void setContinuousProcessing(boolean continuousProcessing) {
		this.continuousProcessing = continuousProcessing;
	}

	/**
	 * It is called by another thread, so it must be thread-safe.
	 */
	public void stop() {
		continuousProcessing = false;

		// cancel all inputs and activate the main loop if it is blocked
		for (Input input : new Input[]{input1, input2}) {
			input.unlisten();
		}
	}

	public void cleanup() {
		// close all inputs
		for (Input input : new Input[]{input1, input2}) {
			try {
				input.close();
			} catch (Throwable t) {
				LOG.warn("An exception occurred when closing input" + input.getId() + ".", t);
			}
		}
	}

	private boolean setUnavailableAndListenInput(int inputIndex) {
		int bitMask = 1 << inputIndex;

		// set `unavailable` flag
		availableInputsMask.getAndUpdate(mask -> mask & (~bitMask));

		// listen the input
		Input input = inputIndex == 0 ? input1 : input2;
		if (!input.isFinished()) {
			listenFutures[inputIndex] = input.listen().thenApply(v -> {
				availableInputsMask.getAndUpdate(mask -> mask | bitMask);
				return inputIndex;
			});

			return true;
		}

		return false;
	}

	private void waitForAvailableInput(int selectionMask) throws IOException, ExecutionException, InterruptedException {
		if (selectionMask == InputSelection.ALL.getInputMask() || selectionMask == TWO_INPUT_ANY_MASK) {
			List<CompletableFuture<Integer>> futures = new ArrayList<>();
			for (CompletableFuture<Integer> future : listenFutures) {
				if (future != null) {
					futures.add(future);
				}
			}
			checkState(futures.size() > 0, "No input in listening.");

			// block to wait for a available input
			CompletableFuture.anyOf(futures.toArray(new CompletableFuture<?>[0])).get();

			// clear all completed futures
			for (CompletableFuture<Integer> future : futures) {
				if (future != null && future.isDone()) {
					listenFutures[future.get()] = null;
				}
			}
		} else {
			int inputIndex = (selectionMask == InputSelection.FIRST.getInputMask()) ? 0 : 1;
			if (isFinished[inputIndex]) {
				throw new IOException("Could not read the finished input: input" + (inputIndex + 1) + ".");
			}

			CompletableFuture<Integer> future = listenFutures[inputIndex];
			checkState(future != null, "No input in listening.");

			// block to wait the input being available
			listenFutures[future.get()] = null;
		}
	}

	private boolean finishInput(int inputIndex) throws Exception {
		if (inputIndex == 0) {
			auxiliaryHandler.endInput1();
			inputSelection = InputSelection.SECOND;
		} else {
			auxiliaryHandler.endInput2();
			inputSelection = InputSelection.FIRST;
		}

		isFinished[inputIndex] = true;
		return isFinished[1 - inputIndex];
	}

	private final class AuxiliaryHandler {

		public void processOtherElement1(StreamElement element) throws Exception {
			if (element.isLatencyMarker()) {
				synchronized (lock) {
					streamOperator.processLatencyMarker1(element.asLatencyMarker());
				}
			} else {
				throw new IOException("Unexpected element type on input1: " + element.getClass().getName());
			}
		}

		public void processOtherElement2(StreamElement element) throws Exception {
			if (element.isLatencyMarker()) {
				synchronized (lock) {
					streamOperator.processLatencyMarker2(element.asLatencyMarker());
				}
			} else {
				throw new IOException("Unexpected element type on input2: " + element.getClass().getName());
			}
		}

		public void endInput1() throws Exception {
			// TODO: implements the runtime handling of the BoundedMultiInput interface
		}

		public void endInput2() throws Exception {
			// TODO: implements the runtime handling of the BoundedMultiInput interface
		}
	}

	/**
	 * The {@link StreamStatus} handler for the two-input stream task.
	 */
	private static class StreamTwoInputStreamStatusHandler implements StreamStatusHandler {

		private final StreamStatusMaintainer streamStatusMaintainer;

		private final Object lock;

		/**
		 * Stream status for the two inputs. We need to keep track for determining when
		 * to forward stream status changes downstream.
		 */
		private StreamStatus streamStatus1;
		private StreamStatus streamStatus2;

		public StreamTwoInputStreamStatusHandler(StreamStatusMaintainer streamStatusMaintainer, Object lock) {
			this.streamStatusMaintainer = checkNotNull(streamStatusMaintainer);
			this.lock = checkNotNull(lock);

			this.streamStatus1 = StreamStatus.ACTIVE;
			this.streamStatus2 = StreamStatus.ACTIVE;
		}

		@Override
		public void handleStreamStatus(int inputId, StreamStatus streamStatus) {
			checkState(inputId >= 1 && inputId <= 2);

			try {
				synchronized (lock) {
					final StreamStatus anotherStreamStatus;
					if (inputId == INPUT1_ID) {
						streamStatus1 = streamStatus;
						anotherStreamStatus = streamStatus2;
					} else {
						streamStatus2 = streamStatus;
						anotherStreamStatus = streamStatus1;
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
				throw new RuntimeException("Exception occurred while processing valve output stream status: ", e);
			}
		}
	}
}
