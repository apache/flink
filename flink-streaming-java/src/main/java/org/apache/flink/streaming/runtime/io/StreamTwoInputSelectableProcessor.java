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
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

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

	private final Object lock;

	// -------------------------------------------------------
	//  Immutable members after initialization
	// -------------------------------------------------------

	private NetworkInput input1;
	private NetworkInput input2;

	private TwoInputStreamOperator<IN1, IN2, ?> streamOperator;

	private InputSelectable inputSelector;

	private AuxiliaryHandler auxiliaryHandler;

	// -------------------------------------------------------
	//  State variables
	// -------------------------------------------------------

	private volatile boolean running = true;

	private CompletableFuture<Integer>[] listenFutures;

	private boolean[] isFinished;

	private InputSelection inputSelection;

	private int availableInputsMask = (int) new InputSelection.Builder()
		.select(1)
		.select(2)
		.build()
		.getInputMask();

	// ---------------- Metrics ------------------

	private WatermarkGauge input1WatermarkGauge;
	private WatermarkGauge input2WatermarkGauge;

	private Counter numRecordsIn;

	public StreamTwoInputSelectableProcessor(Object lock) {
		this.lock = checkNotNull(lock);
	}

	@SuppressWarnings("unchecked")
	public void init(
		Collection<InputGate> inputGates1,
		Collection<InputGate> inputGates2,
		TypeSerializer<IN1> inputSerializer1,
		TypeSerializer<IN2> inputSerializer2,
		IOManager ioManager,
		StreamStatusMaintainer streamStatusMaintainer,
		TwoInputStreamOperator<IN1, IN2, ?> streamOperator,
		WatermarkGauge input1WatermarkGauge,
		WatermarkGauge input2WatermarkGauge) {

		checkState(streamOperator instanceof InputSelectable);

		// create a NetworkInput instance for each input
		StreamTwoInputStreamStatusHandler streamStatusHandler = new StreamTwoInputStreamStatusHandler(
			streamStatusMaintainer, lock);
		this.input1 = new NetworkInput(0, inputGates1, inputSerializer1, streamStatusHandler, ioManager);
		this.input2 = new NetworkInput(1, inputGates2, inputSerializer2, streamStatusHandler, ioManager);

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
	public void prepareProcess() {
		inputSelection = inputSelector.nextSelection();

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

	public void loopProcessingInput() throws Exception {
		// cache inputs reference on the stack, to make the code more JIT friendly
		final NetworkInput input1 = this.input1;
		final NetworkInput input2 = this.input2;

		// always try to read from the first input
		int lastReadInputIndex = 1;

		while (running) {
			int readingInputIndex = inputSelection.fairSelectNextIndexOutOf2(availableInputsMask, lastReadInputIndex);
			if (readingInputIndex > -1) {
				// if the input selection is `ALL`, always try to check and
				// set the availability of another input
				if (inputSelection.isALLMaskOf2() && availableInputsMask < 3) {
					int anotherInputIndex = 1 - readingInputIndex;
					if (!isFinished[anotherInputIndex]) {
						checkAndSetAvailableInput(listenFutures[anotherInputIndex], 0);
					}
				}
			} else {
				// block to wait for a available target input
				try {
					// set timeout so that the task can be canceled properly
					waitForAvailableInput(inputSelection, 2000);
				} catch (TimeoutException te) {
					// do nothing
				}
				continue;
			}

			lastReadInputIndex = readingInputIndex;

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
					return;
				}
			}
		}
	}

	@VisibleForTesting
	public void start() {
		running = true;
	}

	public void stop() {
		running = false;
	}

	public void cleanup() {
		// close all inputs
		for (Input input : new Input[]{input1, input2}) {
			try {
				if (input != null) {
					input.close();
				}
			} catch (Throwable t) {
				LOG.warn("An exception occurred when closing input" + (input.getInputIndex() + 1) + ".", t);
			}
		}
	}

	private boolean setUnavailableAndListenInput(int inputIndex) {
		// set `unavailable` flag
		setUnavailableInput(inputIndex);

		// listen the input
		Input input = inputIndex == 0 ? input1 : input2;
		if (!input.isFinished()) {
			listenFutures[inputIndex] = input.listen().thenApply(v -> inputIndex);
			return true;
		}

		return false;
	}

	private void waitForAvailableInput(InputSelection inputSelection, long maxWaitMillis)
		throws IOException, ExecutionException, InterruptedException, TimeoutException {

		if (inputSelection.isALLMaskOf2()) {
			List<CompletableFuture<Integer>> futures = new ArrayList<>();
			for (CompletableFuture<Integer> future : listenFutures) {
				if (future != null) {
					futures.add(future);
				}
			}
			checkState(futures.size() > 0, "No input in listening.");

			// block to wait for a available input
			CompletableFuture.anyOf(futures.toArray(new CompletableFuture<?>[0]))
				.get(maxWaitMillis, TimeUnit.MILLISECONDS);

			// clear all completed futures
			for (CompletableFuture<Integer> future : futures) {
				checkAndSetAvailableInput(future, 0);
			}
		} else {
			int inputIndex = (inputSelection.getInputMask() == InputSelection.FIRST.getInputMask()) ? 0 : 1;
			if (isFinished[inputIndex]) {
				throw new IOException("Could not read the finished input: input" + (inputIndex + 1) + ".");
			}

			CompletableFuture<Integer> future = listenFutures[inputIndex];
			checkState(future != null, "The input is not in listening.");

			// block to wait the input being available
			checkAndSetAvailableInput(future, maxWaitMillis);
		}
	}

	private void checkAndSetAvailableInput(CompletableFuture<Integer> future, long maxWaitMillis)
		throws ExecutionException, InterruptedException, TimeoutException {

		int availableIndex = (maxWaitMillis == 0) ?
			future.getNow(-1) : future.get(maxWaitMillis, TimeUnit.MILLISECONDS);
		if (availableIndex > -1) {
			setAvailableInput(availableIndex);
			listenFutures[availableIndex] = null;
		}
	}

	private void setAvailableInput(int inputIndex) {
		availableInputsMask |= 1 << inputIndex;
	}

	private void setUnavailableInput(int inputIndex) {
		availableInputsMask &= ~(1 << inputIndex);
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
		public void handleStreamStatus(int inputIndex, StreamStatus streamStatus) {
			checkState(inputIndex >= 0 && inputIndex < 2);

			try {
				synchronized (lock) {
					final StreamStatus anotherStreamStatus;
					if (inputIndex == 0) {
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
