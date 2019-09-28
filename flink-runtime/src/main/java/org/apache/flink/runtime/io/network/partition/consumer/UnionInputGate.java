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

package org.apache.flink.runtime.io.network.partition.consumer;

import org.apache.flink.runtime.event.TaskEvent;
import org.apache.flink.runtime.io.network.api.EndOfPartitionEvent;

import org.apache.flink.shaded.guava18.com.google.common.collect.Maps;
import org.apache.flink.shaded.guava18.com.google.common.collect.Sets;

import java.io.IOException;
import java.util.Iterator;
import java.util.LinkedHashSet;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.CompletableFuture;

import static org.apache.flink.util.Preconditions.checkArgument;
import static org.apache.flink.util.Preconditions.checkNotNull;
import static org.apache.flink.util.Preconditions.checkState;

/**
 * Input gate wrapper to union the input from multiple input gates.
 *
 * <p>Each input gate has input channels attached from which it reads data. At each input gate, the
 * input channels have unique IDs from 0 (inclusive) to the number of input channels (exclusive).
 *
 * <pre>
 * +---+---+      +---+---+---+
 * | 0 | 1 |      | 0 | 1 | 2 |
 * +--------------+--------------+
 * | Input gate 0 | Input gate 1 |
 * +--------------+--------------+
 * </pre>
 *
 * <p>The union input gate maps these IDs from 0 to the *total* number of input channels across all
 * unioned input gates, e.g. the channels of input gate 0 keep their original indexes and the
 * channel indexes of input gate 1 are set off by 2 to 2--4.
 *
 * <pre>
 * +---+---++---+---+---+
 * | 0 | 1 || 2 | 3 | 4 |
 * +--------------------+
 * | Union input gate   |
 * +--------------------+
 * </pre>
 *
 * <strong>It is NOT possible to recursively union union input gates.</strong>
 */
public class UnionInputGate extends InputGate {

	/** The input gates to union. */
	private final InputGate[] inputGates;

	private final Set<InputGate> inputGatesWithRemainingData;

	/**
	 * Gates, which notified this input gate about available data. We are using it as a FIFO
	 * queue of {@link InputGate}s to avoid starvation and provide some basic fairness.
	 */
	private final LinkedHashSet<InputGate> inputGatesWithData = new LinkedHashSet<>();

	/** The total number of input channels across all unioned input gates. */
	private final int totalNumberOfInputChannels;

	/**
	 * A mapping from input gate to (logical) channel index offset. Valid channel indexes go from 0
	 * (inclusive) to the total number of input channels (exclusive).
	 */
	private final Map<InputGate, Integer> inputGateToIndexOffsetMap;

	public UnionInputGate(InputGate... inputGates) {
		this.inputGates = checkNotNull(inputGates);
		checkArgument(inputGates.length > 1, "Union input gate should union at least two input gates.");

		this.inputGateToIndexOffsetMap = Maps.newHashMapWithExpectedSize(inputGates.length);
		this.inputGatesWithRemainingData = Sets.newHashSetWithExpectedSize(inputGates.length);

		int currentNumberOfInputChannels = 0;

		synchronized (inputGatesWithData) {
			for (InputGate inputGate : inputGates) {
				if (inputGate instanceof UnionInputGate) {
					// if we want to add support for this, we need to implement pollNext()
					throw new UnsupportedOperationException("Cannot union a union of input gates.");
				}

				// The offset to use for buffer or event instances received from this input gate.
				inputGateToIndexOffsetMap.put(checkNotNull(inputGate), currentNumberOfInputChannels);
				inputGatesWithRemainingData.add(inputGate);

				currentNumberOfInputChannels += inputGate.getNumberOfInputChannels();

				CompletableFuture<?> available = inputGate.isAvailable();

				if (available.isDone()) {
					inputGatesWithData.add(inputGate);
				} else {
					available.thenRun(() -> queueInputGate(inputGate));
				}
			}

			if (!inputGatesWithData.isEmpty()) {
				isAvailable = AVAILABLE;
			}
		}

		this.totalNumberOfInputChannels = currentNumberOfInputChannels;
	}

	/**
	 * Returns the total number of input channels across all unioned input gates.
	 */
	@Override
	public int getNumberOfInputChannels() {
		return totalNumberOfInputChannels;
	}

	@Override
	public boolean isFinished() {
		return inputGatesWithRemainingData.isEmpty();
	}

	@Override
	public Optional<BufferOrEvent> getNext() throws IOException, InterruptedException {
		return getNextBufferOrEvent(true);
	}

	@Override
	public Optional<BufferOrEvent> pollNext() throws IOException, InterruptedException {
		return getNextBufferOrEvent(false);
	}

	private Optional<BufferOrEvent> getNextBufferOrEvent(boolean blocking) throws IOException, InterruptedException {
		if (inputGatesWithRemainingData.isEmpty()) {
			return Optional.empty();
		}

		Optional<InputWithData<InputGate, BufferOrEvent>> next = waitAndGetNextData(blocking);
		if (!next.isPresent()) {
			return Optional.empty();
		}

		InputWithData<InputGate, BufferOrEvent> inputWithData = next.get();

		handleEndOfPartitionEvent(inputWithData.data, inputWithData.input);
		return Optional.of(adjustForUnionInputGate(
			inputWithData.data,
			inputWithData.input,
			inputWithData.moreAvailable));
	}

	private Optional<InputWithData<InputGate, BufferOrEvent>> waitAndGetNextData(boolean blocking)
			throws IOException, InterruptedException {
		while (true) {
			Optional<InputGate> inputGate = getInputGate(blocking);
			if (!inputGate.isPresent()) {
				return Optional.empty();
			}

			// In case of inputGatesWithData being inaccurate do not block on an empty inputGate, but just poll the data.
			// Do not poll the gate under inputGatesWithData lock, since this can trigger notifications
			// that could deadlock because of wrong locks taking order.
			Optional<BufferOrEvent> bufferOrEvent = inputGate.get().pollNext();

			synchronized (inputGatesWithData) {
				if (bufferOrEvent.isPresent() && bufferOrEvent.get().moreAvailable()) {
					// enqueue the inputGate at the end to avoid starvation
					inputGatesWithData.add(inputGate.get());
				} else if (!inputGate.get().isFinished()) {
					inputGate.get().isAvailable().thenRun(() -> queueInputGate(inputGate.get()));
				}

				if (inputGatesWithData.isEmpty()) {
					resetIsAvailable();
				}

				if (bufferOrEvent.isPresent()) {
					return Optional.of(new InputWithData<>(
						inputGate.get(),
						bufferOrEvent.get(),
						!inputGatesWithData.isEmpty()));
				}
			}
		}
	}

	private BufferOrEvent adjustForUnionInputGate(
			BufferOrEvent bufferOrEvent,
			InputGate inputGate,
			boolean moreInputGatesAvailable) {
		// Set the channel index to identify the input channel (across all unioned input gates)
		final int channelIndexOffset = inputGateToIndexOffsetMap.get(inputGate);

		bufferOrEvent.setChannelIndex(channelIndexOffset + bufferOrEvent.getChannelIndex());
		bufferOrEvent.setMoreAvailable(bufferOrEvent.moreAvailable() || moreInputGatesAvailable);

		return bufferOrEvent;
	}

	private void handleEndOfPartitionEvent(BufferOrEvent bufferOrEvent, InputGate inputGate) {
		if (bufferOrEvent.isEvent()
			&& bufferOrEvent.getEvent().getClass() == EndOfPartitionEvent.class
			&& inputGate.isFinished()) {

			checkState(!bufferOrEvent.moreAvailable());
			if (!inputGatesWithRemainingData.remove(inputGate)) {
				throw new IllegalStateException("Couldn't find input gate in set of remaining " +
					"input gates.");
			}
			if (isFinished()) {
				markAvailable();
			}
		}
	}

	private void markAvailable() {
		CompletableFuture<?> toNotfiy;
		synchronized (inputGatesWithData) {
			toNotfiy = isAvailable;
			isAvailable = AVAILABLE;
		}
		toNotfiy.complete(null);
	}

	@Override
	public void sendTaskEvent(TaskEvent event) throws IOException {
		for (InputGate inputGate : inputGates) {
			inputGate.sendTaskEvent(event);
		}
	}

	@Override
	public void setup() {
	}

	@Override
	public void close() throws IOException {
	}

	private void queueInputGate(InputGate inputGate) {
		checkNotNull(inputGate);

		CompletableFuture<?> toNotify = null;

		synchronized (inputGatesWithData) {
			if (inputGatesWithData.contains(inputGate)) {
				return;
			}

			int availableInputGates = inputGatesWithData.size();

			inputGatesWithData.add(inputGate);

			if (availableInputGates == 0) {
				inputGatesWithData.notifyAll();
				toNotify = isAvailable;
				isAvailable = AVAILABLE;
			}
		}

		if (toNotify != null) {
			toNotify.complete(null);
		}
	}

	private Optional<InputGate> getInputGate(boolean blocking) throws InterruptedException {
		synchronized (inputGatesWithData) {
			while (inputGatesWithData.size() == 0) {
				if (blocking) {
					inputGatesWithData.wait();
				} else {
					resetIsAvailable();
					return Optional.empty();
				}
			}

			Iterator<InputGate> inputGateIterator = inputGatesWithData.iterator();
			InputGate inputGate = inputGateIterator.next();
			inputGateIterator.remove();

			return Optional.of(inputGate);
		}
	}
}
