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
import java.util.ArrayDeque;
import java.util.HashSet;
import java.util.Map;
import java.util.Optional;
import java.util.Set;

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
public class UnionInputGate implements InputGate, InputGateListener {

	/** The input gates to union. */
	private final InputGate[] inputGates;

	private final Set<InputGate> inputGatesWithRemainingData;

	/** Gates, which notified this input gate about available data. */
	private final ArrayDeque<InputGate> inputGatesWithData = new ArrayDeque<>();

	/**
	 * Guardian against enqueuing an {@link InputGate} multiple times on {@code inputGatesWithData}.
	 */
	private final Set<InputGate> enqueuedInputGatesWithData = new HashSet<>();

	/** The total number of input channels across all unioned input gates. */
	private final int totalNumberOfInputChannels;

	/** Registered listener to forward input gate notifications to. */
	private volatile InputGateListener inputGateListener;

	/**
	 * A mapping from input gate to (logical) channel index offset. Valid channel indexes go from 0
	 * (inclusive) to the total number of input channels (exclusive).
	 */
	private final Map<InputGate, Integer> inputGateToIndexOffsetMap;

	/** Flag indicating whether partitions have been requested. */
	private boolean requestedPartitionsFlag;

	public UnionInputGate(InputGate... inputGates) {
		this.inputGates = checkNotNull(inputGates);
		checkArgument(inputGates.length > 1, "Union input gate should union at least two input gates.");

		this.inputGateToIndexOffsetMap = Maps.newHashMapWithExpectedSize(inputGates.length);
		this.inputGatesWithRemainingData = Sets.newHashSetWithExpectedSize(inputGates.length);

		int currentNumberOfInputChannels = 0;

		for (InputGate inputGate : inputGates) {
			if (inputGate instanceof UnionInputGate) {
				// if we want to add support for this, we need to implement pollNextBufferOrEvent()
				throw new UnsupportedOperationException("Cannot union a union of input gates.");
			}

			// The offset to use for buffer or event instances received from this input gate.
			inputGateToIndexOffsetMap.put(checkNotNull(inputGate), currentNumberOfInputChannels);
			inputGatesWithRemainingData.add(inputGate);

			currentNumberOfInputChannels += inputGate.getNumberOfInputChannels();

			// Register the union gate as a listener for all input gates
			inputGate.registerListener(this);
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
	public String getOwningTaskName() {
		// all input gates have the same owning task
		return inputGates[0].getOwningTaskName();
	}

	@Override
	public boolean isFinished() {
		for (InputGate inputGate : inputGates) {
			if (!inputGate.isFinished()) {
				return false;
			}
		}

		return true;
	}

	@Override
	public void requestPartitions() throws IOException, InterruptedException {
		if (!requestedPartitionsFlag) {
			for (InputGate inputGate : inputGates) {
				inputGate.requestPartitions();
			}

			requestedPartitionsFlag = true;
		}
	}

	@Override
	public Optional<BufferOrEvent> getNextBufferOrEvent() throws IOException, InterruptedException {
		if (inputGatesWithRemainingData.isEmpty()) {
			return Optional.empty();
		}

		// Make sure to request the partitions, if they have not been requested before.
		requestPartitions();

		InputGateWithData inputGateWithData = waitAndGetNextInputGate();
		InputGate inputGate = inputGateWithData.inputGate;
		BufferOrEvent bufferOrEvent = inputGateWithData.bufferOrEvent;

		if (bufferOrEvent.moreAvailable()) {
			// this buffer or event was now removed from the non-empty gates queue
			// we re-add it in case it has more data, because in that case no "non-empty" notification
			// will come for that gate
			queueInputGate(inputGate);
		}

		if (bufferOrEvent.isEvent()
			&& bufferOrEvent.getEvent().getClass() == EndOfPartitionEvent.class
			&& inputGate.isFinished()) {

			checkState(!bufferOrEvent.moreAvailable());
			if (!inputGatesWithRemainingData.remove(inputGate)) {
				throw new IllegalStateException("Couldn't find input gate in set of remaining " +
					"input gates.");
			}
		}

		// Set the channel index to identify the input channel (across all unioned input gates)
		final int channelIndexOffset = inputGateToIndexOffsetMap.get(inputGate);

		bufferOrEvent.setChannelIndex(channelIndexOffset + bufferOrEvent.getChannelIndex());
		bufferOrEvent.setMoreAvailable(bufferOrEvent.moreAvailable() || inputGateWithData.moreInputGatesAvailable);

		return Optional.of(bufferOrEvent);
	}

	@Override
	public Optional<BufferOrEvent> pollNextBufferOrEvent() throws UnsupportedOperationException {
		throw new UnsupportedOperationException();
	}

	private InputGateWithData waitAndGetNextInputGate() throws IOException, InterruptedException {
		while (true) {
			InputGate inputGate;
			boolean moreInputGatesAvailable;
			synchronized (inputGatesWithData) {
				while (inputGatesWithData.size() == 0) {
					inputGatesWithData.wait();
				}
				inputGate = inputGatesWithData.remove();
				enqueuedInputGatesWithData.remove(inputGate);
				moreInputGatesAvailable = enqueuedInputGatesWithData.size() > 0;
			}

			// In case of inputGatesWithData being inaccurate do not block on an empty inputGate, but just poll the data.
			Optional<BufferOrEvent> bufferOrEvent = inputGate.pollNextBufferOrEvent();
			if (bufferOrEvent.isPresent()) {
				return new InputGateWithData(inputGate, bufferOrEvent.get(), moreInputGatesAvailable);
			}
		}
	}

	private static class InputGateWithData {
		private final InputGate inputGate;
		private final BufferOrEvent bufferOrEvent;
		private final boolean moreInputGatesAvailable;

		InputGateWithData(InputGate inputGate, BufferOrEvent bufferOrEvent, boolean moreInputGatesAvailable) {
			this.inputGate = checkNotNull(inputGate);
			this.bufferOrEvent = checkNotNull(bufferOrEvent);
			this.moreInputGatesAvailable = moreInputGatesAvailable;
		}
	}

	@Override
	public void sendTaskEvent(TaskEvent event) throws IOException {
		for (InputGate inputGate : inputGates) {
			inputGate.sendTaskEvent(event);
		}
	}

	@Override
	public void registerListener(InputGateListener listener) {
		if (this.inputGateListener == null) {
			this.inputGateListener = listener;
		} else {
			throw new IllegalStateException("Multiple listeners");
		}
	}

	@Override
	public int getPageSize() {
		int pageSize = -1;
		for (InputGate gate : inputGates) {
			if (pageSize == -1) {
				pageSize = gate.getPageSize();
			} else if (gate.getPageSize() != pageSize) {
				throw new IllegalStateException("Found input gates with different page sizes.");
			}
		}
		return pageSize;
	}

	@Override
	public void notifyInputGateNonEmpty(InputGate inputGate) {
		queueInputGate(checkNotNull(inputGate));
	}

	private void queueInputGate(InputGate inputGate) {
		int availableInputGates;

		synchronized (inputGatesWithData) {
			if (enqueuedInputGatesWithData.contains(inputGate)) {
				return;
			}

			availableInputGates = inputGatesWithData.size();

			inputGatesWithData.add(inputGate);
			enqueuedInputGatesWithData.add(inputGate);

			if (availableInputGates == 0) {
				inputGatesWithData.notifyAll();
			}
		}

		if (availableInputGates == 0) {
			InputGateListener listener = inputGateListener;
			if (listener != null) {
				listener.notifyInputGateNonEmpty(this);
			}
		}
	}
}
