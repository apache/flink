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

package org.apache.flink.streaming.runtime.io;

import org.apache.flink.annotation.Internal;
import org.apache.flink.annotation.VisibleForTesting;
import org.apache.flink.runtime.io.network.api.CancelCheckpointMarker;
import org.apache.flink.runtime.io.network.api.CheckpointBarrier;
import org.apache.flink.runtime.io.network.api.EndOfPartitionEvent;
import org.apache.flink.runtime.io.network.partition.consumer.BufferOrEvent;
import org.apache.flink.runtime.io.network.partition.consumer.InputGate;
import org.apache.flink.runtime.jobgraph.tasks.AbstractInvokable;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Nullable;

import java.io.IOException;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;

import static org.apache.flink.util.Preconditions.checkNotNull;

/**
 * The barrier buffer is {@link CheckpointedInputGate} that blocks inputs with barriers until
 * all inputs have received the barrier for a given checkpoint.
 *
 * <p>To avoid back-pressuring the input streams (which may cause distributed deadlocks), the
 * BarrierBuffer continues receiving buffers from the blocked channels and stores them internally until
 * the blocks are released.
 */
@Internal
public class BarrierBuffer implements CheckpointedInputGate {

	private static final Logger LOG = LoggerFactory.getLogger(BarrierBuffer.class);

	private final CheckpointBarrierHandler barrierHandler;

	/** The gate that the buffer draws its input from. */
	private final InputGate inputGate;

	private final BufferStorage bufferStorage;

	/** Flag to indicate whether we have drawn all available input. */
	private boolean endOfInputGate;

	/** Indicate end of the input. Set to true after encountering {@link #endOfInputGate} and depleting
	 * {@link #bufferStorage}. */
	private boolean isFinished;

	/**
	 * Creates a new checkpoint stream aligner.
	 *
	 * <p>There is no limit to how much data may be buffered during an alignment.
	 *
	 * @param inputGate The input gate to draw the buffers and events from.
	 * @param bufferStorage The storage to hold the buffers and events for blocked channels.
	 */
	@VisibleForTesting
	BarrierBuffer(InputGate inputGate, BufferStorage bufferStorage) {
		this (inputGate, bufferStorage, "Testing: No task associated", null);
	}

	BarrierBuffer(
			InputGate inputGate,
			BufferStorage bufferStorage,
			String taskName,
			@Nullable AbstractInvokable toNotifyOnCheckpoint) {
		this(
			inputGate,
			bufferStorage,
			new CheckpointBarrierAligner(
				inputGate.getNumberOfInputChannels(),
				taskName,
				toNotifyOnCheckpoint)
		);
	}

	/**
	 * Creates a new checkpoint stream aligner.
	 *
	 * <p>The aligner will allow only alignments that buffer up to the given number of bytes.
	 * When that number is exceeded, it will stop the alignment and notify the task that the
	 * checkpoint has been cancelled.
	 *
	 * @param inputGate The input gate to draw the buffers and events from.
	 * @param bufferStorage The storage to hold the buffers and events for blocked channels.
	 * @param barrierHandler Handler that controls which channels are blocked.
	 */
	BarrierBuffer(
			InputGate inputGate,
			BufferStorage bufferStorage,
			CheckpointBarrierHandler barrierHandler) {
		this.inputGate = inputGate;
		this.bufferStorage = checkNotNull(bufferStorage);
		this.barrierHandler = barrierHandler;
	}

	@Override
	public CompletableFuture<?> isAvailable() {
		if (bufferStorage.isEmpty()) {
			return inputGate.isAvailable();
		}
		return AVAILABLE;
	}

	@Override
	public Optional<BufferOrEvent> pollNext() throws Exception {
		while (true) {
			// process buffered BufferOrEvents before grabbing new ones
			Optional<BufferOrEvent> next;
			if (bufferStorage.isEmpty()) {
				next = inputGate.pollNext();
			}
			else {
				// TODO: FLINK-12536 for non credit-based flow control, getNext method is blocking
				next = bufferStorage.pollNext();
				if (!next.isPresent()) {
					return pollNext();
				}
			}

			if (!next.isPresent()) {
				return handleEmptyBuffer();
			}

			BufferOrEvent bufferOrEvent = next.get();
			if (barrierHandler.isBlocked(bufferOrEvent.getChannelIndex())) {
				// if the channel is blocked, we just store the BufferOrEvent
				bufferStorage.add(bufferOrEvent);
				if (bufferStorage.isFull()) {
					barrierHandler.checkpointSizeLimitExceeded(bufferStorage.getMaxBufferedBytes());
					bufferStorage.rollOver();
				}
			}
			else if (bufferOrEvent.isBuffer()) {
				return next;
			}
			else if (bufferOrEvent.getEvent().getClass() == CheckpointBarrier.class) {
				CheckpointBarrier checkpointBarrier = (CheckpointBarrier) bufferOrEvent.getEvent();
				if (!endOfInputGate) {
					// process barriers only if there is a chance of the checkpoint completing
					if (barrierHandler.processBarrier(checkpointBarrier, bufferOrEvent.getChannelIndex(), bufferStorage.getPendingBytes())) {
						bufferStorage.rollOver();
					}
				}
			}
			else if (bufferOrEvent.getEvent().getClass() == CancelCheckpointMarker.class) {
				if (barrierHandler.processCancellationBarrier((CancelCheckpointMarker) bufferOrEvent.getEvent())) {
					bufferStorage.rollOver();
				}
			}
			else {
				if (bufferOrEvent.getEvent().getClass() == EndOfPartitionEvent.class) {
					if (barrierHandler.processEndOfPartition()) {
						bufferStorage.rollOver();
					}
				}
				return next;
			}
		}
	}

	private Optional<BufferOrEvent> handleEmptyBuffer() throws Exception {
		if (!inputGate.isFinished()) {
			return Optional.empty();
		}

		if (endOfInputGate) {
			isFinished = true;
			return Optional.empty();
		} else {
			// end of input stream. stream continues with the buffered data
			endOfInputGate = true;
			barrierHandler.releaseBlocksAndResetBarriers();
			bufferStorage.rollOver();
			return pollNext();
		}
	}

	@Override
	public boolean isEmpty() {
		return bufferStorage.isEmpty();
	}

	@Override
	public boolean isFinished() {
		return isFinished;
	}

	@Override
	public void cleanup() throws IOException {
		bufferStorage.close();
	}

	// ------------------------------------------------------------------------
	//  Properties
	// ------------------------------------------------------------------------

	/**
	 * Gets the ID defining the current pending, or just completed, checkpoint.
	 *
	 * @return The ID of the pending of completed checkpoint.
	 */
	public long getLatestCheckpointId() {
		return barrierHandler.getLatestCheckpointId();
	}

	@Override
	public long getAlignmentDurationNanos() {
		return barrierHandler.getAlignmentDurationNanos();
	}

	@Override
	public int getNumberOfInputChannels() {
		return inputGate.getNumberOfInputChannels();
	}

	// ------------------------------------------------------------------------
	// Utilities
	// ------------------------------------------------------------------------

	@Override
	public String toString() {
		return barrierHandler.toString();
	}
}
