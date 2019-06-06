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
import org.apache.flink.runtime.io.network.api.CancelCheckpointMarker;
import org.apache.flink.runtime.io.network.api.CheckpointBarrier;
import org.apache.flink.runtime.io.network.partition.consumer.BufferOrEvent;
import org.apache.flink.runtime.io.network.partition.consumer.InputGate;
import org.apache.flink.runtime.jobgraph.tasks.AbstractInvokable;

import java.util.Optional;
import java.util.concurrent.CompletableFuture;

/**
 * The BarrierDiscarder discards checkpoint barriers have been received from which input channels.
 */
@Internal
public class BarrierDiscarder implements CheckpointBarrierHandler {

	// ------------------------------------------------------------------------

	/** The input gate, to draw the buffers and events from. */
	private final InputGate inputGate;

	/**
	 * The number of channels. Once that many barriers have been received for a checkpoint, the
	 * checkpoint is considered complete.
	 */
	private final int totalNumberOfInputChannels;


	/** The listener to be notified on complete checkpoints. */
	private AbstractInvokable toNotifyOnCheckpoint;

	// ------------------------------------------------------------------------

	public BarrierDiscarder(InputGate inputGate) {
		this.inputGate = inputGate;
		this.totalNumberOfInputChannels = inputGate.getNumberOfInputChannels();
	}

	@Override
	public CompletableFuture<?> isAvailable() {
		return inputGate.isAvailable();
	}

	@Override
	public boolean isFinished() {
		return inputGate.isFinished();
	}

	@Override
	public Optional<BufferOrEvent> pollNext() throws Exception {
		while (true) {
			Optional<BufferOrEvent> next = inputGate.pollNext();
			if (!next.isPresent()) {
				// buffer or input exhausted
				return next;
			}

			BufferOrEvent bufferOrEvent = next.get();
			if (bufferOrEvent.isBuffer()) {
				return next;
			}
			else if (bufferOrEvent.getEvent().getClass() != CheckpointBarrier.class &&
				bufferOrEvent.getEvent().getClass() != CancelCheckpointMarker.class) {
				// some other event
				return next;
			}
		}
	}

	@Override
	public void registerCheckpointEventHandler(AbstractInvokable toNotifyOnCheckpoint) {
		if (this.toNotifyOnCheckpoint == null) {
			this.toNotifyOnCheckpoint = toNotifyOnCheckpoint;
		}
		else {
			throw new IllegalStateException("BarrierDiscarder already has a registered checkpoint notifyee");
		}
	}

	@Override
	public void cleanup() {

	}

	@Override
	public boolean isEmpty() {
		return true;
	}

	@Override
	public long getAlignmentDurationNanos() {
		// this one does not do alignment at all
		return 0L;
	}

	@Override
	public int getNumberOfInputChannels() {
		return totalNumberOfInputChannels;
	}
}
