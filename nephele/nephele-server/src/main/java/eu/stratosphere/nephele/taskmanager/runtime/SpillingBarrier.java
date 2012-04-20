/***********************************************************************************************************************
 *
 * Copyright (C) 2010 by the Stratosphere project (http://stratosphere.eu)
 *
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software distributed under the License is distributed on
 * an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the License for the
 * specific language governing permissions and limitations under the License.
 *
 **********************************************************************************************************************/

package eu.stratosphere.nephele.taskmanager.runtime;

import java.io.IOException;

import eu.stratosphere.nephele.event.task.AbstractEvent;
import eu.stratosphere.nephele.io.channels.bytebuffered.ByteBufferedChannelActivateEvent;
import eu.stratosphere.nephele.taskmanager.bytebuffered.AbstractOutputChannelForwarder;
import eu.stratosphere.nephele.taskmanager.transferenvelope.SpillingQueue;
import eu.stratosphere.nephele.taskmanager.transferenvelope.TransferEnvelope;

public final class SpillingBarrier extends AbstractOutputChannelForwarder {

	/**
	 * Queue to store outgoing transfer envelope in case the receiver of the envelopes is not yet running.
	 */
	private final SpillingQueue queuedOutgoingEnvelopes;

	/**
	 * Indicates whether the receiver of an envelope is currently running.
	 */
	private boolean isReceiverRunning = false;

	public SpillingBarrier(final boolean isReceiverRunning, final boolean mergeSpillBuffers,
			final AbstractOutputChannelForwarder next) {
		super(next);

		if (next == null) {
			throw new IllegalArgumentException("Argument next must not be null");
		}

		this.isReceiverRunning = isReceiverRunning;
		this.queuedOutgoingEnvelopes = null;
	}

	/**
	 * {@inheritDoc}
	 */
	@Override
	public void push(final TransferEnvelope transferEnvelope) throws IOException, InterruptedException {

		/*
		 * if (!this.isReceiverRunning) {
		 * // TODO: Add this to the spilling queue
		 * return false;
		 * }
		 */

		getNext().push(transferEnvelope);
	}

	/**
	 * {@inheritDoc}
	 */
	@Override
	public void processEvent(final AbstractEvent event) {

		if (event instanceof ByteBufferedChannelActivateEvent) {
			this.isReceiverRunning = true;
		}

		getNext().processEvent(event);
	}
}
