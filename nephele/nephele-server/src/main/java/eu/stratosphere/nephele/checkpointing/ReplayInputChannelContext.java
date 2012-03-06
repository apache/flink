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

package eu.stratosphere.nephele.checkpointing;

import java.io.IOException;

import eu.stratosphere.nephele.io.channels.Buffer;
import eu.stratosphere.nephele.io.channels.ChannelID;
import eu.stratosphere.nephele.io.channels.ChannelType;
import eu.stratosphere.nephele.jobgraph.JobID;
import eu.stratosphere.nephele.taskmanager.bytebuffered.InputChannelContext;
import eu.stratosphere.nephele.taskmanager.transferenvelope.TransferEnvelope;

final class ReplayInputChannelContext implements InputChannelContext {

	/**
	 * The encapsulated input channel context.
	 */
	private final InputChannelContext encapsulatedContext;

	/**
	 * Constructs a new replay input channel context.
	 * 
	 * @param encapsulatedContext
	 *        the encapsulated input channel context
	 */
	ReplayInputChannelContext(final InputChannelContext encapsulatedContext) {
		this.encapsulatedContext = encapsulatedContext;
	}

	/**
	 * {@inheritDoc}
	 */
	@Override
	public boolean isInputChannel() {

		return true;
	}

	/**
	 * {@inheritDoc}
	 */
	@Override
	public JobID getJobID() {

		return this.encapsulatedContext.getJobID();
	}

	/**
	 * {@inheritDoc}
	 */
	@Override
	public ChannelID getChannelID() {

		return this.encapsulatedContext.getChannelID();
	}

	/**
	 * {@inheritDoc}
	 */
	@Override
	public ChannelID getConnectedChannelID() {

		return this.encapsulatedContext.getConnectedChannelID();
	}

	/**
	 * {@inheritDoc}
	 */
	@Override
	public ChannelType getType() {

		return null;
	}

	/**
	 * {@inheritDoc}
	 */
	@Override
	public void queueTransferEnvelope(final TransferEnvelope transferEnvelope) {

		this.encapsulatedContext.queueTransferEnvelope(transferEnvelope);
	}

	/**
	 * {@inheritDoc}
	 */
	@Override
	public Buffer requestEmptyBuffer(final int minimumSizeOfBuffer) throws IOException {

		return this.encapsulatedContext.requestEmptyBuffer(minimumSizeOfBuffer);
	}

	/**
	 * {@inheritDoc}
	 */
	@Override
	public Buffer requestEmptyBufferBlocking(final int minimumSizeOfBuffer) throws IOException, InterruptedException {

		return this.encapsulatedContext.requestEmptyBufferBlocking(minimumSizeOfBuffer);
	}

	/**
	 * {@inheritDoc}
	 */
	@Override
	public int getMaximumBufferSize() {

		return this.encapsulatedContext.getMaximumBufferSize();
	}

	/**
	 * {@inheritDoc}
	 */
	@Override
	public boolean isShared() {

		return this.encapsulatedContext.isShared();
	}

	/**
	 * {@inheritDoc}
	 */
	@Override
	public void reportAsynchronousEvent() {

		this.encapsulatedContext.reportAsynchronousEvent();
	}

	/**
	 * {@inheritDoc}
	 */
	@Override
	public void destroy() {

		this.encapsulatedContext.destroy();
	}

	/**
	 * {@inheritDoc}
	 */
	@Override
	public void logQueuedEnvelopes() {

		this.encapsulatedContext.logQueuedEnvelopes();
	}
}
