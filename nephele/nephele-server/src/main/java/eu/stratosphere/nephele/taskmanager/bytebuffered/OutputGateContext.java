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

package eu.stratosphere.nephele.taskmanager.bytebuffered;

import java.io.IOException;

import eu.stratosphere.nephele.io.OutputGate;
import eu.stratosphere.nephele.io.channels.Buffer;
import eu.stratosphere.nephele.io.channels.BufferFactory;
import eu.stratosphere.nephele.io.channels.ChannelType;
import eu.stratosphere.nephele.io.channels.FileBufferManager;
import eu.stratosphere.nephele.taskmanager.checkpointing.EphemeralCheckpoint;
import eu.stratosphere.nephele.taskmanager.transferenvelope.TransferEnvelope;
import eu.stratosphere.nephele.taskmanager.transferenvelope.TransferEnvelopeDispatcher;

final class OutputGateContext {

	private final TaskContext taskContext;

	private final OutputGate<?> outputGate;

	private final FileBufferManager fileBufferManager;

	private final EphemeralCheckpoint ephemeralCheckpoint;

	private final boolean allowSenderSideSpilling;

	/**
	 * The dispatcher for received transfer envelopes.
	 */
	private final TransferEnvelopeDispatcher transferEnvelopeDispatcher;

	OutputGateContext(final TaskContext taskContext, final OutputGate<?> outputGate,
			final TransferEnvelopeDispatcher transferEnvelopeDispatcher, final FileBufferManager fileBufferManager,
			final boolean allowSenderSideSpilling) {

		this.taskContext = taskContext;
		this.outputGate = outputGate;

		this.transferEnvelopeDispatcher = transferEnvelopeDispatcher;
		this.fileBufferManager = fileBufferManager;

		this.ephemeralCheckpoint = new EphemeralCheckpoint(this.outputGate.getGateID(),
			(outputGate.getChannelType() == ChannelType.FILE) ? false : true, this.fileBufferManager);

		this.allowSenderSideSpilling = allowSenderSideSpilling;
	}

	Buffer requestEmptyBufferBlocking(final OutputChannelContext caller, final int minimumSizeOfBuffer)
			throws IOException, InterruptedException {

		Buffer buffer;

		final ChannelType channelType = this.outputGate.getChannelType();

		if (channelType == ChannelType.FILE) {

			// File channels always receive file based buffers
			buffer = getFileBuffer(minimumSizeOfBuffer);

		} else {

			// For network and in-memory channels, try to get a memory-based buffer first
			buffer = this.taskContext.requestEmptyBuffer(minimumSizeOfBuffer, 0);

			// No memory-based buffer available
			if (buffer == null) {

				// If sender-side spilling is allowed, network channels can also get a file-based buffer
				if (channelType == ChannelType.NETWORK && this.allowSenderSideSpilling) {
					buffer = getFileBuffer(minimumSizeOfBuffer);
				} else {

					// We are out of byte buffers
					if (!this.ephemeralCheckpoint.isDecided()) {
						this.ephemeralCheckpoint.destroy();
						// this.ephemeralCheckpoint.write();
					}

					// Wait until a memory-based buffer is available
					buffer = this.taskContext.requestEmptyBufferBlocking(minimumSizeOfBuffer, 0);
				}
			}
		}

		return buffer;
	}

	int getMaximumBufferSize() {

		return this.taskContext.getMaximumBufferSize();
	}

	/**
	 * Called by the attached output channel wrapper to forward a {@link TransferEnvelope} object
	 * to its final destination. Within this method the provided transfer envelope is possibly also
	 * forwarded to the assigned ephemeral checkpoint.
	 * 
	 * @param outgoingTransferEnvelope
	 *        the transfer envelope to be forwarded
	 * @throws IOException
	 *         thrown if an I/O error occurs while processing the envelope
	 * @throws InterruptedException
	 *         thrown if the thread is interrupted while waiting for the envelope to be processed
	 */
	void processEnvelope(final OutputChannelContext caller, final TransferEnvelope outgoingTransferEnvelope)
			throws IOException,
			InterruptedException {

		/*
		 * if (!this.ephemeralCheckpoint.isDiscarded()) {
		 * final TransferEnvelope dup = outgoingTransferEnvelope.duplicate();
		 * this.ephemeralCheckpoint.addTransferEnvelope(dup);
		 * }
		 */

		this.transferEnvelopeDispatcher.processEnvelopeFromOutputChannel(outgoingTransferEnvelope);
	}

	Buffer getFileBuffer(final int bufferSize) throws IOException {

		return BufferFactory.createFromFile(bufferSize, this.outputGate.getGateID(), this.fileBufferManager);
	}
}
