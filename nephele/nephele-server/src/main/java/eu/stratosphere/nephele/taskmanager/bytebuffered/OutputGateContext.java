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
import java.util.HashSet;
import java.util.Iterator;
import java.util.Set;

import eu.stratosphere.nephele.io.AbstractID;
import eu.stratosphere.nephele.io.channels.Buffer;
import eu.stratosphere.nephele.taskmanager.bufferprovider.AsynchronousEventListener;
import eu.stratosphere.nephele.taskmanager.bufferprovider.BufferProvider;
import eu.stratosphere.nephele.taskmanager.transferenvelope.TransferEnvelope;

final class OutputGateContext implements BufferProvider, AsynchronousEventListener {

	private final TaskContext taskContext;
	
	private final Set<OutputChannelContext> inactiveOutputChannels;

	OutputGateContext(final TaskContext taskContext, final int outputGateIndex) {

		this.taskContext = taskContext;
		this.inactiveOutputChannels = new HashSet<OutputChannelContext>();
		
		this.taskContext.registerAsynchronousEventListener(outputGateIndex, this);
	}

	void registerInactiveOutputChannel(final OutputChannelContext outputChannelContext) {

		this.inactiveOutputChannels.add(outputChannelContext);
	}

	AbstractID getFileOwnerID() {
		
		return this.taskContext.getFileOwnerID();
	}
	
	private long spillQueueWithLargestAmountOfMainMemory() {

		if (this.inactiveOutputChannels.isEmpty()) {
			return 0L;
		}

		final Iterator<OutputChannelContext> it = this.inactiveOutputChannels.iterator();

		long maxMainMemory = 0;
		OutputChannelContext maxContext = null;

		while (it.hasNext()) {

			final OutputChannelContext context = it.next();
			final long mm = context.getAmountOfMainMemoryInQueue();

			if (mm > maxMainMemory) {
				maxMainMemory = mm;
				maxContext = context;
			}
		}

		if (maxContext != null) {
			try {
				return maxContext.spillQueueWithOutgoingEnvelopes();
			} catch (IOException ioe) {
				maxContext.reportIOException(ioe);
			}
		}

		return 0L;
	}

	private void checkForActiveOutputChannels() throws IOException, InterruptedException {

		final Iterator<OutputChannelContext> it = this.inactiveOutputChannels.iterator();
		while (it.hasNext()) {
			final OutputChannelContext channelContext = it.next();
			if (channelContext.isChannelActive()) {
				channelContext.flushQueuedOutgoingEnvelopes();
				it.remove();
			} else {
			}
		}
	}

	/**
	 * {@inheritDoc}
	 */
	@Override
	public int getMaximumBufferSize() {

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
	void processEnvelope(final TransferEnvelope outgoingTransferEnvelope) throws IOException, InterruptedException {

		
		
		this.taskContext.processEnvelope(outgoingTransferEnvelope);
	}

	/**
	 * {@inheritDoc}
	 */
	@Override
	public void asynchronousEventOccurred() throws IOException, InterruptedException {

		checkForActiveOutputChannels();
	}

	/**
	 * {@inheritDoc}
	 */
	@Override
	public Buffer requestEmptyBuffer(int minimumSizeOfBuffer) throws IOException {

		throw new IllegalStateException("requestEmptyBuffer called on OutputGateContext");
	}

	/**
	 * {@inheritDoc}
	 */
	@Override
	public Buffer requestEmptyBufferBlocking(int minimumSizeOfBuffer) throws IOException, InterruptedException {

		Buffer buffer = this.taskContext.requestEmptyBuffer(minimumSizeOfBuffer);

		// No memory-based buffer available
		if (buffer == null) {

			// Report exhaustion of memory buffers to the task context
			this.taskContext.reportExhaustionOfMemoryBuffers();

			// Spill queue that contains the largest amount of main memory, encapsulated in the queued buffers, to disk
			spillQueueWithLargestAmountOfMainMemory();

			// Wait until a memory-based buffer is available
			buffer = this.taskContext.requestEmptyBufferBlocking(minimumSizeOfBuffer);
		}

		return buffer;
	}

	/**
	 * {@inheritDoc}
	 */
	@Override
	public boolean isShared() {

		return this.taskContext.isShared();
	}

	/**
	 * {@inheritDoc}
	 */
	@Override
	public void reportAsynchronousEvent() {

		this.taskContext.reportAsynchronousEvent();
	}
}
