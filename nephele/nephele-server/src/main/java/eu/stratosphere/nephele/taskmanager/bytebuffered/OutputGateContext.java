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

import eu.stratosphere.nephele.event.task.AbstractEvent;
import eu.stratosphere.nephele.event.task.EventList;
import eu.stratosphere.nephele.io.AbstractID;
import eu.stratosphere.nephele.io.channels.Buffer;
import eu.stratosphere.nephele.io.channels.ChannelID;
import eu.stratosphere.nephele.io.channels.ChannelType;
import eu.stratosphere.nephele.io.channels.bytebuffered.ByteBufferedChannelCloseEvent;
import eu.stratosphere.nephele.taskmanager.bufferprovider.AsynchronousEventListener;
import eu.stratosphere.nephele.taskmanager.bufferprovider.BufferProvider;
import eu.stratosphere.nephele.taskmanager.transferenvelope.SpillingQueue;
import eu.stratosphere.nephele.taskmanager.transferenvelope.TransferEnvelope;

final class OutputGateContext implements BufferProvider, AsynchronousEventListener {

	private final TaskContext taskContext;

	private final ChannelType channelType;

	private final Set<OutputChannelContext> inactiveOutputChannels;

	OutputGateContext(final TaskContext taskContext, final ChannelType channelType, final int outputGateIndex) {

		this.taskContext = taskContext;
		this.channelType = channelType;

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
	 * @param caller
	 *        the output channel context calling this method
	 * @param outgoingTransferEnvelope
	 *        the transfer envelope to be forwarded
	 * @throws IOException
	 *         thrown if an I/O error occurs while processing the envelope
	 * @throws InterruptedException
	 *         thrown if the thread is interrupted while waiting for the envelope to be processed
	 */
	void processEnvelope(final OutputChannelContext caller, final TransferEnvelope outgoingTransferEnvelope)
			throws IOException, InterruptedException {

		this.taskContext.processEnvelope(outgoingTransferEnvelope);

		if (this.channelType == ChannelType.FILE) {
			// Check if the event list of the envelope contains a close event and acknowledge it
			final EventList eventList = outgoingTransferEnvelope.getEventList();
			if (eventList != null) {
				final Iterator<AbstractEvent> it = eventList.iterator();
				while (it.hasNext()) {
					final AbstractEvent event = it.next();
					if (event instanceof ByteBufferedChannelCloseEvent) {
						caller.processEvent(event);
					}
				}
			}
		}
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

		return this.taskContext.requestEmptyBuffer(minimumSizeOfBuffer);
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

	/**
	 * Registers the given spilling queue with a network connection. The network connection is in charge of polling the
	 * remaining elements from the queue.
	 * 
	 * @param sourceChannelID
	 *        the ID of the source channel which is associated with the spilling queue
	 * @param spillingQueue
	 *        the spilling queue to be registered
	 * @return <code>true</code> if the has been successfully registered with the network connection, <code>false</code>
	 *         if the receiver runs within the same task manager and there is no network operation required to transfer
	 *         the queued data
	 * @throws IOException
	 *         thrown if an I/O error occurs while looking up the destination of the queued envelopes
	 * @throws InterruptedException
	 *         thrown if the thread is interrupted while looking up the destination of the queued envelopes
	 */
	boolean registerSpillingQueueWithNetworkConnection(final ChannelID sourceChannelID,
			final SpillingQueue spillingQueue) throws IOException, InterruptedException {

		return this.taskContext.registerSpillingQueueWithNetworkConnection(sourceChannelID, spillingQueue);
	}
}
