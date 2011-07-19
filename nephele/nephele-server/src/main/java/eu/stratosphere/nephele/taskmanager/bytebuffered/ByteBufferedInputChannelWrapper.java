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
import java.util.ArrayDeque;
import java.util.Iterator;
import java.util.Queue;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import eu.stratosphere.nephele.event.task.AbstractEvent;
import eu.stratosphere.nephele.event.task.EventList;
import eu.stratosphere.nephele.io.channels.Buffer;
import eu.stratosphere.nephele.io.channels.ChannelID;
import eu.stratosphere.nephele.io.channels.bytebuffered.AbstractByteBufferedInputChannel;
import eu.stratosphere.nephele.io.channels.bytebuffered.BufferPairRequest;
import eu.stratosphere.nephele.io.channels.bytebuffered.BufferPairResponse;
import eu.stratosphere.nephele.io.channels.bytebuffered.ByteBufferedInputChannelBroker;
import eu.stratosphere.nephele.io.compression.CompressionLevel;
import eu.stratosphere.nephele.jobgraph.JobID;
import eu.stratosphere.nephele.taskmanager.bufferprovider.ReadBufferProvider;
import eu.stratosphere.nephele.taskmanager.transferenvelope.TransferEnvelope;
import eu.stratosphere.nephele.taskmanager.transferenvelope.TransferEnvelopeDispatcher;
import eu.stratosphere.nephele.types.Record;

public class ByteBufferedInputChannelWrapper implements ByteBufferedInputChannelBroker, ByteBufferedChannelWrapper {

	private static final Log LOG = LogFactory.getLog(ByteBufferedInputChannelWrapper.class);

	private final AbstractByteBufferedInputChannel<? extends Record> byteBufferedInputChannel;

	private final TransferEnvelopeDispatcher transferEnvelopeDispatcher;

	private final ReadBufferProvider readBufferProvider;

	private final Queue<TransferEnvelope> queuedEnvelopes = new ArrayDeque<TransferEnvelope>();

	private int nextExpectedSequenceNumber = 0;

	private int numberOfMemoryBuffers = 0;

	private int numberOfFileBuffers = 0;

	/**
	 * In case of compression this variable points to the uncompressed data buffer.
	 */
	private Buffer uncompressedDataBuffer = null;

	public ByteBufferedInputChannelWrapper(AbstractByteBufferedInputChannel<? extends Record> byteBufferedInputChannel,
			TransferEnvelopeDispatcher transferEnvelopeDispatcher, ReadBufferProvider readBufferProvider) {

		this.byteBufferedInputChannel = byteBufferedInputChannel;
		this.byteBufferedInputChannel.setInputChannelBroker(this);

		this.transferEnvelopeDispatcher = transferEnvelopeDispatcher;
		this.readBufferProvider = readBufferProvider;
	}

	@Override
	public BufferPairResponse getReadBufferToConsume() {

		TransferEnvelope transferEnvelope = null;

		synchronized (this.queuedEnvelopes) {

			if (this.queuedEnvelopes.isEmpty()) {
				return null;
			}

			transferEnvelope = this.queuedEnvelopes.peek();

			// If envelope does not have a buffer, remove it immediately
			if (transferEnvelope.getBuffer() == null) {
				this.queuedEnvelopes.poll();
			}
		}

		// Make sure we have all necessary buffers before we go on
		if (transferEnvelope.getBuffer() == null) {

			// No buffers necessary
			final EventList eventList = transferEnvelope.getEventList();
			if (!eventList.isEmpty()) {
				final Iterator<AbstractEvent> it = eventList.iterator();
				while (it.hasNext()) {
					this.byteBufferedInputChannel.processEvent(it.next());
				}
			}

			return null;
		}

		// Make sure we get all the buffers proceed
		BufferPairResponse response;
		if (this.byteBufferedInputChannel.getCompressionLevel() == CompressionLevel.NO_COMPRESSION) {

			// Compression disabled
			response = new BufferPairResponse(null, transferEnvelope.getBuffer()); // No need to copy anything
		} else {

			// Compression enabled
			final int maximumBufferSize = this.readBufferProvider.getMaximumBufferSize();
			final BufferPairRequest request = new BufferPairRequest(
				transferEnvelope.getBuffer().isBackedByMemory() ? -1 : transferEnvelope.getBuffer().size(),
				maximumBufferSize, true);

			try {
				response = this.readBufferProvider.requestEmptyReadBuffers(request);
			} catch (InterruptedException e) {
				this.byteBufferedInputChannel.checkForNetworkEvents(); // Make sure we check again
				return null;
			}

			if (transferEnvelope.getBuffer().isBackedByMemory()) {
				response = new BufferPairResponse(transferEnvelope.getBuffer(), response.getUncompressedDataBuffer()); // Overwrite
				// response
				// to
				// include
				// new
				// buffer
			} else {

				final Buffer oldBuffer = transferEnvelope.getBuffer();

				try {
					oldBuffer.copyToBuffer(response.getCompressedDataBuffer());
					//TODO: Fix this
					transferEnvelope.setBuffer(response.getCompressedDataBuffer());
				} catch (IOException ioe) {
					LOG.error(ioe);
					this.byteBufferedInputChannel.reportIOException(ioe);
					return null;
				}

				// Recycle old buffer
				oldBuffer.recycleBuffer();
			}

			this.uncompressedDataBuffer = response.getUncompressedDataBuffer();
		}
		
		// Process events
		final EventList eventList = transferEnvelope.getEventList();
		if (!eventList.isEmpty()) {
			final Iterator<AbstractEvent> it = eventList.iterator();
			while (it.hasNext()) {
				this.byteBufferedInputChannel.processEvent(it.next());
			}
		}

		return response;
	}

	@Override
	public void releaseConsumedReadBuffer() {

		TransferEnvelope transferEnvelope = null;
		synchronized (this.queuedEnvelopes) {

			if (this.queuedEnvelopes.isEmpty()) {
				LOG.error("Inconsistency: releaseConsumedReadBuffer called on empty queue!");
				return;
			}

			transferEnvelope = this.queuedEnvelopes.poll();
		}

		final Buffer consumedBuffer = transferEnvelope.getBuffer();
		if (consumedBuffer == null) {
			LOG.error("Inconsistency: consumed read buffer is null!");
			return;
		}

		if (consumedBuffer.remaining() > 0) {
			LOG.error("consumedReadBuffer has " + consumedBuffer.remaining() + " unconsumed bytes left!!");
		}

		// Recycle consumed read buffer
		consumedBuffer.recycleBuffer();

		if (this.uncompressedDataBuffer != null) {
			this.uncompressedDataBuffer.recycleBuffer();
			this.uncompressedDataBuffer = null;
		}
	}

	/**
	 * {@inheritDoc}
	 */
	@Override
	public void transferEventToOutputChannel(AbstractEvent event) throws IOException, InterruptedException {

		final TransferEnvelope ephemeralTransferEnvelope = new TransferEnvelope(0, getJobID(), getChannelID());

		ephemeralTransferEnvelope.addEvent(event);
		this.transferEnvelopeDispatcher.processEnvelopeFromInputChannel(ephemeralTransferEnvelope);
	}

	@Override
	public void queueTransferEnvelope(TransferEnvelope transferEnvelope) {

		synchronized (this.queuedEnvelopes) {
			this.queuedEnvelopes.add(transferEnvelope);
		}

		// Notify the channel about the new data
		this.byteBufferedInputChannel.checkForNetworkEvents();
	}

=======
>>>>>>> experimental
	@Override
	public void reportIOException(IOException ioe) {

		this.byteBufferedInputChannel.reportIOException(ioe);
		this.byteBufferedInputChannel.checkForNetworkEvents();
	}

	@Override
	public ChannelID getChannelID() {

		return this.byteBufferedInputChannel.getID();
	}

	@Override
	public ChannelID getConnectedChannelID() {

		return this.byteBufferedInputChannel.getConnectedChannelID();
	}

	@Override
	public JobID getJobID() {

		return this.byteBufferedInputChannel.getJobID();
	}

	@Override
	public boolean isInputChannel() {

		return this.byteBufferedInputChannel.isInputChannel();
	}

	public int getNumberOfQueuedEnvelopes() {

		synchronized (this.queuedEnvelopes) {

			return this.queuedEnvelopes.size();
		}
	}

	public int getNumberOfQueuedMemoryBuffers() {

		synchronized (this.queuedEnvelopes) {

			int count = 0;

			final Iterator<TransferEnvelope> it = this.queuedEnvelopes.iterator();
			while (it.hasNext()) {

				final TransferEnvelope envelope = it.next();
				if (envelope.getBuffer() != null) {
					if (envelope.getBuffer().isBackedByMemory()) {
						++count;
					}
				}
			}

			return count;
		}
	}

	public void releaseAllResources() {

		final Queue<Buffer> buffersToRecycle = new ArrayDeque<Buffer>();
		
		synchronized (this.queuedEnvelopes) {
						
			while(!this.queuedEnvelopes.isEmpty()) {
				final TransferEnvelope envelope = this.queuedEnvelopes.poll();
				if(envelope.getBuffer() != null) {
					buffersToRecycle.add(envelope.getBuffer());
				}
			}
		}

		if(this.uncompressedDataBuffer != null) {
			buffersToRecycle.add(this.uncompressedDataBuffer);
			this.uncompressedDataBuffer = null;
		}
		
		while(!buffersToRecycle.isEmpty()) {
			buffersToRecycle.poll().recycleBuffer();
		}
	}
}
