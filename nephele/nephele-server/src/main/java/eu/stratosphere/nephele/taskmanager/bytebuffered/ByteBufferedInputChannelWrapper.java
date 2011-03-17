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
import eu.stratosphere.nephele.types.Record;

public class ByteBufferedInputChannelWrapper implements ByteBufferedInputChannelBroker, ByteBufferedChannelWrapper {

	private static final Log LOG = LogFactory.getLog(ByteBufferedInputChannelWrapper.class);

	private final AbstractByteBufferedInputChannel<? extends Record> byteBufferedInputChannel;

	private final ByteBufferedChannelManager byteBufferedChannelManager;

	private final int minimumQueueLengthForThrottling;

	private final int maximumQueueLengthForThrottling;

	private final Queue<TransferEnvelope> queuedEnvelopes = new ArrayDeque<TransferEnvelope>();

	/**
	 * In case of compression this variable points to the uncompressed data buffer.
	 */
	private Buffer uncompressedDataBuffer = null;

	public ByteBufferedInputChannelWrapper(AbstractByteBufferedInputChannel<? extends Record> byteBufferedInputChannel,
			ByteBufferedChannelManager byteBufferedChannelManager, int minimumQueueLengthForThrottling,
			int maximumQueueLengthForThrottling) {
		this.byteBufferedInputChannel = byteBufferedInputChannel;
		this.byteBufferedChannelManager = byteBufferedChannelManager;
		this.minimumQueueLengthForThrottling = minimumQueueLengthForThrottling;
		this.maximumQueueLengthForThrottling = maximumQueueLengthForThrottling;

		this.byteBufferedInputChannel.setInputChannelBroker(this);
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
			final int maximumBufferSize = this.byteBufferedChannelManager.getMaximumBufferSize();
			final BufferPairRequest request = new BufferPairRequest(
				transferEnvelope.getBuffer().isBackedByMemory() ? -1 : transferEnvelope.getBuffer().size(),
				maximumBufferSize, true);

			try {
				response = this.byteBufferedChannelManager.requestEmptyReadBuffers(request);
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
					oldBuffer.copyToMemoryBackedBuffer(response.getCompressedDataBuffer());
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

			if (queuedEnvelopes.size() == (this.minimumQueueLengthForThrottling - 1)) {
				stopThrottling();
			}
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
		}
	}

	/**
	 * {@inheritDoc}
	 */
	@Override
	public void transferEventToOutputChannel(AbstractEvent event) throws IOException, InterruptedException {

		final TransferEnvelope ephemeralTransferEnvelope = new TransferEnvelope(this.byteBufferedInputChannel.getID(),
			this.byteBufferedInputChannel.getConnectedChannelID(), new TransferEnvelopeProcessingLog(true, false));

		ephemeralTransferEnvelope.setSequenceNumber(0);
		ephemeralTransferEnvelope.addEvent(event);
		this.byteBufferedChannelManager.queueOutgoingTransferEnvelope(ephemeralTransferEnvelope);
	}

	void queueIncomingTransferEnvelope(TransferEnvelope transferEnvelope) throws IOException {

		synchronized (this.queuedEnvelopes) {
			this.queuedEnvelopes.add(transferEnvelope);

			if (this.queuedEnvelopes.size() == (this.maximumQueueLengthForThrottling + 1)) {
				startThrottling();
			}
		}

		// Notify the channel about the new data
		this.byteBufferedInputChannel.checkForNetworkEvents();
	}

	private void startThrottling() {

		// transferEventToOutputChannel(new NetworkThrottleEvent(true));
	}

	private void stopThrottling() {

		// transferEventToOutputChannel(new NetworkThrottleEvent(false));
	}

	@Override
	public void reportIOException(IOException ioe) {

		this.byteBufferedInputChannel.reportIOException(ioe);
		this.byteBufferedInputChannel.checkForNetworkEvents();
		// Corresponding output channel might be throttled down, so make sure it will make up to process the IOException
		stopThrottling();
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
}
