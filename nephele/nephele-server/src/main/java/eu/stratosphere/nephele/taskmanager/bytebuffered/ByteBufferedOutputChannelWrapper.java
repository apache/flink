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
import java.util.Iterator;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import eu.stratosphere.nephele.event.task.AbstractEvent;
import eu.stratosphere.nephele.io.channels.Buffer;
import eu.stratosphere.nephele.io.channels.ChannelID;
import eu.stratosphere.nephele.io.channels.bytebuffered.AbstractByteBufferedOutputChannel;
import eu.stratosphere.nephele.io.channels.bytebuffered.BufferPairRequest;
import eu.stratosphere.nephele.io.channels.bytebuffered.BufferPairResponse;
import eu.stratosphere.nephele.io.channels.bytebuffered.ByteBufferedOutputChannelBroker;
import eu.stratosphere.nephele.io.compression.CompressionLevel;
import eu.stratosphere.nephele.io.compression.CompressionLoader;
import eu.stratosphere.nephele.jobgraph.JobID;
import eu.stratosphere.nephele.taskmanager.bufferprovider.OutOfByteBuffersListener;
import eu.stratosphere.nephele.taskmanager.transferenvelope.TransferEnvelope;
import eu.stratosphere.nephele.types.Record;

/**
 * This class is responsible for managing the buffers requested and released by the associated
 * {@link AbstractByteBufferedOutputChannel} object. In particular, it takes care of providing
 * buffers with the correct size to the channel and prepares the released buffers for further
 * processing.
 * 
 * @author warneke
 */
public class ByteBufferedOutputChannelWrapper implements ByteBufferedOutputChannelBroker, ByteBufferedChannelWrapper,
		OutOfByteBuffersListener {

	/**
	 * The static object used for logging.
	 */
	private static final Log LOG = LogFactory.getLog(ByteBufferedOutputChannelWrapper.class);

	/**
	 * The byte buffered output channel this channel wrapper is attached to.
	 */
	private final AbstractByteBufferedOutputChannel<? extends Record> byteBufferedOutputChannel;

	/**
	 * The channel group this channel wrapper (and the concrete channel) belongs to.
	 */
	private final ByteBufferedOutputChannelGroup byteBufferedOutputChannelGroup;

	/**
	 * Points to the {@link TransferEnvelope} object that will be passed to the framework upon
	 * the next <code>releaseWriteBuffers</code> call.
	 */
	private TransferEnvelope outgoingTransferEnvelope = null;

	/**
	 * The sequence number for the next {@link TransferEnvelope} to be created.
	 */
	private int sequenceNumber = 0;

	/**
	 * In case of compression this variable points to the uncompressed data buffer.
	 */
	private Buffer uncompressedDataBuffer = null;

	/**
	 * Constructs a new byte buffered output channel wrapper.
	 * 
	 * @param byteBufferedOutputChannel
	 *        the {@link AbstractByteBufferedOutputChannel} this wrapper belongs to
	 * @param byteBufferedOutputChannelGroup
	 *        the {@link ByteBufferedOutputChannelGroup} this wrapper belongs to
	 */
	public ByteBufferedOutputChannelWrapper(
			AbstractByteBufferedOutputChannel<? extends Record> byteBufferedOutputChannel,
			ByteBufferedOutputChannelGroup byteBufferedOutputChannelGroup) {
		this.byteBufferedOutputChannel = byteBufferedOutputChannel;
		this.byteBufferedOutputChannelGroup = byteBufferedOutputChannelGroup;

		this.byteBufferedOutputChannel.setByteBufferedOutputChannelBroker(this);
	}

	/**
	 * {@inheritDoc}
	 */
	@Override
	public void releaseWriteBuffers() throws IOException, InterruptedException {

		if (this.outgoingTransferEnvelope == null) {
			LOG.error("Cannot find transfer envelope for channel with ID " + this.byteBufferedOutputChannel.getID());
			return;
		}

		// Consistency check
		if (this.outgoingTransferEnvelope.getBuffer() == null) {
			LOG.error("Channel " + this.byteBufferedOutputChannel.getID() + " has no buffer attached");
			return;
		}

		// Finish the write phase of the buffer
		try {
			this.outgoingTransferEnvelope.getBuffer().finishWritePhase();
		} catch (IOException ioe) {
			this.byteBufferedOutputChannel.reportIOException(ioe);
		}

		this.byteBufferedOutputChannelGroup.processEnvelope(this, this.outgoingTransferEnvelope);

		if (this.uncompressedDataBuffer != null) {
			this.uncompressedDataBuffer.recycleBuffer();
			this.uncompressedDataBuffer = null;
		}

		this.outgoingTransferEnvelope = null;

	}

	/**
	 * {@inheritDoc}
	 */
	@Override
	public BufferPairResponse requestEmptyWriteBuffers() throws IOException, InterruptedException {

		if (this.outgoingTransferEnvelope == null) {
			this.outgoingTransferEnvelope = createNewOutgoingTransferEnvelope();
		} else {
			if (this.outgoingTransferEnvelope.getBuffer() != null) {
				LOG.error("Channel " + this.byteBufferedOutputChannel.getID()
					+ "' transfer envelope already has a buffer attached");
				return null;
			}
		}

		final CompressionLevel compressionLevel = this.byteBufferedOutputChannel.getCompressionLevel();
		BufferPairRequest byteBufferPair = null;
		if (compressionLevel == CompressionLevel.NO_COMPRESSION) {

			final int uncompressedBufferSize = calculateBufferSize();
			byteBufferPair = new BufferPairRequest(-1, uncompressedBufferSize, true);

		} else { // Compression
			final int compressedBufferSize = calculateBufferSize();
			final int uncompressedBufferSize = CompressionLoader.getUncompressedBufferSize(compressedBufferSize,
				compressionLevel);
			byteBufferPair = new BufferPairRequest(compressedBufferSize, uncompressedBufferSize, true);
		}

		final BufferPairResponse bufferResponse = this.byteBufferedOutputChannelGroup
			.requestEmptyWriteBuffers(this, byteBufferPair);

		// Put the buffer into the transfer envelope
		if (compressionLevel == CompressionLevel.NO_COMPRESSION) {
			this.outgoingTransferEnvelope.setBuffer(bufferResponse.getUncompressedDataBuffer());
		} else {
			this.outgoingTransferEnvelope.setBuffer(bufferResponse.getCompressedDataBuffer());
			this.uncompressedDataBuffer = bufferResponse.getUncompressedDataBuffer();
		}

		return bufferResponse;
	}

	/**
	 * Calculates the recommended size of the next buffer to be
	 * handed to the attached channel object in bytes.
	 * 
	 * @return the recommended size of the next buffer in bytes
	 */
	private int calculateBufferSize() {

		// TODO: Include latency considerations
		return this.byteBufferedOutputChannelGroup.getMaximumBufferSize();
	}

	/**
	 * Creates a new {@link TransferEnvelope} object. The method assigns
	 * and increases the sequence number. Moreover, it will look up the list of receivers for this transfer envelope.
	 * This method will block until the lookup is completed.
	 * 
	 * @return a new {@link TransferEnvelope} object containing the correct sequence number and receiver list
	 */
	private TransferEnvelope createNewOutgoingTransferEnvelope() {

		final TransferEnvelope transferEnvelope = new TransferEnvelope(this.sequenceNumber++, getJobID(),
			getChannelID());

		return transferEnvelope;
	}

	/**
	 * {@inheritDoc}
	 */
	@Override
	public void transferEventToInputChannel(AbstractEvent event) throws InterruptedException, IOException {

		if (this.outgoingTransferEnvelope != null) {
			this.outgoingTransferEnvelope.addEvent(event);
		} else {
			final TransferEnvelope ephemeralTransferEnvelope = createNewOutgoingTransferEnvelope();
			ephemeralTransferEnvelope.addEvent(event);
			this.byteBufferedOutputChannelGroup.processEnvelope(this, ephemeralTransferEnvelope);
		}
	}

	/**
	 * Called by the framework to report events to
	 * the attached channel object.
	 * 
	 * @param abstractEvent
	 *        the event to be reported
	 */
	void processEvent(AbstractEvent abstractEvent) {

		this.byteBufferedOutputChannel.processEvent(abstractEvent);
	}

	/**
	 * {@inheritDoc}
	 */
	@Override
	public void reportIOException(IOException ioe) {

		this.byteBufferedOutputChannel.reportIOException(ioe);
	}

	/**
	 * {@inheritDoc}
	 */
	@Override
	public ChannelID getChannelID() {

		return this.byteBufferedOutputChannel.getID();
	}

	/**
	 * {@inheritDoc}
	 */
	@Override
	public ChannelID getConnectedChannelID() {

		return this.byteBufferedOutputChannel.getConnectedChannelID();
	}

	/**
	 * {@inheritDoc}
	 */
	@Override
	public JobID getJobID() {

		return this.byteBufferedOutputChannel.getJobID();
	}

	/**
	 * {@inheritDoc}
	 */
	@Override
	public boolean isInputChannel() {

		return this.byteBufferedOutputChannel.isInputChannel();
	}

	/**
	 * {@inheritDoc}
	 */
	@Override
	public void outOfByteBuffers() {
		this.byteBufferedOutputChannel.channelCapacityExhausted();
	}

	@Override
	public void queueTransferEnvelope(TransferEnvelope transferEnvelope) {
		
		if(transferEnvelope.getBuffer() != null) {
			LOG.error("Transfer envelope for output channel has buffer attached");
		}
		
		Iterator<AbstractEvent> it = transferEnvelope.getEventList().iterator();
		while(it.hasNext()) {
			
			this.byteBufferedOutputChannel.processEvent(it.next());
		}
	}
}
