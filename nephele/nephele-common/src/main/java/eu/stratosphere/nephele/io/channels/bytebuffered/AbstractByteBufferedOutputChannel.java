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

package eu.stratosphere.nephele.io.channels.bytebuffered;

import java.io.IOException;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import eu.stratosphere.nephele.event.task.AbstractEvent;
import eu.stratosphere.nephele.event.task.AbstractTaskEvent;
import eu.stratosphere.nephele.io.OutputGate;
import eu.stratosphere.nephele.io.channels.AbstractOutputChannel;
import eu.stratosphere.nephele.io.channels.Buffer;
import eu.stratosphere.nephele.io.channels.ChannelID;
import eu.stratosphere.nephele.io.channels.ChannelType;
import eu.stratosphere.nephele.io.channels.SerializationBuffer;
import eu.stratosphere.nephele.io.compression.CompressionEvent;
import eu.stratosphere.nephele.io.compression.CompressionException;
import eu.stratosphere.nephele.io.compression.CompressionLevel;
import eu.stratosphere.nephele.io.compression.Compressor;
import eu.stratosphere.nephele.types.Record;

public abstract class AbstractByteBufferedOutputChannel<T extends Record> extends AbstractOutputChannel<T> {

	/**
	 * The serialization buffer used to serialize records.
	 */
	private final SerializationBuffer<T> serializationBuffer = new SerializationBuffer<T>();

	/**
	 * Buffer for the serialized output data.
	 */
	private Buffer dataBuffer = null;

	/**
	 * Stores whether the channel is requested to be closed.
	 */
	private boolean closeRequested = false;

	/**
	 * The output channel broker the channel should contact to request and release write buffers.
	 */
	private ByteBufferedOutputChannelBroker outputChannelBroker = null;

	/**
	 * The compressor used to compress the outgoing data.
	 */
	private Compressor compressor = null;

	/**
	 * Stores the number of bytes transmitted through this output channel since its instantiation.
	 */
	private long amountOfDataTransmitted = 0L;

	private static final Log LOG = LogFactory.getLog(AbstractByteBufferedOutputChannel.class);

	/**
	 * Creates a new byte buffered output channel.
	 * 
	 * @param outputGate
	 *        the output gate this channel is wired to
	 * @param channelIndex
	 *        the channel's index at the associated output gate
	 * @param channelID
	 *        the ID of the channel
	 * @param connectedChannelID
	 *        the ID of the channel this channel is connected to
	 * @param compressionLevel
	 *        the level of compression to be used for this channel
	 */
	protected AbstractByteBufferedOutputChannel(final OutputGate<T> outputGate, final int channelIndex,
			final ChannelID channelID, final ChannelID connectedChannelID, final CompressionLevel compressionLevel) {
		super(outputGate, channelIndex, channelID, connectedChannelID, compressionLevel);
	}

	/**
	 * {@inheritDoc}
	 */
	@Override
	public boolean isClosed() throws IOException, InterruptedException {

		if (this.closeRequested && this.dataBuffer == null
			&& !this.serializationBuffer.dataLeftFromPreviousSerialization()) {

			if (!this.outputChannelBroker.hasDataLeftToTransmit()) {
				return true;
			}
		}

		return false;
	}

	/**
	 * {@inheritDoc}
	 */
	@Override
	public void requestClose() throws IOException, InterruptedException {

		if (!this.closeRequested) {
			this.closeRequested = true;
			if (this.serializationBuffer.dataLeftFromPreviousSerialization()) {
				// make sure we serialized all data before we send the close event
				flush();
			}

			if (getType() == ChannelType.INMEMORY || !isBroadcastChannel() || getChannelIndex() == 0) {
				transferEvent(new ByteBufferedChannelCloseEvent());
				flush();
			}
		}
	}

	/**
	 * Requests a new write buffer from the framework. This method blocks until the requested buffer is available.
	 * 
	 * @throws InterruptedException
	 *         thrown if the thread is interrupted while waiting for the buffer
	 * @throws IOException
	 *         thrown if an I/O error occurs while waiting for the buffer
	 */
	private void requestWriteBufferFromBroker() throws InterruptedException, IOException {

		this.dataBuffer = this.outputChannelBroker.requestEmptyWriteBuffer();
	}

	/**
	 * Returns the filled buffer to the framework and triggers further processing.
	 * 
	 * @throws IOException
	 *         thrown if an I/O error occurs while releasing the buffers
	 * @throws InterruptedException
	 *         thrown if the thread is interrupted while releasing the buffers
	 */
	private void releaseWriteBuffer() throws IOException, InterruptedException {

		if (getCompressionLevel() == CompressionLevel.DYNAMIC_COMPRESSION) {
			this.outputChannelBroker.transferEventToInputChannel(new CompressionEvent(this.compressor
				.getCurrentInternalCompressionLibraryIndex()));
		}

		// Keep track of number of bytes transmitted through this channel
		this.amountOfDataTransmitted += this.dataBuffer.size();

		this.outputChannelBroker.releaseWriteBuffer(this.dataBuffer);
		this.dataBuffer = null;
	}

	/**
	 * {@inheritDoc}
	 */
	@Override
	public void writeRecord(T record) throws IOException, InterruptedException {

		// Get a write buffer from the broker
		if (this.dataBuffer == null) {
			requestWriteBufferFromBroker();
		}

		if (this.closeRequested) {
			throw new IOException("Channel is aready requested to be closed");
		}

		// Check if we can accept new records or if there are still old
		// records to be transmitted
		if (this.compressor != null) {
			while (this.serializationBuffer.dataLeftFromPreviousSerialization()) {

				this.serializationBuffer.read(this.dataBuffer);
				if (this.dataBuffer.remaining() == 0) {

					this.dataBuffer = this.compressor.compress(this.dataBuffer);
					// this.leasedWriteBuffer.flip();
					releaseWriteBuffer();
					requestWriteBufferFromBroker();
				}
			}
		} else {
			while (this.serializationBuffer.dataLeftFromPreviousSerialization()) {

				this.serializationBuffer.read(this.dataBuffer);
				if (this.dataBuffer.remaining() == 0) {
					releaseWriteBuffer();
					requestWriteBufferFromBroker();
				}
			}
		}

		if (this.serializationBuffer.dataLeftFromPreviousSerialization()) {
			throw new IOException("Serialization buffer is expected to be empty!");
		}

		this.serializationBuffer.serialize(record);

		if (this.compressor != null) {
			this.serializationBuffer.read(this.dataBuffer);

			if (this.dataBuffer.remaining() == 0) {
				this.dataBuffer = this.compressor.compress(this.dataBuffer);
				// this.leasedWriteBuffer.flip();
				releaseWriteBuffer();
			}
		} else {
			this.serializationBuffer.read(this.dataBuffer);
			if (this.dataBuffer.remaining() == 0) {
				releaseWriteBuffer();
			}
		}
	}

	/**
	 * Sets the output channel broker this channel should contact to request and release write buffers.
	 * 
	 * @param byteBufferedOutputChannelBroker
	 *        the output channel broker the channel should contact to request and release write buffers
	 */
	public void setByteBufferedOutputChannelBroker(ByteBufferedOutputChannelBroker byteBufferedOutputChannelBroker) {

		this.outputChannelBroker = byteBufferedOutputChannelBroker;
	}

	/**
	 * {@inheritDoc}
	 */
	@Override
	public void initializeCompressor() throws CompressionException {

		if (this.compressor != null) {
			throw new IllegalStateException("Decompressor has already been initialized for channel " + getID());
		}

		if (this.outputChannelBroker == null) {
			throw new IllegalStateException("Input channel broker has not been set");
		}

		this.compressor = this.outputChannelBroker.getCompressor();
	}

	/**
	 * {@inheritDoc}
	 */
	@Override
	public void processEvent(AbstractEvent event) {

		if (event instanceof AbstractTaskEvent) {
			getOutputGate().deliverEvent((AbstractTaskEvent) event);
		} else {
			LOG.error("Channel " + getID() + " received unknown event " + event);
		}
	}

	/**
	 * {@inheritDoc}
	 */
	@Override
	public void transferEvent(AbstractEvent event) throws IOException, InterruptedException {

		flush();
		this.outputChannelBroker.transferEventToInputChannel(event);
	}

	/**
	 * {@inheritDoc}
	 */
	@Override
	public void flush() throws IOException, InterruptedException {

		// Get rid of remaining data in the serialization buffer
		while (this.serializationBuffer.dataLeftFromPreviousSerialization()) {

			if (this.dataBuffer == null) {

				try {
					requestWriteBufferFromBroker();
				} catch (InterruptedException e) {
					LOG.error(e);
				}
			}
			if (this.compressor != null) {
				this.serializationBuffer.read(this.dataBuffer);
				if (this.dataBuffer.remaining() == 0) {
					this.dataBuffer = this.compressor.compress(this.dataBuffer);
					// this.leasedWriteBuffer.flip();
					releaseWriteBuffer();
				}
			} else {
				this.serializationBuffer.read(this.dataBuffer);
				if (this.dataBuffer.remaining() == 0) {
					releaseWriteBuffer();
				}
			}
		}

		// Get rid of the leased write buffer
		if (this.compressor != null) {
			if (this.dataBuffer != null) {
				this.dataBuffer = this.compressor.compress(this.dataBuffer);

				// this.leasedWriteBuffer.flip();
				releaseWriteBuffer();
			}
		} else {
			if (this.dataBuffer != null) {
				releaseWriteBuffer();
			}
		}
	}

	/**
	 * {@inheritDoc}
	 */
	@Override
	public void releaseAllResources() {

		// TODO: Reconsider release of broker's resources here
		this.closeRequested = true;

		this.serializationBuffer.clear();

		if (this.dataBuffer != null) {
			this.dataBuffer.recycleBuffer();
			this.dataBuffer = null;
		}

		if (this.compressor != null) {
			this.compressor.shutdown();
		}
	}

	/**
	 * {@inheritDoc}
	 */
	@Override
	public long getAmountOfDataTransmitted() {

		return this.amountOfDataTransmitted;
	}
}
