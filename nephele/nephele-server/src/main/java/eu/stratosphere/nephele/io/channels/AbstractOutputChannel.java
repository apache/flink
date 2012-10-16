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

package eu.stratosphere.nephele.io.channels;

import java.io.IOException;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import eu.stratosphere.nephele.event.task.AbstractEvent;
import eu.stratosphere.nephele.event.task.AbstractTaskEvent;
import eu.stratosphere.nephele.io.OutputGate;
import eu.stratosphere.nephele.io.compression.CompressionEvent;
import eu.stratosphere.nephele.io.compression.CompressionException;
import eu.stratosphere.nephele.io.compression.CompressionLevel;
import eu.stratosphere.nephele.io.compression.Compressor;
import eu.stratosphere.nephele.jobgraph.JobID;
import eu.stratosphere.nephele.types.Record;

/**
 * OutputChannel is an abstract base class to all different kinds of concrete
 * output channels that can be used. Input channels are always parameterized to
 * a specific type that can be transported through the channel.
 * 
 * @author warneke
 * @param <T>
 *        The Type of the record that can be transported through the channel.
 */
public abstract class AbstractOutputChannel<T extends Record> extends AbstractChannel {

	/**
	 * The log object used to report errors.
	 */
	private static final Log LOG = LogFactory.getLog(AbstractOutputChannel.class);

	/**
	 * The output gate this output channel is connected to.
	 */
	private final OutputGate<T> outputGate;

	/**
	 * The record serializer used to serialize records.
	 */
	private final RecordSerializer<T> recordSerializer;

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

	/**
	 * Creates a new abstract output channel.
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
	 * @param recordSerializer
	 *        the record serializer used to serialize records
	 */
	protected AbstractOutputChannel(final OutputGate<T> outputGate, final int channelIndex, final ChannelID channelID,
			final ChannelID connectedChannelID, final CompressionLevel compressionLevel,
			final RecordSerializer<T> recordSerializer) {

		super(channelIndex, channelID, connectedChannelID, compressionLevel);

		this.outputGate = outputGate;
		this.recordSerializer = recordSerializer;
	}

	/**
	 * Returns the output gate this channel is connected to.
	 * 
	 * @return the output gate this channel is connected to
	 */
	public OutputGate<T> getOutputGate() {
		return this.outputGate;
	}

	/**
	 * Writes a record to the channel. The operation may block until the record
	 * is completely written to the channel.
	 * 
	 * @param record
	 *        the record to be written to the channel
	 * @throws IOException
	 *         thrown if an error occurred while transmitting the record
	 */
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
			while (this.recordSerializer.dataLeftFromPreviousSerialization()) {

				if (!this.recordSerializer.read(this.dataBuffer)) {

					this.dataBuffer = this.compressor.compress(this.dataBuffer);
					// this.leasedWriteBuffer.flip();
					releaseWriteBuffer();
					requestWriteBufferFromBroker();
				}
			}
		} else {
			while (this.recordSerializer.dataLeftFromPreviousSerialization()) {

				if (!this.recordSerializer.read(this.dataBuffer)) {
					releaseWriteBuffer();
					requestWriteBufferFromBroker();
				}
			}
		}

		if (this.recordSerializer.dataLeftFromPreviousSerialization()) {
			throw new IOException("Serialization buffer is expected to be empty!");
		}

		this.recordSerializer.serialize(record);

		if (this.compressor != null) {
			if (!this.recordSerializer.read(this.dataBuffer)) {

				this.dataBuffer = this.compressor.compress(this.dataBuffer);
				// this.leasedWriteBuffer.flip();
				releaseWriteBuffer();
			}
		} else {
			if (!this.recordSerializer.read(this.dataBuffer)) {
				releaseWriteBuffer();
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
	public void releaseAllResources() {

		// TODO: Reconsider release of broker's resources here
		this.closeRequested = true;

		this.recordSerializer.clear();

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

	/**
	 * Requests the output channel to close. After calling this method no more records can be written
	 * to the channel. The channel is finally closed when all remaining data that may exist in internal buffers
	 * are written to the channel.
	 * 
	 * @throws InterruptedException
	 *         thrown if the thread is interrupted while requesting the close operation
	 * @throws IOException
	 *         thrown if an I/O error occurs while requesting the close operation
	 */
	public void requestClose() throws IOException, InterruptedException {

		if (!this.closeRequested) {
			this.closeRequested = true;
			if (this.recordSerializer.dataLeftFromPreviousSerialization()) {
				// make sure we serialized all data before we send the close event
				flush();
			}

			if (!isBroadcastChannel() || getChannelIndex() == 0) {
				transferEvent(new ByteBufferedChannelCloseEvent());
				flush();
			}
		}
	}

	/**
	 * Initializes the compressor object for this output channel.
	 * 
	 * @throws CompressionException
	 *         thrown if an error occurs during the initialization process
	 */
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
	public boolean isInputChannel() {
		return false;
	}

	public void flush() throws IOException, InterruptedException {

		// Get rid of remaining data in the serialization buffer
		while (this.recordSerializer.dataLeftFromPreviousSerialization()) {

			if (this.dataBuffer == null) {

				try {
					requestWriteBufferFromBroker();
				} catch (InterruptedException e) {
					LOG.error(e);
				}
			}
			if (this.compressor != null) {
				if (!this.recordSerializer.read(this.dataBuffer)) {
					this.dataBuffer = this.compressor.compress(this.dataBuffer);
					// this.leasedWriteBuffer.flip();
					releaseWriteBuffer();
				}
			} else {
				if (!this.recordSerializer.read(this.dataBuffer)) {
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
	public JobID getJobID() {
		return this.outputGate.getJobID();
	}

	/**
	 * Returns <code>true</code> if this channel is connected to an output gate which operates in broadcast mode,
	 * <code>false</code> otherwise.
	 * 
	 * @return <code>true</code> if the connected output gate operates in broadcase mode, <code>false</code> otherwise
	 */
	public boolean isBroadcastChannel() {

		return this.outputGate.isBroadcast();
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
	public boolean isClosed() throws IOException, InterruptedException {

		if (this.closeRequested && this.dataBuffer == null
			&& !this.recordSerializer.dataLeftFromPreviousSerialization()) {

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
	public void processEvent(final AbstractEvent event) {

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
	public void transferEvent(final AbstractEvent event) throws IOException, InterruptedException {

		flush();
		this.outputChannelBroker.transferEventToInputChannel(event);
	}
}
