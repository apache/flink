/***********************************************************************************************************************
 *
 * Copyright (C) 2010-2012 by the Stratosphere project (http://stratosphere.eu)
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

import java.io.EOFException;
import java.io.IOException;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import eu.stratosphere.nephele.event.task.AbstractEvent;
import eu.stratosphere.nephele.event.task.AbstractTaskEvent;
import eu.stratosphere.nephele.io.InputGate;
import eu.stratosphere.nephele.io.compression.CompressionEvent;
import eu.stratosphere.nephele.io.compression.CompressionException;
import eu.stratosphere.nephele.io.compression.CompressionLevel;
import eu.stratosphere.nephele.io.compression.Decompressor;
import eu.stratosphere.nephele.jobgraph.JobID;
import eu.stratosphere.nephele.types.Record;

/**
 * InputChannel is an abstract base class to all different kinds of concrete
 * input channels that can be used. Input channels are always parameterized to
 * a specific type that can be transported through the channel.
 * 
 * @author warneke
 * @param <T>
 *        The Type of the record that can be transported through the channel.
 */
public abstract class AbstractInputChannel<T extends Record> extends AbstractChannel {

	private final InputGate<T> inputGate;

	/**
	 * The log object used to report warnings and errors.
	 */
	private static final Log LOG = LogFactory.getLog(AbstractInputChannel.class);

	/**
	 * The deserializer used to deserialize records.
	 */
	private final RecordDeserializer<T> deserializer;

	/**
	 * Buffer for the uncompressed (raw) data.
	 */
	private Buffer dataBuffer;

	private ByteBufferedInputChannelBroker inputChannelBroker;

	/**
	 * The decompressor object to decompress incoming data
	 */
	private Decompressor decompressor = null;

	/**
	 * The exception observed in this channel while processing the buffers. Checked and thrown
	 * per-buffer.
	 */
	private volatile IOException ioException;

	/**
	 * Stores the number of bytes read through this input channel since its instantiation.
	 */
	private long amountOfDataTransmitted;

	private volatile boolean brokerAggreedToCloseChannel;

	/**
	 * Creates a new abstract input channel.
	 * 
	 * @param inputGate
	 *        the input gate this channel is wired to
	 * @param channelIndex
	 *        the channel's index at the associated input gate
	 * @param type
	 *        the type of record transported through this channel
	 * @param channelID
	 *        the ID of the channel
	 * @param connectedChannelID
	 *        the ID of the channel this channel is connected to
	 * @param compressionLevel
	 *        the level of compression to be used for this channel
	 * @param deserializer
	 *        the record deserializer to be used
	 */
	protected AbstractInputChannel(final InputGate<T> inputGate, final int channelIndex, final ChannelID channelID,
			final ChannelID connectedChannelID, final CompressionLevel compressionLevel,
			final RecordDeserializer<T> deserializer) {
		super(channelIndex, channelID, connectedChannelID, compressionLevel);

		this.inputGate = inputGate;
		this.deserializer = deserializer;
	}

	/**
	 * Returns the input gate associated with the input channel.
	 * 
	 * @return the input gate associated with the input channel.
	 */
	public InputGate<T> getInputGate() {
		return this.inputGate;
	}

	/**
	 * Reads a record from the input channel. If currently no record is available the method
	 * returns <code>null</code>. If the channel is closed (i.e. no more records will be received), the method
	 * throws an {@link EOFException}.
	 * 
	 * @return a record that has been transported through the channel or <code>null</code> if currently no record is
	 *         available
	 * @throws IOException
	 *         thrown if the input channel is already closed {@link EOFException} or a transmission error has occurred
	 */
	public T readRecord(T target) throws IOException {

		if (isClosed()) {
			throw new EOFException();
		}

		return deserializeNextRecord(target);
	}

	/**
	 * Immediately closes the input channel. The corresponding output channels are
	 * notified if necessary. Any remaining records in any buffers or queue is considered
	 * irrelevant and is discarded.
	 * 
	 * @throws InterruptedException
	 *         thrown if the thread is interrupted while waiting for the channel to close
	 * @throws IOException
	 *         thrown if an I/O error occurs while closing the channel
	 */
	public void close() throws IOException, InterruptedException {

		this.deserializer.clear();
		if (this.dataBuffer != null) {
			releasedConsumedReadBuffer();
		}

		// This code fragment makes sure the isClosed method works in case the channel input has not been fully consumed
		if (this.getType() == ChannelType.NETWORK || this.getType() == ChannelType.INMEMORY) {
			while (!this.brokerAggreedToCloseChannel) {
				requestReadBufferFromBroker();
				if (this.dataBuffer != null) {
					releasedConsumedReadBuffer();
					continue;
				}
				Thread.sleep(200);
			}
		}

		/*
		 * Send close event to indicate the input channel has successfully
		 * processed all data it is interested in.
		 */
		final ChannelType type = getType();
		if (type == ChannelType.NETWORK || type == ChannelType.INMEMORY) {
			transferEvent(new ChannelCloseEvent());
		}
	}

	/**
	 * {@inheritDoc}
	 */
	@Override
	public boolean isClosed() throws IOException {

		if (this.dataBuffer != null) {
			return false;
		}

		if (this.ioException != null) {
			throw this.ioException;
		}

		if (!this.brokerAggreedToCloseChannel) {
			return false;
		}

		return true;
	}

	/**
	 * Initializes the decompressor object for this input channel.
	 * 
	 * @throws CompressionException
	 *         thrown if an error occurs during the initialization process
	 */
	public void initializeDecompressor() throws CompressionException {

		if (this.decompressor != null) {
			throw new IllegalStateException("Decompressor has already been initialized for channel " + getID());
		}

		if (this.inputChannelBroker == null) {
			throw new IllegalStateException("Input channel broker has not been set");
		}

		this.decompressor = this.inputChannelBroker.getDecompressor();
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
		return this.inputGate.getJobID();
	}

	/**
	 * Activates the input channel.
	 * 
	 * @throws IOException
	 *         thrown if an I/O error occurs while transmitting the activation event to the connected output channel
	 * @throws InterruptedException
	 *         thrown if the calling thread is interrupted while completing the activation request
	 */
	public void activate() throws IOException, InterruptedException {

		transferEvent(new ChannelActivateEvent());
	}

	/**
	 * Deserializes the next record from one of the data buffers.
	 * 
	 * @return the next record or <code>null</code> if all data buffers are exhausted
	 * @throws IOException
	 *         thrown if the record cannot be deserialized
	 */
	private T deserializeNextRecord(final T target) throws IOException {

		if (this.dataBuffer == null) {

			if (this.ioException != null) {
				throw this.ioException;
			}

			requestReadBufferFromBroker();

			if (this.dataBuffer == null) {
				return null;
			}

			if (this.decompressor != null) {
				this.dataBuffer = this.decompressor.decompress(this.dataBuffer);
			}
		}

		final T nextRecord = this.deserializer.readData(target, this.dataBuffer);

		if (this.dataBuffer.remaining() == 0) {
			releasedConsumedReadBuffer();
		}

		return nextRecord;
	}

	private void releasedConsumedReadBuffer() {

		// Keep track of number of bytes transmitted through this channel
		this.amountOfDataTransmitted += this.dataBuffer.size();

		this.inputChannelBroker.releaseConsumedReadBuffer(this.dataBuffer);
		this.dataBuffer = null;
	}

	public void setInputChannelBroker(ByteBufferedInputChannelBroker inputChannelBroker) {
		this.inputChannelBroker = inputChannelBroker;
	}

	private void requestReadBufferFromBroker() {

		// this.leasedReadBuffer = this.inputChannelBroker.getReadBufferToConsume();
		final Buffer buffer = this.inputChannelBroker.getReadBufferToConsume();

		if (buffer == null) {
			return;
		}

		this.dataBuffer = buffer;
	}

	public void checkForNetworkEvents() {

		// Create an event for the input gate queue
		this.getInputGate().notifyRecordIsAvailable(getChannelIndex());
	}

	@Override
	public void processEvent(AbstractEvent event) {

		if (ChannelCloseEvent.class.isInstance(event)) {
			// System.out.println("Received close event");
			this.brokerAggreedToCloseChannel = true;
			// Make sure the application wake's up to check this
			checkForNetworkEvents();
		} else if (AbstractTaskEvent.class.isInstance(event)) {
			// Simply dispatch the event if it comes from a task
			getInputGate().deliverEvent((AbstractTaskEvent) event);
		} else if (CompressionEvent.class.isInstance(event)) {
			final CompressionEvent compressionEvent = (CompressionEvent) event;
			this.decompressor.setCurrentInternalDecompressionLibraryIndex(compressionEvent
				.getCurrentInternalCompressionLibraryIndex());
		} else {
			// TODO: Handle unknown event
			LOG.error("Received unknown event: " + event);
		}
	}

	/**
	 * {@inheritDoc}
	 */
	@Override
	public void transferEvent(AbstractEvent event) throws IOException, InterruptedException {

		this.inputChannelBroker.transferEventToOutputChannel(event);
	}

	public void reportIOException(IOException ioe) {

		this.ioException = ioe;
	}

	/**
	 * {@inheritDoc}
	 */
	@Override
	public long getAmountOfDataTransmitted() {

		return this.amountOfDataTransmitted;
	}

	/**
	 * {@inheritDoc}
	 */
	@Override
	public void releaseAllResources() {

		this.brokerAggreedToCloseChannel = true;

		this.deserializer.clear();

		// The buffers are recycled by the input channel wrapper

		if (this.decompressor != null) {
			this.decompressor.shutdown();
		}
	}

	/**
	 * Notify the channel that a data unit has been consumed.
	 */
	public void notifyDataUnitConsumed() {

		this.getInputGate().notifyDataUnitConsumed(getChannelIndex());
	}
}
