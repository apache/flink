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

package eu.stratosphere.nephele.io.channels.direct;

import java.io.EOFException;
import java.io.IOException;
import java.util.ArrayDeque;

import eu.stratosphere.nephele.event.task.AbstractEvent;
import eu.stratosphere.nephele.event.task.AbstractTaskEvent;
import eu.stratosphere.nephele.io.InputGate;
import eu.stratosphere.nephele.io.RecordBuffer;
import eu.stratosphere.nephele.io.RecordDeserializer;
import eu.stratosphere.nephele.io.channels.AbstractInputChannel;
import eu.stratosphere.nephele.io.channels.ChannelID;
import eu.stratosphere.nephele.io.compression.CompressionLevel;
import eu.stratosphere.nephele.types.Record;

public abstract class AbstractDirectInputChannel<T extends Record> extends AbstractInputChannel<T> {

	/**
	 * The synchronization object to protect critical sections.
	 */
	private final Object synchronizationMontior = new Object();

	/**
	 * Number of attempt to connect to the corresponding output channel before giving up.
	 */
	private int numberOfConnectionRetries = 0;

	private static final long CONNECTION_SLEEP_INTERVAL = 1000;

	private ArrayDeque<RecordBuffer<T>> emptyBuffers;

	private ArrayDeque<RecordBuffer<T>> fullBuffers;

	private RecordBuffer<T> currentReadBuffer;

	private RecordBuffer<T> currentWriteBuffer;

	// TODO: Is this variable necessary
	private boolean currentReadBufferIsNull = true;

	/**
	 * Stores if the connected output channel has requested to close the channel.
	 */
	private boolean closeRequestedByRemote = false;

	private final RecordDeserializer<T> deserializer;

	/**
	 * The direct channel broker used to find the corresponding output channel.
	 */
	private DirectChannelBroker directChannelBroker = null;

	/**
	 * The connected in-memory output channel.
	 */
	private InMemoryOutputChannel<T> connectedInMemoryOutputChannel = null;

	/**
	 * Creates a new in-memory input channel.
	 * 
	 * @param inputGate
	 *        the input gate this channel is connected to
	 * @param channelIndex
	 *        the channel's index at the associated input gate
	 * @param type
	 *        the type of record transported through this channel
	 * @param channelID
	 *        the channel ID to assign to the new channel, <code>null</code> to generate a new ID
	 * @param compressionLevel
	 *        the level of compression to be used for this channel
	 */
	public AbstractDirectInputChannel(InputGate<T> inputGate, int channelIndex, RecordDeserializer<T> deserializer,
			ChannelID channelID, CompressionLevel compressionLevel) {
		super(inputGate, channelIndex, channelID, compressionLevel);

		this.fullBuffers = new ArrayDeque<RecordBuffer<T>>();
		this.emptyBuffers = new ArrayDeque<RecordBuffer<T>>();
		this.deserializer = deserializer;
	}

	/**
	 * Called by the corresponding in-memory output channel to notify
	 * this channel no more records will follow.
	 */
	void requestClose() {

		synchronized (this.synchronizationMontior) {

			if (this.closeRequestedByRemote) {
				return;
			}

			if (this.currentWriteBuffer != null) {

				this.fullBuffers.add(this.currentWriteBuffer);
				this.getInputGate().notifyRecordIsAvailable(getChannelIndex());
				this.currentWriteBuffer = null;
			}

			this.closeRequestedByRemote = true;
			// Notify the input gate, so that it will catch the EOF exception
			this.getInputGate().notifyRecordIsAvailable(getChannelIndex());
		}
	}

	public void initializeBuffers(int numberOfBuffersPerChannel, int bufferSizeInRecords) {

		synchronized (this.synchronizationMontior) {

			for (int i = 0; i < numberOfBuffersPerChannel; i++) {
				this.emptyBuffers.add(new RecordBuffer<T>(this.deserializer.getRecordType(), bufferSizeInRecords));
			}
		}
	}

	/**
	 * {@inheritDoc}
	 */
	@Override
	public T readRecord() throws IOException {

		if (this.currentReadBuffer == null) {

			synchronized (this.synchronizationMontior) {

				if (this.isClosed()) {
					throw new EOFException();
				}

				if (this.fullBuffers.isEmpty()) {
					return null;
				}

				this.currentReadBuffer = this.fullBuffers.poll();
				this.currentReadBufferIsNull = false;
			}
		}

		T record = this.currentReadBuffer.get();

		if (this.currentReadBuffer.getSize() == 0) {

			synchronized (this.synchronizationMontior) {

				this.emptyBuffers.add(this.currentReadBuffer);
				this.currentReadBuffer = null;
				this.currentReadBufferIsNull = true;
				this.synchronizationMontior.notify();
			}
		}

		return record;
	}

	/**
	 * {@inheritDoc}
	 */
	@Override
	public boolean isClosed() throws IOException {

		if (this.currentReadBuffer != null) {
			return false;
		}

		synchronized (this.synchronizationMontior) {

			if (this.closeRequestedByRemote && this.fullBuffers.isEmpty()) {
				return true;
			}
		}

		return false;
	}

	public boolean allQueuedRecordsConsumed() {

		synchronized (this.synchronizationMontior) {

			if (this.currentReadBufferIsNull && this.fullBuffers.isEmpty()) {
				return true;
			}

		}

		return false;
	}

	/**
	 * {@inheritDoc}
	 */
	@Override
	public void close() {

		synchronized (this.synchronizationMontior) {

			// TODO: Check for notification here

			// Make sure there are enough empty buffers, so the output channel will notice the
			// EnoughRecordsEmittedException
			if (this.currentReadBuffer != null) {
				this.currentReadBuffer.clear();
				this.emptyBuffers.add(this.currentReadBuffer);
				this.currentReadBuffer = null;
				this.currentReadBufferIsNull = true;
			}

			while (!this.fullBuffers.isEmpty()) {

				RecordBuffer<T> buffer = this.fullBuffers.poll();
				buffer.clear();
				this.emptyBuffers.add(buffer);
			}

			this.synchronizationMontior.notify();
		}
	}

	/**
	 * Forwards the given record to this in-memory input channel. This method
	 * may block until this in-memory input channel can accept the record.
	 * 
	 * @param record
	 *        the record to be forwarded to this input channel
	 */
	void forwardRecord(T record) throws IOException, InterruptedException {
		if (this.currentWriteBuffer == null) {

			synchronized (this.synchronizationMontior) {

				while (this.emptyBuffers.isEmpty()) {
					// Notify the framework that the channel capacity is exhausted
					if (this.connectedInMemoryOutputChannel == null) {
						this.connectedInMemoryOutputChannel = getConnectedOutputChannel();
					}
					this.connectedInMemoryOutputChannel.channelCapacityExhausted();
					this.synchronizationMontior.wait();
				}

				this.currentWriteBuffer = this.emptyBuffers.poll();
			}
		}

		this.currentWriteBuffer.put(record);

		if (this.currentWriteBuffer.getSize() >= this.currentWriteBuffer.capacity()) {

			synchronized (this.synchronizationMontior) {

				this.fullBuffers.add(this.currentWriteBuffer);
				this.getInputGate().notifyRecordIsAvailable(getChannelIndex());
				this.currentWriteBuffer = null;
			}
		}
	}

	/**
	 * Sets the direct channel broker this channel should contact to find
	 * its corresponding output channel.
	 * 
	 * @param directChannelBroker
	 *        the direct channel broker for discovering the corresponding output channel
	 */
	public void setDirectChannelBroker(DirectChannelBroker directChannelBroker) {
		this.directChannelBroker = directChannelBroker;
	}

	@SuppressWarnings("unchecked")
	private InMemoryOutputChannel<T> getConnectedOutputChannel() throws IOException {

		InMemoryOutputChannel<T> inMemoryOutputChannel = null;

		if (this.directChannelBroker == null) {
			throw new IOException("inMemoryChannelBroker is null!");
		}

		for (int i = 0; i < this.numberOfConnectionRetries; i++) {
			inMemoryOutputChannel = (InMemoryOutputChannel<T>) this.directChannelBroker
				.getDirectOutputChannelByID(getConnectedChannelID());
			if (inMemoryOutputChannel != null) {
				return inMemoryOutputChannel;
			}

			try {
				Thread.sleep(CONNECTION_SLEEP_INTERVAL);
			} catch (InterruptedException e) {
				// We need to finish this operation, otherwise the proper shutdown of the consumer is at risk
				e.printStackTrace();
			}
		}

		throw new IOException("Cannot find corresponding in-memory output channel for in-memory input channel "
			+ getID());
	}

	public void setNumberOfConnectionRetries(int numberOfConnectionRetries) {
		this.numberOfConnectionRetries = numberOfConnectionRetries;
	}

	@Override
	public void processEvent(AbstractEvent event) {

		// In-memory channels are currently not interested in events, just pass it on...
		if (AbstractTaskEvent.class.isInstance(event)) {
			getInputGate().deliverEvent((AbstractTaskEvent) event);
		} else {
			// TODO: Received unknown event
		}
	}

	@Override
	public void transferEvent(AbstractEvent event) throws IOException {

		if (this.connectedInMemoryOutputChannel == null) {
			this.connectedInMemoryOutputChannel = getConnectedOutputChannel();
		}

		this.connectedInMemoryOutputChannel.processEvent(event);
	}
}
