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

import java.io.IOException;

import eu.stratosphere.nephele.event.task.AbstractEvent;
import eu.stratosphere.nephele.event.task.AbstractTaskEvent;
import eu.stratosphere.nephele.io.OutputGate;
import eu.stratosphere.nephele.io.channels.AbstractOutputChannel;
import eu.stratosphere.nephele.io.channels.ChannelID;
import eu.stratosphere.nephele.io.compression.CompressionLevel;
import eu.stratosphere.nephele.types.Record;

public abstract class AbstractDirectOutputChannel<T extends Record> extends AbstractOutputChannel<T> {

	/**
	 * The connected direct input channel.
	 */
	private AbstractDirectInputChannel<T> connectedDirectInputChannel = null;

	/**
	 * Number of attempt to connect to the corresponding input channel before giving up.
	 */
	private int numberOfConnectionRetries = 0;

	private static final long CONNECTION_SLEEP_INTERVAL = 1000;

	/**
	 * The direct channel broker used to find the corresponding input channel.
	 */
	private DirectChannelBroker directChannelBroker = null;

	/**
	 * Stores if the channel is requested to be closed.
	 */
	private boolean closeRequested = false;

	private IOException connectionFailureException = null;

	/**
	 * Creates a new direct output channel.
	 * 
	 * @param outputGate
	 *        the output gate the new channel is wired to.
	 * @param channelIndex
	 *        the channel's index at the associated output gate
	 * @param channelID
	 *        the channel ID to assign to the new channel, <code>null</code> to generate a new ID
	 * @param compressionLevel
	 *        the level of compression to be used for this channel
	 */
	public AbstractDirectOutputChannel(OutputGate<T> outputGate, int channelIndex, ChannelID channelID,
			CompressionLevel compressionLevel) {
		super(outputGate, channelIndex, channelID, compressionLevel);
	}

	/**
	 * Sets the direct channel broker this channel should contact to find
	 * its corresponding input channel.
	 * 
	 * @param directChannelBroker
	 *        the direct channel broker for discovering the corresponding input channel
	 */
	public void setDirectChannelBroker(DirectChannelBroker directChannelBroker) {
		this.directChannelBroker = directChannelBroker;
	}

	/**
	 * {@inheritDoc}
	 */
	@Override
	public void writeRecord(T record) throws IOException, InterruptedException {

		if (this.connectedDirectInputChannel == null) {
			this.connectedDirectInputChannel = getConnectedInputChannel();
		}

		this.connectedDirectInputChannel.forwardRecord(record);
	}

	/**
	 * {@inheritDoc}
	 */
	@Override
	public boolean isClosed() throws IOException {

		if (this.connectionFailureException != null) {
			// Re-throw exception that may have occurred on requestClose() call
			throw this.connectionFailureException;
		}

		// As we don't buffer records this channels is closed in the same moment the close is requested
		if (!this.closeRequested) {
			return false;
		}

		if (this.connectedDirectInputChannel == null) {
			return true;
		}

		if (!this.connectedDirectInputChannel.allQueuedRecordsConsumed()) {
			return false;
		}

		return true;
	}

	/**
	 * {@inheritDoc}
	 */
	@Override
	public void requestClose() throws InterruptedException {

		this.closeRequested = true;
		if (this.connectedDirectInputChannel == null) {

			try {
				this.connectedDirectInputChannel = getConnectedInputChannel();
			} catch (IOException e) {
				// Catch exception and re-throw it on isClosed() call
				this.connectionFailureException = e;
				return;
			}
		}

		this.connectedDirectInputChannel.requestClose();
	}

	public void setNumberOfConnectionRetries(int numberOfConnectionRetries) {
		this.numberOfConnectionRetries = numberOfConnectionRetries;
	}

	@SuppressWarnings("unchecked")
	private AbstractDirectInputChannel<T> getConnectedInputChannel() throws IOException, InterruptedException {

		AbstractDirectInputChannel<T> directInputChannel = null;

		if (this.directChannelBroker == null) {
			throw new IOException("directChannelBroker is null!");
		}

		for (int i = 0; i < this.numberOfConnectionRetries; i++) {
			directInputChannel = (AbstractDirectInputChannel<T>) this.directChannelBroker
				.getDirectInputChannelByID(getConnectedChannelID());
			if (directInputChannel != null) {
				return directInputChannel;
			}

			Thread.sleep(CONNECTION_SLEEP_INTERVAL);
		}

		throw new IOException("Cannot find corresponding in-memory input channel for in-memory output channel "
			+ getID());
	}

	@Override
	public void processEvent(AbstractEvent event) {

		// In-memory channels are currently not interested in events, just pass it on...
		if (AbstractTaskEvent.class.isInstance(event)) {
			getOutputGate().deliverEvent((AbstractTaskEvent) event);
		} else {
			// TODO: Received unknown event
		}
	}

	/**
	 * {@inheritDoc}
	 */
	@Override
	public void transferEvent(AbstractEvent event) throws IOException, InterruptedException {

		if (this.connectedDirectInputChannel == null) {
			this.connectedDirectInputChannel = getConnectedInputChannel();
		}

		this.connectedDirectInputChannel.processEvent(event);
	}

	/**
	 * {@inheritDoc}
	 */
	@Override
	public void flush() throws IOException, InterruptedException {
		if (this.connectedDirectInputChannel == null) {
			this.connectedDirectInputChannel = getConnectedInputChannel();
		}

		this.connectedDirectInputChannel.requestFlush();
	}

	/**
	 * {@inheritDoc}
	 */
	@Override
	public void releaseResources() {

		this.closeRequested = true;
	}
}
