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

package eu.stratosphere.nephele.taskmanager.direct;

import java.util.HashMap;
import java.util.Map;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import eu.stratosphere.nephele.configuration.Configuration;
import eu.stratosphere.nephele.configuration.GlobalConfiguration;
import eu.stratosphere.nephele.io.channels.ChannelID;
import eu.stratosphere.nephele.io.channels.direct.AbstractDirectInputChannel;
import eu.stratosphere.nephele.io.channels.direct.AbstractDirectOutputChannel;
import eu.stratosphere.nephele.io.channels.direct.DirectChannelBroker;
import eu.stratosphere.nephele.types.Record;

public class DirectChannelManager implements DirectChannelBroker {

	private static final Log LOG = LogFactory.getLog(DirectChannelManager.class);

	private static final int DEFAULT_NUMBER_OF_BUFFERS_PER_CHANNEL = 4;

	private static final int DEFAULT_BUFFER_SIZE_IN_RECORDS = 1024; // 1024 records

	private static final int DEFAULT_NUMBER_OF_CONNECTION_RETRIES = 30;

	private final int numberOfBuffersPerChannel;

	private final int bufferSizeInRecords;

	private final int numberOfConnectionRetries;

	private final Map<ChannelID, AbstractDirectInputChannel<? extends Record>> directInputChannels = new HashMap<ChannelID, AbstractDirectInputChannel<? extends Record>>();

	private final Map<ChannelID, AbstractDirectOutputChannel<? extends Record>> directOutputChannels = new HashMap<ChannelID, AbstractDirectOutputChannel<? extends Record>>();

	public DirectChannelManager() {

		final Configuration configuration = GlobalConfiguration.getConfiguration();

		this.numberOfBuffersPerChannel = configuration.getInteger("channel.inMemory.numberOfBuffersPerChannel",
			DEFAULT_NUMBER_OF_BUFFERS_PER_CHANNEL);
		this.bufferSizeInRecords = configuration.getInteger("channel.inMemory.bufferSizeInRecords",
			DEFAULT_BUFFER_SIZE_IN_RECORDS);
		this.numberOfConnectionRetries = configuration.getInteger("channel.inMemory.numberOfConnectionRetries",
			DEFAULT_NUMBER_OF_CONNECTION_RETRIES);
	}

	public void registerDirectInputChannel(AbstractDirectInputChannel<? extends Record> directInputChannel) {

		LOG.debug("Registering direct input channel " + directInputChannel.getID());

		synchronized (this.directInputChannels) {

			if (this.directInputChannels.containsKey(directInputChannel.getID())) {
				LOG.error("Direct input channel " + directInputChannel.getID() + " is already registered");
				return;
			}

			// Set broker
			directInputChannel.setDirectChannelBroker(this);
			// Initialize the channel buffers according to the configuration
			directInputChannel.initializeBuffers(this.numberOfBuffersPerChannel, this.bufferSizeInRecords);
			// Set number of connection retries
			directInputChannel.setNumberOfConnectionRetries(this.numberOfConnectionRetries);

			this.directInputChannels.put(directInputChannel.getID(), directInputChannel);
		}
	}

	public void registerDirectOutputChannel(AbstractDirectOutputChannel<? extends Record> directOutputChannel) {

		LOG.debug("Registering direct output channel " + directOutputChannel.getID());

		synchronized (this.directOutputChannels) {

			if (this.directOutputChannels.containsKey(directOutputChannel.getID())) {
				LOG.error("Direct output channel " + directOutputChannel.getID() + " is already registered");
				return;
			}

			// Set broker
			directOutputChannel.setDirectChannelBroker(this);
			// Set number of connection retries
			directOutputChannel.setNumberOfConnectionRetries(this.numberOfConnectionRetries);

			this.directOutputChannels.put(directOutputChannel.getID(), directOutputChannel);
		}

	}

	public void unregisterDirectInputChannel(AbstractDirectInputChannel<? extends Record> directInputChannel) {

		LOG.debug("Unregistering direct input channel " + directInputChannel.getID());

		synchronized (this.directInputChannels) {

			if (!this.directInputChannels.containsKey(directInputChannel.getID())) {
				LOG.error("Cannot find direct input channel " + directInputChannel.getID() + " to unregister");
			} else {
				this.directInputChannels.remove(directInputChannel.getID());
			}
		}
	}

	public void unregisterDirectOutputChannel(AbstractDirectOutputChannel<? extends Record> directOutputChannel) {

		LOG.debug("Unregistering direct output channel " + directOutputChannel.getID());

		synchronized (this.directOutputChannels) {

			if (!this.directOutputChannels.containsKey(directOutputChannel.getID())) {
				LOG.error("Cannot find direct output channel " + directOutputChannel.getID() + " to unregister");
			} else {
				this.directOutputChannels.remove(directOutputChannel.getID());
			}
		}
	}

	/**
	 * {@inheritDoc}
	 */
	@Override
	public AbstractDirectInputChannel<? extends Record> getDirectInputChannelByID(ChannelID id) {

		synchronized (this.directInputChannels) {
			return this.directInputChannels.get(id);
		}
	}

	/**
	 * {@inheritDoc}
	 */
	@Override
	public AbstractDirectOutputChannel<? extends Record> getDirectOutputChannelByID(ChannelID id) {

		synchronized (this.directOutputChannels) {
			return this.directOutputChannels.get(id);
		}
	}
}
