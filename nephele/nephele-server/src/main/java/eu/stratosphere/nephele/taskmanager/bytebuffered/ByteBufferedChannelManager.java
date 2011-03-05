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
import java.net.InetSocketAddress;
import java.nio.ByteBuffer;
import java.nio.channels.ReadableByteChannel;
import java.nio.channels.SocketChannel;
import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.Deque;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Queue;
import java.util.Set;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import eu.stratosphere.nephele.configuration.Configuration;
import eu.stratosphere.nephele.configuration.GlobalConfiguration;
import eu.stratosphere.nephele.event.task.AbstractEvent;
import eu.stratosphere.nephele.event.task.EventList;
import eu.stratosphere.nephele.instance.InstanceConnectionInfo;
import eu.stratosphere.nephele.io.channels.Buffer;
import eu.stratosphere.nephele.io.channels.BufferFactory;
import eu.stratosphere.nephele.io.channels.ChannelID;
import eu.stratosphere.nephele.io.channels.FileBufferManager;
import eu.stratosphere.nephele.io.channels.bytebuffered.AbstractByteBufferedInputChannel;
import eu.stratosphere.nephele.io.channels.bytebuffered.AbstractByteBufferedOutputChannel;
import eu.stratosphere.nephele.io.channels.bytebuffered.BufferPairRequest;
import eu.stratosphere.nephele.io.channels.bytebuffered.BufferPairResponse;
import eu.stratosphere.nephele.protocols.ChannelLookupProtocol;
import eu.stratosphere.nephele.taskmanager.bufferprovider.BufferProvider;
import eu.stratosphere.nephele.types.Record;
import eu.stratosphere.nephele.util.StringUtils;

public final class ByteBufferedChannelManager implements TransferEnvelopeDispatcher {

	/**
	 * The log object used to report problems and errors.
	 */
	private static final Log LOG = LogFactory.getLog(ByteBufferedChannelManager.class);

	private final BufferProvider bufferProvider;

	private final Map<ChannelID, ByteBufferedChannelWrapper> registeredChannels = new HashMap<ChannelID, ByteBufferedChannelWrapper>();

	//private final Map<ChannelID, InetSocketAddress> connectionAddresses = new HashMap<ChannelID, InetSocketAddress>();

	private final NetworkConnectionManager networkConnectionManager;

	private final ChannelLookupProtocol channelLookupService;

	private final InstanceConnectionInfo localConnectionInfo;

	public ByteBufferedChannelManager(ChannelLookupProtocol channelLookupService,
			InstanceConnectionInfo localInstanceConnectionInfo)
												throws IOException {

		this.channelLookupService = channelLookupService;

		this.localConnectionInfo = localInstanceConnectionInfo;

		this.bufferProvider = BufferProvider.getBufferProvider();

		this.networkConnectionManager = new NetworkConnectionManager(this.bufferProvider, this,
			localInstanceConnectionInfo.getAddress(),
			localInstanceConnectionInfo.getDataPort());
	}

	/**
	 * Registers the byte buffered input channel with the byte buffered channel manager.
	 * 
	 * @param byteBufferedInputChannel
	 *        the byte buffered input channel to register
	 */
	public void registerByteBufferedInputChannel(
			AbstractByteBufferedInputChannel<? extends Record> byteBufferedInputChannel) {

		LOG.debug("Registering byte buffered input channel " + byteBufferedInputChannel.getID());

		synchronized (this.registeredChannels) {

			if (this.registeredChannels.containsKey(byteBufferedInputChannel.getID())) {
				LOG.error("Byte buffered input channel " + byteBufferedInputChannel.getID() + " is already registered");
				return;
			}

			final ByteBufferedInputChannelWrapper networkInputChannelWrapper = new ByteBufferedInputChannelWrapper(
				byteBufferedInputChannel, this, this.bufferProvider);
			this.registeredChannels.put(byteBufferedInputChannel.getID(), networkInputChannelWrapper);
		}
	}

	/**
	 * Registers the byte buffered output channel with the byte buffered channel manager.
	 * 
	 * @param byteBufferedOutputChannel
	 * @param channelGroup
	 */
	public void registerByteBufferedOutputChannel(
			AbstractByteBufferedOutputChannel<? extends Record> byteBufferedOutputChannel,
			ByteBufferedOutputChannelGroup channelGroup) {

		LOG.debug("Registering byte buffered output channel " + byteBufferedOutputChannel.getID());

		synchronized (this.registeredChannels) {

			if (this.registeredChannels.containsKey(byteBufferedOutputChannel.getID())) {
				LOG.error("Byte buffered output channel " + byteBufferedOutputChannel.getID()
					+ " is already registered");
				return;
			}

			ByteBufferedOutputChannelWrapper channelWrapper = null;
			switch (byteBufferedOutputChannel.getType()) {
			case FILE:
			case NETWORK:
				channelWrapper = new ByteBufferedOutputChannelWrapper(byteBufferedOutputChannel, channelGroup);
				break;
			default:
				LOG.error("Cannot find channel wrapper for byte buffered channel " + byteBufferedOutputChannel.getID());
				return;
			}

			// Register channel with channel group in case we need checkpointing later on
			channelGroup.registerOutputChannel(byteBufferedOutputChannel.getID());
			// Register out-of-buffers listener
			this.bufferProvider.registerOutOfWriteBuffersListener(channelWrapper);

			this.registeredChannels.put(byteBufferedOutputChannel.getID(), channelWrapper);
		}
	}

	public void unregisterByteBufferedInputChannel(
			AbstractByteBufferedInputChannel<? extends Record> byteBufferedInputChannel) {

		LOG.debug("Unregistering byte buffered input channel " + byteBufferedInputChannel.getID());

		synchronized (this.registeredChannels) {

			if (this.registeredChannels.remove(byteBufferedInputChannel.getID()) == null) {
				LOG.error("Cannot find byte buffered input channel " + byteBufferedInputChannel.getID()
					+ " to unregister");
				return;
			}
		}

		// Make sure all buffers are released in case the channel is unregistered as a result of an error
		Queue<TransferEnvelope> incomingTransferEnvelopes = null;

		/*synchronized (this.queuedIncomingTransferEnvelopes) {

			incomingTransferEnvelopes = this.queuedIncomingTransferEnvelopes.remove(byteBufferedInputChannel.getID());
		}*/

		if (incomingTransferEnvelopes == null) {
			// No more data buffered for us, we are done...
			return;
		}

		// A dump thread might be working on the same queue, so access must be synchronized
		/*synchronized (incomingTransferEnvelopes) {

			final Iterator<TransferEnvelope> it = incomingTransferEnvelopes.iterator();
			while (it.hasNext()) {
				final TransferEnvelope envelope = it.next();
				if (envelope.getBuffer() != null) {
					envelope.getBuffer().recycleBuffer();
				}
			}
		}*/
	}

	public void unregisterByteBufferedOutputChannel(
			AbstractByteBufferedOutputChannel<? extends Record> byteBufferedOutputChannel) {

		LOG.debug("Unregistering byte buffered output channel " + byteBufferedOutputChannel.getID());

		ByteBufferedOutputChannelWrapper channelWrapper;

		synchronized (this.registeredChannels) {

			channelWrapper = (ByteBufferedOutputChannelWrapper) this.registeredChannels
				.remove(byteBufferedOutputChannel.getID());
			if (channelWrapper == null) {
				LOG.error("Cannot find byte buffered output channel " + byteBufferedOutputChannel.getID()
					+ " to unregister");
				return;
			}
		}

		// Unregister out-of-buffer listener
		this.bufferProvider.unregisterOutOfWriteBuffersLister(channelWrapper);

		// Make sure all output buffers are leased and recycled
		/*InetSocketAddress connectionAddress = null;
		synchronized (this.connectionAddresses) {
			connectionAddress = this.connectionAddresses.remove(byteBufferedOutputChannel.getID());
		}
		if (connectionAddress == null) {
			// Apparently, the connected task has not yet transmitted any data, so no buffers can be queued
			return;
		}*/

		// Make sure all queued outgoing buffers are dropped and recycled
		/*synchronized (this.outgoingConnections) {
			final OutgoingConnection outgoingConnection = this.outgoingConnections.get(connectionAddress);
			if (outgoingConnection != null) {
				outgoingConnection.dropAllQueuedEnvelopesForChannel(byteBufferedOutputChannel.getID(), true);
				if (outgoingConnection.canBeRemoved()) {
					// reflects no envelopes, no
					// currentEnvelope and not connected
					this.outgoingConnections.remove(connectionAddress);
				}
			}
		}*/
	}

	public void queueIncomingTransferEnvelope(TransferEnvelope transferEnvelope) throws IOException {

		final ChannelID targetID = transferEnvelope.getTarget();
		ByteBufferedChannelWrapper targetChannelWrapper = null;
		if (targetID == null) {
			throw new IOException("Cannot process incoming transfer envelope: target channel ID is null!");
		}

		synchronized (this.registeredChannels) {
			targetChannelWrapper = this.registeredChannels.get(targetID);
			if (targetChannelWrapper == null) {

				// Release buffer immediately
				if (transferEnvelope.getBuffer() != null) {
					transferEnvelope.getBuffer().recycleBuffer();
				}

				throw new IOException("Cannot find target channel to ID " + targetID
					+ " to process incoming transfer envelope");
			}
		}

		if (targetChannelWrapper.isInputChannel()) {

			final ByteBufferedInputChannelWrapper networkInputChannelWrapper = (ByteBufferedInputChannelWrapper) targetChannelWrapper;
			networkInputChannelWrapper.queueIncomingTransferEnvelope(transferEnvelope);

		} else {

			final ByteBufferedOutputChannelWrapper networkOutputChannelWrapper = (ByteBufferedOutputChannelWrapper) targetChannelWrapper;

			// In case of an output channel, we only expect events and no buffers
			if (transferEnvelope.getBuffer() != null) {
				LOG.error("Incoming transfer envelope for network output channel "
					+ targetChannelWrapper.getChannelID() + " has a buffer attached");
			}
			// Process the events immediately
			final EventList eventList = transferEnvelope.getEventList();
			final Iterator<AbstractEvent> iterator = eventList.iterator();
			while (iterator.hasNext()) {
				networkOutputChannelWrapper.processEvent(iterator.next());
			}
		}
	}

	/*private InetSocketAddress getPeerConnectionAddress(ChannelID sourceChannelID) {

		InetSocketAddress connectionAddress = null;

		synchronized (this.connectionAddresses) {
			connectionAddress = this.connectionAddresses.get(sourceChannelID);
		}

		if (connectionAddress == null) {
			ByteBufferedChannelWrapper channelWrapper = null;
			synchronized (this.registeredChannels) {
				channelWrapper = this.registeredChannels.get(sourceChannelID);
				if (channelWrapper == null) {
					LOG.error("Cannot find channel object for ID " + sourceChannelID + " to do lookup");
					return null;
				}
			}

			InstanceConnectionInfo ici = null;
			try {

				while (true) {

					final ConnectionInfoLookupResponse lookupResponse = this.channelLookupService.lookupConnectionInfo(
						this.localConnectionInfo, channelWrapper.getJobID(), channelWrapper.getConnectedChannelID());

					if (lookupResponse.receiverNotFound()) {
						throw new IOException("Task with channel ID " + channelWrapper.getConnectedChannelID()
							+ " does not appear to be running");
					}

					if (lookupResponse.receiverNotReady()) {
						try {
							Thread.sleep(500);
						} catch (InterruptedException e) {
							return null;
						}
						continue;
					}

					if (lookupResponse.receiverReady()) {
						ici = lookupResponse.getRemoteTargets().get(0);
						break;
					}
				}

			} catch (IOException e) {
				LOG.error(StringUtils.stringifyException(e));
				return null;
			}
			if (ici != null) {
				connectionAddress = new InetSocketAddress(ici.getAddress(), ici.getDataPort());
				synchronized (this.connectionAddresses) {
					this.connectionAddresses.put(sourceChannelID, connectionAddress);
				}
			}
		}

		return connectionAddress;
	}*/

	/*
	 * public int getNumberOfQueuedOutgoingEnvelopes(ChannelID sourceChannelID) {
	 * final InetSocketAddress socketAddress = getPeerConnectionAddress(sourceChannelID);
	 * if (socketAddress == null) {
	 * return 0;
	 * }
	 * synchronized (this.outgoingConnections) {
	 * final OutgoingConnection outgoingConnection = this.outgoingConnections.get(socketAddress);
	 * if (outgoingConnection == null) {
	 * return 0;
	 * }
	 * return outgoingConnection.getNumberOfQueuedEnvelopesForChannel(sourceChannelID, true);
	 * }
	 * }
	 */

	/**
	 * Shuts down the network channel manager and
	 * stops all its internal processes.
	 */
	public void shutdown() {

	}

	public void reportIOExceptionForAllInputChannels(IOException ioe) {

		synchronized (this.registeredChannels) {

			final Iterator<ByteBufferedChannelWrapper> it = this.registeredChannels.values().iterator();

			while (it.hasNext()) {

				final ByteBufferedChannelWrapper channelWrapper = it.next();
				if (channelWrapper.isInputChannel()) {
					channelWrapper.reportIOException(ioe);
				}
			}
		}
	}

	public void reportIOExceptionForOutputChannel(ChannelID sourceChannelID, IOException ioe) {

		ByteBufferedChannelWrapper channelWrapper = null;
		synchronized (this.registeredChannels) {
			channelWrapper = this.registeredChannels.get(sourceChannelID);
		}
		if (channelWrapper == null) {
			LOG.error("Cannot find network output channel with ID " + sourceChannelID);
			return;
		}

		if (channelWrapper.isInputChannel()) {
			channelWrapper.reportIOException(ioe);
		}
	}

	public BufferProvider getBufferProvider() {
		return this.bufferProvider;
	}

	public NetworkConnectionManager getNetworkConnectionManager() {
		
		return this.networkConnectionManager;
	}
	
	/**
	 * {@inheritDoc}
	 */
	@Override
	public void processEnvelope(TransferEnvelope transferEnvelope) {
		// TODO Auto-generated method stub

		System.out.println("Received transferEnvelope ");
	}
}
