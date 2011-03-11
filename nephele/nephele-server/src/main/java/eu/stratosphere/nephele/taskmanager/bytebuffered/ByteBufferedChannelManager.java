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
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Queue;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import eu.stratosphere.nephele.instance.InstanceConnectionInfo;
import eu.stratosphere.nephele.io.channels.Buffer;
import eu.stratosphere.nephele.io.channels.ChannelID;
import eu.stratosphere.nephele.io.channels.bytebuffered.AbstractByteBufferedInputChannel;
import eu.stratosphere.nephele.io.channels.bytebuffered.AbstractByteBufferedOutputChannel;
import eu.stratosphere.nephele.jobgraph.JobID;
import eu.stratosphere.nephele.protocols.ChannelLookupProtocol;
import eu.stratosphere.nephele.taskmanager.bufferprovider.BufferProvider;
import eu.stratosphere.nephele.taskmanager.transferenvelope.TransferEnvelope;
import eu.stratosphere.nephele.taskmanager.transferenvelope.TransferEnvelopeDispatcher;
import eu.stratosphere.nephele.taskmanager.transferenvelope.TransferEnvelopeReceiverList;
import eu.stratosphere.nephele.types.Record;

public final class ByteBufferedChannelManager implements TransferEnvelopeDispatcher {

	/**
	 * The log object used to report problems and errors.
	 */
	private static final Log LOG = LogFactory.getLog(ByteBufferedChannelManager.class);

	private final BufferProvider bufferProvider;

	private final Map<ChannelID, ByteBufferedChannelWrapper> registeredChannels = new HashMap<ChannelID, ByteBufferedChannelWrapper>();

	/**
	 * This map caches transfer envelope receiver lists.
	 */
	private final Map<ChannelID, TransferEnvelopeReceiverList> receiverCache = new HashMap<ChannelID, TransferEnvelopeReceiverList>();

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

		/*
		 * synchronized (this.queuedIncomingTransferEnvelopes) {
		 * incomingTransferEnvelopes = this.queuedIncomingTransferEnvelopes.remove(byteBufferedInputChannel.getID());
		 * }
		 */

		if (incomingTransferEnvelopes == null) {
			// No more data buffered for us, we are done...
			return;
		}

		// A dump thread might be working on the same queue, so access must be synchronized
		/*
		 * synchronized (incomingTransferEnvelopes) {
		 * final Iterator<TransferEnvelope> it = incomingTransferEnvelopes.iterator();
		 * while (it.hasNext()) {
		 * final TransferEnvelope envelope = it.next();
		 * if (envelope.getBuffer() != null) {
		 * envelope.getBuffer().recycleBuffer();
		 * }
		 * }
		 * }
		 */
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
		/*
		 * InetSocketAddress connectionAddress = null;
		 * synchronized (this.connectionAddresses) {
		 * connectionAddress = this.connectionAddresses.remove(byteBufferedOutputChannel.getID());
		 * }
		 * if (connectionAddress == null) {
		 * // Apparently, the connected task has not yet transmitted any data, so no buffers can be queued
		 * return;
		 * }
		 */

		// Make sure all queued outgoing buffers are dropped and recycled
		/*
		 * synchronized (this.outgoingConnections) {
		 * final OutgoingConnection outgoingConnection = this.outgoingConnections.get(connectionAddress);
		 * if (outgoingConnection != null) {
		 * outgoingConnection.dropAllQueuedEnvelopesForChannel(byteBufferedOutputChannel.getID(), true);
		 * if (outgoingConnection.canBeRemoved()) {
		 * // reflects no envelopes, no
		 * // currentEnvelope and not connected
		 * this.outgoingConnections.remove(connectionAddress);
		 * }
		 * }
		 * }
		 */
	}

	/*
	 * public void queueIncomingTransferEnvelope(TransferEnvelope transferEnvelope) throws IOException {
	 * final ChannelID targetID = transferEnvelope.getTarget();
	 * ByteBufferedChannelWrapper targetChannelWrapper = null;
	 * if (targetID == null) {
	 * throw new IOException("Cannot process incoming transfer envelope: target channel ID is null!");
	 * }
	 * synchronized (this.registeredChannels) {
	 * targetChannelWrapper = this.registeredChannels.get(targetID);
	 * if (targetChannelWrapper == null) {
	 * // Release buffer immediately
	 * if (transferEnvelope.getBuffer() != null) {
	 * transferEnvelope.getBuffer().recycleBuffer();
	 * }
	 * throw new IOException("Cannot find target channel to ID " + targetID
	 * + " to process incoming transfer envelope");
	 * }
	 * }
	 * if (targetChannelWrapper.isInputChannel()) {
	 * final ByteBufferedInputChannelWrapper networkInputChannelWrapper = (ByteBufferedInputChannelWrapper)
	 * targetChannelWrapper;
	 * networkInputChannelWrapper.queueIncomingTransferEnvelope(transferEnvelope);
	 * } else {
	 * final ByteBufferedOutputChannelWrapper networkOutputChannelWrapper = (ByteBufferedOutputChannelWrapper)
	 * targetChannelWrapper;
	 * // In case of an output channel, we only expect events and no buffers
	 * if (transferEnvelope.getBuffer() != null) {
	 * LOG.error("Incoming transfer envelope for network output channel "
	 * + targetChannelWrapper.getChannelID() + " has a buffer attached");
	 * }
	 * // Process the events immediately
	 * final EventList eventList = transferEnvelope.getEventList();
	 * final Iterator<AbstractEvent> iterator = eventList.iterator();
	 * while (iterator.hasNext()) {
	 * networkOutputChannelWrapper.processEvent(iterator.next());
	 * }
	 * }
	 * }
	 */

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

	private boolean processEnvelope(final TransferEnvelope transferEnvelope) throws IOException, InterruptedException {

		final TransferEnvelopeReceiverList receiverList = getReceiverList(transferEnvelope.getJobID(),
			transferEnvelope.getSource());

		if (receiverList == null) {
			throw new IOException("Transfer envelope " + transferEnvelope.getSequenceNumber() + " from source channel "
				+ transferEnvelope.getSource() + " has not have a receiver list");
		}

		if (receiverList.getTotalNumberOfReceivers() == 0) {
			throw new IOException("Total number of receivers for envelope " + transferEnvelope.getSequenceNumber()
				+ " from source channel " + transferEnvelope.getSource() + " is 0");
		}

		// This envelope is known to have either no buffer or an memory-based input buffer
		if (transferEnvelope.getBuffer() == null) {
			return processEnvelopeEnvelopeWithoutBuffer(transferEnvelope, receiverList);
		} else {
			return processEnvelopeWithBuffer(transferEnvelope, receiverList);
		}
	}

	private boolean processEnvelopeWithBuffer(final TransferEnvelope transferEnvelope,
			final TransferEnvelopeReceiverList receiverList) throws IOException, InterruptedException {

		final Buffer buffer = transferEnvelope.getBuffer();
		if (buffer.isReadBuffer()) {

			if (receiverList.hasRemoteReceivers()) {

				final List<InetSocketAddress> remoteReceivers = receiverList.getRemoteReceivers();

				// Create remote envelope
				final Buffer writeBuffer = this.bufferProvider.requestEmptyWriteBuffer(buffer.size());
				if (writeBuffer == null) {
					return false;
				}

				// TODO: Copy buffer content

				final TransferEnvelope remoteEnvelope = new TransferEnvelope(transferEnvelope.getSequenceNumber(),
					transferEnvelope.getJobID(), transferEnvelope.getSource(), transferEnvelope.getEventList());

				remoteEnvelope.setBuffer(writeBuffer);

				for (int i = 1; i < remoteReceivers.size(); ++i) {

					this.networkConnectionManager.queueEnvelopeForTransfer(remoteReceivers.get(i),
						remoteEnvelope.duplicate());
				}

				this.networkConnectionManager.queueEnvelopeForTransfer(remoteReceivers.get(0), remoteEnvelope);
			}

			if (receiverList.hasLocalReceivers()) {

				final List<ChannelID> localReceivers = receiverList.getLocalReceivers();

				synchronized (this.registeredChannels) {

					ByteBufferedChannelWrapper channelWrapper = null;
					for (int i = 1; i < localReceivers.size(); ++i) {

						channelWrapper = this.registeredChannels.get(localReceivers.get(i));
						if (channelWrapper == null) {
							LOG.error("Cannot find local receiver " + localReceivers.get(i) + " for job "
								+ transferEnvelope.getJobID());
							continue;
						}
						channelWrapper.queueTransferEnvelope(transferEnvelope.duplicate());
					}

					channelWrapper = this.registeredChannels.get(localReceivers.get(0));
					if (channelWrapper == null) {
						LOG.error("Cannot find local receiver " + localReceivers.get(0) + " for job "
							+ transferEnvelope.getJobID());
					} else {
						channelWrapper.queueTransferEnvelope(transferEnvelope);
					}
				}
			} else {
				buffer.recycleBuffer();
			}

		} else {

			if (receiverList.hasLocalReceivers()) {

				final List<ChannelID> localReceivers = receiverList.getLocalReceivers();

				final Buffer readBuffer = this.bufferProvider.requestEmptyReadBufferAndWait(buffer.size(),
					localReceivers.get(0));

				// Copy content of buffer
				transferEnvelope.getBuffer().copyToBuffer(readBuffer);

				final TransferEnvelope localEnvelope = new TransferEnvelope(transferEnvelope.getSequenceNumber(),
					transferEnvelope.getJobID(), transferEnvelope.getSource(), transferEnvelope.getEventList());

				localEnvelope.setBuffer(readBuffer);

				synchronized (this.registeredChannels) {

					ByteBufferedChannelWrapper channelWrapper = null;
					for (int i = 1; i < localReceivers.size(); ++i) {

						channelWrapper = this.registeredChannels.get(localReceivers.get(i));
						if (channelWrapper == null) {
							LOG.error("Cannot find local receiver " + localReceivers.get(i) + " for job "
								+ transferEnvelope.getJobID());
							continue;
						}
						channelWrapper.queueTransferEnvelope(localEnvelope.duplicate());
					}

					channelWrapper = this.registeredChannels.get(localReceivers.get(0));
					if (channelWrapper == null) {
						LOG.error("Cannot find local receiver " + localReceivers.get(0) + " for job "
							+ transferEnvelope.getJobID());
					} else {

						channelWrapper.queueTransferEnvelope(localEnvelope);
					}
				}
			}

			if (receiverList.hasRemoteReceivers()) {

				final List<InetSocketAddress> remoteReceivers = receiverList.getRemoteReceivers();

				for (int i = 1; i < remoteReceivers.size(); ++i) {

					this.networkConnectionManager.queueEnvelopeForTransfer(remoteReceivers.get(i),
						transferEnvelope.duplicate());
				}

				this.networkConnectionManager.queueEnvelopeForTransfer(remoteReceivers.get(0), transferEnvelope);

			} else {
				buffer.recycleBuffer();
			}

		}

		return true;
	}

	private boolean processEnvelopeEnvelopeWithoutBuffer(final TransferEnvelope transferEnvelope,
			final TransferEnvelopeReceiverList receiverList) {

		System.out.println("Received envelope without buffer with event list size "
			+ transferEnvelope.getEventList().size());

		// No need to copy anything
		final Iterator<ChannelID> localIt = receiverList.getLocalReceivers().iterator();

		while (localIt.hasNext()) {

			final ChannelID localReceiver = localIt.next();
			synchronized (this.registeredChannels) {

				final ByteBufferedChannelWrapper channelWrapper = this.registeredChannels.get(localReceiver);
				if (channelWrapper == null) {
					LOG.error("Cannot find local receiver " + localReceiver + " for job "
						+ transferEnvelope.getJobID());
					continue;
				}
				channelWrapper.queueTransferEnvelope(transferEnvelope);
			}
		}

		final Iterator<InetSocketAddress> remoteIt = receiverList.getRemoteReceivers().iterator();

		while (remoteIt.hasNext()) {

			final InetSocketAddress remoteReceiver = remoteIt.next();
			this.networkConnectionManager.queueEnvelopeForTransfer(remoteReceiver, transferEnvelope);
		}

		return true;
	}

	private TransferEnvelopeReceiverList getReceiverList(final JobID jobID, final ChannelID sourceChannelID)
			throws IOException, InterruptedException {

		synchronized (this.receiverCache) {

			TransferEnvelopeReceiverList receiverList = this.receiverCache.get(sourceChannelID);
			if (receiverList == null) {

				while (true) {

					final ConnectionInfoLookupResponse lookupResponse = this.channelLookupService.lookupConnectionInfo(
							this.localConnectionInfo, jobID, sourceChannelID);

					if (lookupResponse.receiverNotFound()) {
						throw new IOException("Cannot find task(s) waiting for data from source channel with ID "
							+ sourceChannelID);
					}

					if (lookupResponse.receiverNotReady()) {
						Thread.sleep(500);
						continue;
					}

					if (lookupResponse.receiverReady()) {
						receiverList = new TransferEnvelopeReceiverList(lookupResponse);
						break;
					}
				}

				if (receiverList == null) {
					LOG.error("Receiver list is null for source channel ID " + sourceChannelID);
				} else {
					this.receiverCache.put(sourceChannelID, receiverList);

					System.out.println("Receiver list for source channel ID " + sourceChannelID + " at task manager "
						+ this.localConnectionInfo);
					if (receiverList.hasLocalReceivers()) {
						System.out.println("\tLocal receivers:");
						final Iterator<ChannelID> it = receiverList.getLocalReceivers().iterator();
						while (it.hasNext()) {
							System.out.println("\t\t" + it.next());
						}
					}

					if (receiverList.hasRemoteReceivers()) {
						System.out.println("Remote receivers:");
						final Iterator<InetSocketAddress> it = receiverList.getRemoteReceivers().iterator();
						while (it.hasNext()) {
							System.out.println("\t\t" + it.next());
						}
					}
				}
			}

			return receiverList;
		}
	}

	/**
	 * {@inheritDoc}
	 */
	@Override
	public void processEnvelopeFromOutputChannel(final TransferEnvelope transferEnvelope) throws IOException,
			InterruptedException {

		processEnvelope(transferEnvelope);
	}

	/**
	 * {@inheritDoc}
	 */
	@Override
	public void processEnvelopeFromInputChannel(final TransferEnvelope transferEnvelope) throws IOException,
			InterruptedException {

		processEnvelope(transferEnvelope);
	}

	/**
	 * {@inheritDoc}
	 */
	@Override
	public boolean processEnvelopeFromNetworkOrCheckpoint(final TransferEnvelope transferEnvelope) throws IOException {

		System.out.println("Received envelope from network or checkpoint");

		return false;
	}
}
