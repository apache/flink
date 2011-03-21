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
import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.nio.ByteBuffer;
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
import eu.stratosphere.nephele.types.Record;

public class ByteBufferedChannelManager {

	private static final Log LOG = LogFactory.getLog(ByteBufferedChannelManager.class);

	private static final int DEFAULT_NUMBER_OF_READ_BUFFERS = 128;

	private static final int DEFAULT_NUMBER_OF_WRITE_BUFFERS = 128;

	private static final int DEFAULT_BUFFER_SIZE_IN_BYTES = 128 * 1024; // 128k

	private static final boolean DEFAULT_ALLOW_SPILLING = true;

	private static final int DEFAULT_NUMBER_OF_OUTGOING_CONNECTION_THREADS = 1;

	private static final int DEFAULT_NUMBER_OF_INCOMING_CONNECTION_THREADS = 1;

	private static final int DEFAULT_NUMBER_OF_CONNECTION_RETRIES = 10;

	private final Deque<ByteBuffer> emptyReadBuffers = new ArrayDeque<ByteBuffer>();

	private final Deque<ByteBuffer> emptyWriteBuffers = new ArrayDeque<ByteBuffer>();

	private final Map<ChannelID, Queue<TransferEnvelope>> queuedIncomingTransferEnvelopes = new HashMap<ChannelID, Queue<TransferEnvelope>>();

	private final Map<ChannelID, ByteBufferedChannelWrapper> registeredChannels = new HashMap<ChannelID, ByteBufferedChannelWrapper>();

	private final Map<ChannelID, InetSocketAddress> connectionAddresses = new HashMap<ChannelID, InetSocketAddress>();

	private final Map<InetSocketAddress, OutgoingConnection> outgoingConnections = new HashMap<InetSocketAddress, OutgoingConnection>();

	private final Set<OutOfByteBuffersListener> registeredOutOfWriteBuffersListeners = new HashSet<OutOfByteBuffersListener>();

	private final FileBufferManager fileBufferManager;

	private final List<OutgoingConnectionThread> outgoingConnectionThreads = new ArrayList<OutgoingConnectionThread>();

	private final List<IncomingConnectionThread> incomingConnectionThreads = new ArrayList<IncomingConnectionThread>();

	private final int bufferSizeInBytes;

	private final boolean isSpillingAllowed;

	private final ChannelLookupProtocol channelLookupService;

	private final int numberOfConnectionRetries;

	private final int numberOfReadBuffers;

	private final int numberOfWriteBuffers;

	private int flushThreshold = 0;

	public ByteBufferedChannelManager(ChannelLookupProtocol channelLookupService, InetAddress incomingDataAddress,
			int incomingDataPort, String tmpDir)
												throws IOException {

		final Configuration configuration = GlobalConfiguration.getConfiguration();

		this.fileBufferManager = new FileBufferManager(tmpDir);

		this.numberOfReadBuffers = configuration.getInteger("channel.network.numberOfReadBuffers",
			DEFAULT_NUMBER_OF_READ_BUFFERS);
		this.numberOfWriteBuffers = configuration.getInteger("channel.network.numberOfWriteBuffers",
			DEFAULT_NUMBER_OF_WRITE_BUFFERS);
		this.bufferSizeInBytes = configuration.getInteger("channel.network.bufferSizeInBytes",
			DEFAULT_BUFFER_SIZE_IN_BYTES);

		this.channelLookupService = channelLookupService;

		// Start the connection threads
		final int numberOfOutgoingConnectionThreads = configuration.getInteger(
			"channel.network.numberOfOutgoingConnectionThreads", DEFAULT_NUMBER_OF_OUTGOING_CONNECTION_THREADS);
		synchronized (this.outgoingConnectionThreads) {
			for (int i = 0; i < numberOfOutgoingConnectionThreads; i++) {
				final OutgoingConnectionThread outgoingConnectionThread = new OutgoingConnectionThread();
				outgoingConnectionThread.start();
				this.outgoingConnectionThreads.add(outgoingConnectionThread);
			}
		}

		final int numberOfIncomingConnectionThreads = configuration.getInteger(
			"channel.network.numgerOfIncomingConnectionThreads", DEFAULT_NUMBER_OF_INCOMING_CONNECTION_THREADS);
		synchronized (this.incomingConnectionThreads) {
			for (int i = 0; i < numberOfIncomingConnectionThreads; i++) {
				final IncomingConnectionThread incomingConnectionThread = new IncomingConnectionThread(this, (i == 0),
					new InetSocketAddress(incomingDataAddress, incomingDataPort));
				incomingConnectionThread.start();
				this.incomingConnectionThreads.add(incomingConnectionThread);
			}
		}

		this.numberOfConnectionRetries = configuration.getInteger("channel.network.numberOfConnectionRetries",
			DEFAULT_NUMBER_OF_CONNECTION_RETRIES);
		this.isSpillingAllowed = configuration.getBoolean("channel.network.allowSpilling", DEFAULT_ALLOW_SPILLING);

		LOG.info("Starting NetworkChannelManager with Spilling "
			+ (this.isSpillingAllowed ? "activated" : "deactivated"));

		// Initialize buffers
		for (int i = 0; i < numberOfReadBuffers; i++) {
			final ByteBuffer readBuffer = ByteBuffer.allocateDirect(bufferSizeInBytes);
			this.emptyReadBuffers.add(readBuffer);
		}

		for (int i = 0; i < numberOfWriteBuffers; i++) {
			final ByteBuffer writeBuffer = ByteBuffer.allocateDirect(bufferSizeInBytes);
			this.emptyWriteBuffers.add(writeBuffer);
		}
	}

	/**
	 * Registers a new {@link OutOfByteBuffersListener} object that is notified
	 * when the byte buffered channel manager runs out of write buffers.
	 * 
	 * @param listener
	 *        the listener object to register
	 */
	public void registerOutOfWriterBuffersListener(OutOfByteBuffersListener listener) {

		synchronized (this.registeredOutOfWriteBuffersListeners) {
			this.registeredOutOfWriteBuffersListeners.add(listener);
		}

	}

	/**
	 * Unregisters the given {@link OutOfByteBuffersListener} object so it does no
	 * longer receive notifications about a lack of write buffers.
	 * 
	 * @param listener
	 *        the listener object to unregister
	 */
	public void unregisterOutOfWriterBuffersLister(OutOfByteBuffersListener listener) {

		synchronized (this.registeredOutOfWriteBuffersListeners) {
			this.registeredOutOfWriteBuffersListeners.remove(listener);
		}
	}

	BufferPairResponse requestEmptyWriteBuffers(WriteBufferRequestor requestor, BufferPairRequest bufferPairRequest)
			throws InterruptedException {

		synchronized (this.emptyWriteBuffers) {

			while (this.emptyWriteBuffers.size() < bufferPairRequest.getNumberOfRequestedByteBuffers()) {

				requestor.outOfWriteBuffers();

				/*
				 * synchronized(this.registeredOutOfWriteBuffersListeners) {
				 * if(!this.registeredOutOfWriteBuffersListeners.isEmpty()) {
				 * final Iterator<OutOfByteBuffersListener> it = this.registeredOutOfWriteBuffersListeners.iterator();
				 * while(it.hasNext()) {
				 * it.next().outOfByteBuffers();
				 * }
				 * }
				 * }
				 */

				this.emptyWriteBuffers.wait();
			}

			Buffer compressedDataBuffer = null;
			Buffer uncompressedDataBuffer = null;
			if (bufferPairRequest.getCompressedDataBufferSize() >= 0) {
				compressedDataBuffer = BufferFactory.createFromMemory(bufferPairRequest.getCompressedDataBufferSize(),
					this.emptyWriteBuffers.poll(), this.emptyWriteBuffers);
			}

			if (bufferPairRequest.getUncompressedDataBufferSize() >= 0) {
				uncompressedDataBuffer = BufferFactory.createFromMemory(bufferPairRequest
					.getUncompressedDataBufferSize(), this.emptyWriteBuffers.poll(), this.emptyWriteBuffers);
			}
			return new BufferPairResponse(compressedDataBuffer, uncompressedDataBuffer);
		}
	}

	BufferPairResponse requestEmptyReadBuffers(BufferPairRequest bufferPairRequest) throws InterruptedException {

		synchronized (this.emptyReadBuffers) {

			while (this.emptyReadBuffers.size() < bufferPairRequest.getNumberOfRequestedByteBuffers()) {
				this.emptyReadBuffers.wait();
			}

			Buffer compressedDataBuffer = null;
			Buffer uncompressedDataBuffer = null;
			if (bufferPairRequest.getCompressedDataBufferSize() >= 0) {
				compressedDataBuffer = BufferFactory.createFromMemory(bufferPairRequest.getCompressedDataBufferSize(),
					this.emptyReadBuffers.poll(), this.emptyReadBuffers);
			}

			if (bufferPairRequest.getUncompressedDataBufferSize() >= 0) {
				uncompressedDataBuffer = BufferFactory.createFromMemory(bufferPairRequest
					.getUncompressedDataBufferSize(), this.emptyReadBuffers.poll(), this.emptyReadBuffers);
			}
			return new BufferPairResponse(compressedDataBuffer, uncompressedDataBuffer);
		}
	}

	public Buffer requestEmptyReadBuffer(int minimumSizeOfBuffer, ChannelID sourceChannelID) throws IOException {

		if (minimumSizeOfBuffer > this.bufferSizeInBytes) {
			throw new IOException("Requested buffer size is " + minimumSizeOfBuffer + ", system can offer at most "
				+ this.bufferSizeInBytes);
		}

		synchronized (this.emptyReadBuffers) {

			if ((this.emptyReadBuffers.size() - 2) > 0) { // -2 because there must be at least one buffer left if
				// compression is enabled
				return BufferFactory.createFromMemory(minimumSizeOfBuffer, this.emptyReadBuffers.poll(),
					this.emptyReadBuffers);
			}

			if (this.isSpillingAllowed) {
				return BufferFactory.createFromFile(minimumSizeOfBuffer, sourceChannelID, this.fileBufferManager);
			}

			try {
				this.emptyReadBuffers.wait(100); // Wait for 100 milliseconds, so the NIO thread won't do busy
				// waiting...
			} catch (InterruptedException e) {
				LOG.error(e);
			}
		}

		return null;
	}

	public void registerByteBufferedInputChannel(
			AbstractByteBufferedInputChannel<? extends Record> byteBufferedInputChannel) {

		LOG.debug("Registering byte buffered input channel " + byteBufferedInputChannel.getID());

		synchronized (this.registeredChannels) {

			if (this.registeredChannels.containsKey(byteBufferedInputChannel.getID())) {
				LOG.error("Byte buffered input channel " + byteBufferedInputChannel.getID() + " is already registered");
				return;
			}

			final ByteBufferedInputChannelWrapper networkInputChannelWrapper = new ByteBufferedInputChannelWrapper(
				byteBufferedInputChannel, this);
			this.registeredChannels.put(byteBufferedInputChannel.getID(), networkInputChannelWrapper);
		}
	}

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
			registerOutOfWriterBuffersListener(channelWrapper);

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

		synchronized (this.queuedIncomingTransferEnvelopes) {

			incomingTransferEnvelopes = this.queuedIncomingTransferEnvelopes.remove(byteBufferedInputChannel.getID());
		}

		if (incomingTransferEnvelopes == null) {
			// No more data buffered for us, we are done...
			return;
		}

		// A dump thread might be working on the same queue, so access must be synchronized
		synchronized (incomingTransferEnvelopes) {

			final Iterator<TransferEnvelope> it = incomingTransferEnvelopes.iterator();
			while (it.hasNext()) {
				final TransferEnvelope envelope = it.next();
				if (envelope.getBuffer() != null) {
					envelope.getBuffer().recycleBuffer();
				}
			}
		}
	}

	/*
	 * void recycleEmptyWriteBuffer(ByteBuffer buffer) {
	 * buffer.clear();
	 * synchronized(this.emptyWriteBuffers) {
	 * this.emptyWriteBuffers.add(buffer);
	 * this.emptyWriteBuffers.notify();
	 * }
	 * }
	 */

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
		unregisterOutOfWriterBuffersLister(channelWrapper);

		// Make sure all output buffers are leased and recycled
		InetSocketAddress connectionAddress = null;
		synchronized (this.connectionAddresses) {
			connectionAddress = this.connectionAddresses.remove(byteBufferedOutputChannel.getID());
		}
		if (connectionAddress == null) {
			// Apparently, the connected task has not yet transmitted any data, so no buffers can be queued
			return;
		}

		// Make sure all queued outgoing buffers are dropped and recycled
		synchronized (this.outgoingConnections) {
			final OutgoingConnection outgoingConnection = this.outgoingConnections.get(connectionAddress);
			if (outgoingConnection != null) {
				outgoingConnection.dropAllQueuedEnvelopesForChannel(byteBufferedOutputChannel.getID(), true);
				if (outgoingConnection.canBeRemoved()) {
					// reflects no envelopes, no
					// currentEnvelope and not connected
					this.outgoingConnections.remove(connectionAddress);
				}
			}
		}
	}

	private OutgoingConnectionThread getOutgoingConnectionThread() {

		synchronized (this.outgoingConnectionThreads) {
			return this.outgoingConnectionThreads.get((int) (this.outgoingConnectionThreads.size() * Math.random()));
		}
	}

	void queueOutgoingTransferEnvelope(TransferEnvelope transferEnvelope) throws InterruptedException, IOException {

		// Check to which host the transfer envelope shall be sent
		InetSocketAddress connectionAddress = getPeerConnectionAddress(transferEnvelope.getSource());
		if (connectionAddress == null) {
			LOG.fatal("Cannot resolve channel ID to a connection address: " + transferEnvelope.getTarget());
			return;
		}

		// Check if there is already an existing connection to that address
		OutgoingConnection outgoingConnection = null;
		synchronized (this.outgoingConnections) {
			outgoingConnection = this.outgoingConnections.get(connectionAddress);
			if (outgoingConnection == null) {
				outgoingConnection = createOutgoingConnection(connectionAddress);
			}

			this.outgoingConnections.put(connectionAddress, outgoingConnection);
		}

		outgoingConnection.queueEnvelope(transferEnvelope);
	}

	public void queueIncomingTransferEnvelope(TransferEnvelope transferEnvelope) throws IOException,
			InterruptedException {

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

	private OutgoingConnection createOutgoingConnection(InetSocketAddress connectionAddress) {

		OutgoingConnection connection = new OutgoingConnection(this, connectionAddress, getOutgoingConnectionThread(),
			this.numberOfConnectionRetries);

		return connection;
	}

	private InetSocketAddress getPeerConnectionAddress(ChannelID sourceChannelID) throws InterruptedException,
			IOException {

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

			while (!Thread.interrupted()) {

				final ConnectionInfoLookupResponse lookupResponse = this.channelLookupService.lookupConnectionInfo(
					channelWrapper.getJobID(), channelWrapper.getConnectedChannelID());

				if (lookupResponse.receiverNotFound()) {
					throw new IOException("Task with channel ID " + channelWrapper.getConnectedChannelID()
						+ " does not appear to be running");
				}

				if (lookupResponse.receiverNotReady()) {
					Thread.sleep(500);
					continue;
				}

				if (lookupResponse.receiverReady()) {
					ici = lookupResponse.getInstanceConnectionInfo();
					break;
				}
			}

			if (ici != null) {
				connectionAddress = new InetSocketAddress(ici.getAddress(), ici.getDataPort());
				synchronized (this.connectionAddresses) {
					this.connectionAddresses.put(sourceChannelID, connectionAddress);
				}
			}
		}

		return connectionAddress;
	}

	public int getNumberOfQueuedOutgoingEnvelopes(ChannelID sourceChannelID) throws IOException, InterruptedException {

		final InetSocketAddress socketAddress = getPeerConnectionAddress(sourceChannelID);
		if (socketAddress == null) {
			return 0;
		}

		synchronized (this.outgoingConnections) {

			final OutgoingConnection outgoingConnection = this.outgoingConnections.get(socketAddress);
			if (outgoingConnection == null) {
				return 0;
			}

			return outgoingConnection.getNumberOfQueuedEnvelopesForChannel(sourceChannelID, true);
		}
	}

	/**
	 * Shuts down the network channel manager and
	 * stops all its internal processes.
	 */
	public void shutdown() {

		LOG.info("Shutting down network channel manager");

		// Interrupt the threads we started
		synchronized (this.incomingConnectionThreads) {
			final Iterator<IncomingConnectionThread> it = this.incomingConnectionThreads.iterator();
			while (it.hasNext()) {
				it.next().interrupt();
			}
		}

		synchronized (this.outgoingConnectionThreads) {
			final Iterator<OutgoingConnectionThread> it = this.outgoingConnectionThreads.iterator();
			while (it.hasNext()) {
				it.next().interrupt();
			}
		}

		// Finally, do some consistency tests
		synchronized (this.emptyReadBuffers) {
			if (this.emptyReadBuffers.size() != this.numberOfReadBuffers) {
				LOG.error("Missing " + (this.numberOfReadBuffers - this.emptyReadBuffers.size())
					+ " read buffers during shutdown");
			}
		}

		synchronized (this.emptyWriteBuffers) {
			if (this.emptyWriteBuffers.size() != this.numberOfWriteBuffers) {
				LOG.error("Missing " + (this.numberOfWriteBuffers - this.emptyWriteBuffers.size())
					+ " write buffers during shutdown");
			}
		}
	}

	/**
	 * This method identifies the network channel with the longest
	 * queue of read buffers to process and attempts to dump
	 * occupied memory read buffers to disk.
	 */
	public void dumpMemoryReadBuffersToDisk() {

		Queue<TransferEnvelope> longestQueue = null;
		int longestQueueSize = 0;

		synchronized (this.queuedIncomingTransferEnvelopes) {

			final Iterator<ChannelID> it = this.queuedIncomingTransferEnvelopes.keySet().iterator();
			while (it.hasNext()) {

				final ChannelID channelID = it.next();

				final Queue<TransferEnvelope> queue = this.queuedIncomingTransferEnvelopes.get(channelID);
				synchronized (queue) {
					if (longestQueue == null) {
						longestQueue = queue;
						longestQueueSize = queue.size();
					} else {
						if (queue.size() > longestQueueSize) {
							longestQueue = queue;
						}
					}
				}
			}

			// Launch new dump thread
			if (longestQueue != null) {
				// System.out.println("Longest queue has " + longestQueue.size() + " elements");
				// final ReadBufferDumpThread dumpThread = new ReadBufferDumpThread(longestQueue,
				// this.emptyReadBuffers);
				// dumpThread.start();
			}
		}
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

		if (channelWrapper.isInputChannel())

			channelWrapper.reportIOException(ioe);
	}

	public int getMaximumBufferSize() {
		return this.bufferSizeInBytes;
	}

	public FileBufferManager getFileBufferManager() {
		return this.fileBufferManager;
	}

	/**
	 * Triggers the byte buffer channel manager write the current utilization of its read and write buffers to the logs.
	 * This method is primarily for debugging purposes.
	 */
	public void logBufferUtilization() {

		System.out.println("Buffer utilization for at " + System.currentTimeMillis());
		synchronized (this.emptyWriteBuffers) {
			System.out.println("\tEmpty write buffers: " + this.emptyWriteBuffers.size());
		}
		synchronized (this.emptyReadBuffers) {
			System.out.println("\tEmpty read buffers: " + this.emptyReadBuffers.size());
		}
		synchronized (this.outgoingConnections) {

			System.out.println("\tOutgoing connections:");

			final Iterator<Map.Entry<InetSocketAddress, OutgoingConnection>> it = this.outgoingConnections.entrySet()
				.iterator();

			while (it.hasNext()) {

				final Map.Entry<InetSocketAddress, OutgoingConnection> entry = it.next();
				System.out
					.println("\t\tOC " + entry.getKey() + ": " + entry.getValue().getNumberOfQueuedWriteBuffers());
			}
		}

		synchronized (this.registeredChannels) {

			System.out.println("\tIncoming connections:");

			final Iterator<Map.Entry<ChannelID, ByteBufferedChannelWrapper>> it = this.registeredChannels.entrySet()
				.iterator();

			while (it.hasNext()) {

				final Map.Entry<ChannelID, ByteBufferedChannelWrapper> entry = it.next();
				final ByteBufferedChannelWrapper wrapper = entry.getValue();
				if (wrapper.isInputChannel()) {

					final ByteBufferedInputChannelWrapper inputChannelWrapper = (ByteBufferedInputChannelWrapper) wrapper;
					final int numberOfQueuedEnvelopes = inputChannelWrapper.getNumberOfQueuedEnvelopes();
					final int numberOfQueuedMemoryBuffers = inputChannelWrapper.getNumberOfQueuedMemoryBuffers();

					System.out.println("\t\t" + entry.getKey() + ": " + numberOfQueuedMemoryBuffers + " ("
						+ numberOfQueuedEnvelopes + ")");
				}
			}
		}
	}

	public void setFlushThreshold(final int flushThreshold) {

		synchronized (this.emptyWriteBuffers) {
			this.flushThreshold = flushThreshold;
		}
	}

	public int getFlushThreshold() {

		synchronized (this.emptyWriteBuffers) {
			return this.flushThreshold;
		}
	}
}
