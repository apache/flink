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
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

import eu.stratosphere.nephele.configuration.Configuration;
import eu.stratosphere.nephele.configuration.GlobalConfiguration;
import eu.stratosphere.nephele.taskmanager.transferenvelope.TransferEnvelope;

/**
 * The network connection manager manages incoming and outgoing network connection from and to other hosts.
 * <p>
 * This class is thread-safe.
 * 
 * @author warneke
 */
public final class NetworkConnectionManager {

	/**
	 * The default number of threads dealing with outgoing connections.
	 */
	private static final int DEFAULT_NUMBER_OF_OUTGOING_CONNECTION_THREADS = 1;

	/**
	 * The default number of connection retries before giving up.
	 */
	private static final int DEFAULT_NUMBER_OF_CONNECTION_RETRIES = 10;

	/**
	 * List of active threads dealing with outgoing connections.
	 */
	private final List<OutgoingConnectionThread> outgoingConnectionThreads = new ArrayList<OutgoingConnectionThread>();

	/**
	 * Thread dealing with incoming connections.
	 */
	private final IncomingConnectionThread incomingConnectionThread;

	/**
	 * Map containing currently active outgoing connections.
	 */
	private final Map<InetSocketAddress, OutgoingConnection> outgoingConnections = new HashMap<InetSocketAddress, OutgoingConnection>();

	/**
	 * The number of connection retries before giving up.
	 */
	private final int numberOfConnectionRetries;

	/**
	 * A buffer provider for read buffers
	 */
	private final ByteBufferedChannelManager byteBufferedChannelManager;

	public NetworkConnectionManager(final ByteBufferedChannelManager byteBufferedChannelManager,
			final InetAddress bindAddress, final int dataPort) throws IOException {

		final Configuration configuration = GlobalConfiguration.getConfiguration();

		this.byteBufferedChannelManager = byteBufferedChannelManager;

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

		this.incomingConnectionThread = new IncomingConnectionThread(
			this.byteBufferedChannelManager, true, new InetSocketAddress(bindAddress, dataPort));
		this.incomingConnectionThread.start();

		this.numberOfConnectionRetries = configuration.getInteger("channel.network.numberOfConnectionRetries",
			DEFAULT_NUMBER_OF_CONNECTION_RETRIES);
	}

	/**
	 * Randomly selects one of the active threads dealing with outgoing connections.
	 * 
	 * @return one of the active threads dealing with outgoing connections
	 */
	private OutgoingConnectionThread getOutgoingConnectionThread() {

		synchronized (this.outgoingConnectionThreads) {
			return this.outgoingConnectionThreads.get((int) (this.outgoingConnectionThreads.size() * Math.random()));
		}
	}

	/**
	 * Queues an envelope for transfer to a particular target host.
	 * 
	 * @param targetAddress
	 *        the address of the target host.
	 * @param transferEnvelope
	 *        the envelope to be transfered
	 */
	public void queueEnvelopeForTransfer(final InetSocketAddress targetAddress, final TransferEnvelope transferEnvelope) {

		getOutgoingConnection(targetAddress).queueEnvelope(transferEnvelope);
	}

	/**
	 * Returns (and possibly creates) the outgoing connection for the given target address.
	 * 
	 * @param targetAddress
	 *        the address of the connection target
	 * @return the outgoing connection object
	 */
	private OutgoingConnection getOutgoingConnection(final InetSocketAddress connectionAddress) {

		synchronized (this.outgoingConnections) {

			OutgoingConnection outgoingConnection = this.outgoingConnections.get(connectionAddress);
			if (outgoingConnection == null) {
				outgoingConnection = new OutgoingConnection(connectionAddress, getOutgoingConnectionThread(),
					this.numberOfConnectionRetries);
				this.outgoingConnections.put(connectionAddress, outgoingConnection);
			}

			return outgoingConnection;
		}
	}

	public void shutDown() {

		// Interrupt the threads we started
		this.incomingConnectionThread.interrupt();

		synchronized (this.outgoingConnectionThreads) {
			final Iterator<OutgoingConnectionThread> it = this.outgoingConnectionThreads.iterator();
			while (it.hasNext()) {
				it.next().interrupt();
			}
		}
	}

	public void logBufferUtilization() {
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
	}
}
