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

import java.io.EOFException;
import java.io.IOException;
import java.net.InetSocketAddress;
import java.nio.channels.ReadableByteChannel;
import java.nio.channels.SelectionKey;
import java.nio.channels.Selector;
import java.nio.channels.ServerSocketChannel;
import java.nio.channels.SocketChannel;
import java.util.ArrayDeque;
import java.util.Iterator;
import java.util.Queue;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import eu.stratosphere.nephele.util.StringUtils;

public class IncomingConnectionThread extends Thread {

	private static final Log LOG = LogFactory.getLog(IncomingConnectionThread.class);

	private final NetworkConnectionManager networkConnectionManager;

	private final Selector selector;

	private final ServerSocketChannel listeningSocket;

	private final Queue<IncomingConnection> pendingIncomingConnections = new ArrayDeque<IncomingConnection>();

	public IncomingConnectionThread(NetworkConnectionManager networkConnectionManager, boolean isListeningThread,
			InetSocketAddress listeningAddress)
												throws IOException {
		super("Incoming Connection Thread");

		this.selector = Selector.open();
		this.networkConnectionManager = networkConnectionManager;

		if (isListeningThread) {
			this.listeningSocket = ServerSocketChannel.open();
			this.listeningSocket.configureBlocking(false);
			listeningSocket.register(this.selector, SelectionKey.OP_ACCEPT);
			this.listeningSocket.socket().bind(listeningAddress);
			LOG.debug("Listening on " + this.listeningSocket.socket().getLocalSocketAddress());
		} else {
			this.listeningSocket = null;
		}
	}

	@Override
	public void run() {

		while (!this.isInterrupted()) {

			synchronized (this.pendingIncomingConnections) {

				if (!pendingIncomingConnections.isEmpty()) {
					final IncomingConnection incomingConnection = pendingIncomingConnections.poll();
					final ReadableByteChannel readableByteChannel = incomingConnection.getReadableByteChannel();
					if (readableByteChannel instanceof SocketChannel) {
						final SocketChannel socketChannel = (SocketChannel) readableByteChannel;
						try {
							socketChannel.configureBlocking(false);
							final SelectionKey key = socketChannel.register(this.selector, SelectionKey.OP_READ);
							key.attach(incomingConnection);

							/*
							 * if(Math.random() < 0.1f) {
							 * System.out.println("GENERATING I/O EXCEPTION2");
							 * throw new IOException("Auto generated I/O exception");
							 * }
							 */

						} catch (IOException ioe) {
							incomingConnection.reportTransmissionProblem(null, ioe);
						}
					} else {
						LOG.error("Found pending incoming connection which is not of type SocketChannel buf of type "
							+ readableByteChannel.getClass());
					}
				}
			}

			try {
				selector.select(500);
			} catch (IOException e) {
				LOG.error(e);
			}

			final Iterator<SelectionKey> iter = selector.selectedKeys().iterator();

			while (iter.hasNext()) {
				final SelectionKey key = iter.next();

				iter.remove();
				if (key.isValid()) {
					if (key.isReadable()) {
						doRead(key);
					} else if (key.isAcceptable()) {
						doAccept(key);
					} else {
						LOG.error("Unknown key: " + key);
					}
				} else {
					LOG.error("Received invalid key: " + key);
				}
			}
		}

		// Do cleanup, if necessary
		if (this.listeningSocket != null) {
			try {
				this.listeningSocket.close();
			} catch (IOException ioe) {
				// Actually, we can ignore this exception
				LOG.debug(ioe);
			}
		}

		// Finally, close the selector
		try {
			this.selector.close();
		} catch (IOException ioe) {
			LOG.debug(StringUtils.stringifyException(ioe));
		}
	}

	private void doAccept(SelectionKey key) {

		SocketChannel clientSocket = null;

		try {
			clientSocket = this.listeningSocket.accept();
			if (clientSocket == null) {
				LOG.error("Client socket is null");
				return;
			}
		} catch (IOException ioe) {
			LOG.error(ioe);
			return;
		}

		// Register the new incoming connection with the byte buffered channel manager
		final InetSocketAddress remoteAddress = (InetSocketAddress) clientSocket.socket().getRemoteSocketAddress();
		final IncomingConnectionID incomingConnectionID = new IncomingConnectionID(remoteAddress.getAddress());
		this.networkConnectionManager.registerIncomingConnection(incomingConnectionID, clientSocket);

	}

	private void doRead(SelectionKey key) {

		final IncomingConnection incomingConnection = (IncomingConnection) key.attachment();
		try {
			incomingConnection.read();

			/*
			 * if(Math.random() < 0.005f) {
			 * System.out.println("GENERATING I/O EXCEPTION3");
			 * throw new IOException("Auto generated I/O exception");
			 * }
			 */

		} catch (EOFException eof) {
			if (incomingConnection.isCloseUnexpected()) {
				final SocketChannel socketChannel = (SocketChannel) key.channel();
				LOG.error("Connection from " + socketChannel.socket().getRemoteSocketAddress()
					+ " was closed unexpectedly");
				incomingConnection.reportTransmissionProblem(key, eof);
			} else {
				incomingConnection.closeConnection(key);
			}
		} catch (IOException ioe) {
			incomingConnection.reportTransmissionProblem(key, ioe);
		}
	}

	public void addToPendingIncomingConnections(IncomingConnection incomingConnection) {

		synchronized (this.pendingIncomingConnections) {
			this.pendingIncomingConnections.add(incomingConnection);
		}
	}
}
