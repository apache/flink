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

package eu.stratosphere.nephele.taskmanager.network;

import java.io.EOFException;
import java.io.IOException;
import java.net.InetSocketAddress;
import java.nio.channels.ClosedChannelException;
import java.nio.channels.SelectionKey;
import java.nio.channels.Selector;
import java.nio.channels.ServerSocketChannel;
import java.nio.channels.SocketChannel;
import java.util.ArrayDeque;
import java.util.Iterator;
import java.util.Queue;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import eu.stratosphere.nephele.taskmanager.bufferprovider.BufferAvailabilityListener;
import eu.stratosphere.nephele.taskmanager.routing.RoutingLayer;
import eu.stratosphere.nephele.taskmanager.transferenvelope.NoBufferAvailableException;
import eu.stratosphere.nephele.util.StringUtils;

final class IncomingConnectionThread extends Thread {

	private static final Log LOG = LogFactory.getLog(IncomingConnectionThread.class);

	private final RoutingLayer byteBufferedChannelManager;

	private final Selector selector;

	private final Queue<SelectionKey> pendingReadEventSubscribeRequests = new ArrayDeque<SelectionKey>();

	private final ServerSocketChannel listeningSocket;

	private static final class IncomingConnectionBufferAvailListener implements BufferAvailabilityListener {

		private final Queue<SelectionKey> pendingReadEventSubscribeRequests;

		private final SelectionKey key;

		private IncomingConnectionBufferAvailListener(final Queue<SelectionKey> pendingReadEventSubscribeRequests,
				final SelectionKey key) {

			this.pendingReadEventSubscribeRequests = pendingReadEventSubscribeRequests;
			this.key = key;
		}

		/**
		 * {@inheritDoc}
		 */
		@Override
		public void bufferAvailable() {

			synchronized (this.pendingReadEventSubscribeRequests) {
				this.pendingReadEventSubscribeRequests.add(this.key);
			}
		}
	}

	IncomingConnectionThread(RoutingLayer byteBufferedChannelManager,
			boolean isListeningThread, InetSocketAddress listeningAddress) throws IOException {
		super("Incoming Connection Thread");

		this.selector = Selector.open();
		this.byteBufferedChannelManager = byteBufferedChannelManager;

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

			synchronized (this.pendingReadEventSubscribeRequests) {
				while (!this.pendingReadEventSubscribeRequests.isEmpty()) {
					final SelectionKey key = this.pendingReadEventSubscribeRequests.poll();
					final IncomingConnection incomingConnection = (IncomingConnection) key.attachment();
					final SocketChannel socketChannel = (SocketChannel) key.channel();

					try {
						final SelectionKey newKey = socketChannel.register(this.selector, SelectionKey.OP_READ);
						newKey.attach(incomingConnection);
					} catch (ClosedChannelException e) {
						incomingConnection.reportTransmissionProblem(key, e);
					}
				}
			}

			try {
				this.selector.select(500);
			} catch (IOException e) {
				LOG.error(e);
			}

			final Iterator<SelectionKey> iter = this.selector.selectedKeys().iterator();

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

		final IncomingConnection incomingConnection = new IncomingConnection(this.byteBufferedChannelManager,
			clientSocket);
		SelectionKey clientKey = null;
		try {
			clientSocket.configureBlocking(false);
			clientKey = clientSocket.register(this.selector, SelectionKey.OP_READ);
			clientKey.attach(incomingConnection);
		} catch (IOException ioe) {
			incomingConnection.reportTransmissionProblem(clientKey, ioe);
		}
	}

	private void doRead(SelectionKey key) {

		final IncomingConnection incomingConnection = (IncomingConnection) key.attachment();
		try {
			incomingConnection.read();
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
		} catch (InterruptedException e) {
			// Nothing to do here
		} catch (NoBufferAvailableException e) {
			// There are no buffers available, unsubscribe from read event
			final SocketChannel socketChannel = (SocketChannel) key.channel();
			try {
				final SelectionKey newKey = socketChannel.register(this.selector, 0);
				newKey.attach(incomingConnection);
			} catch (ClosedChannelException e1) {
				incomingConnection.reportTransmissionProblem(key, e1);
			}

			final BufferAvailabilityListener bal = new IncomingConnectionBufferAvailListener(
				this.pendingReadEventSubscribeRequests, key);
			if (!e.getBufferProvider().registerBufferAvailabilityListener(bal)) {
				// In the meantime, a buffer has become available again, subscribe to read event again

				try {
					final SelectionKey newKey = socketChannel.register(this.selector, SelectionKey.OP_READ);
					newKey.attach(incomingConnection);
				} catch (ClosedChannelException e1) {
					incomingConnection.reportTransmissionProblem(key, e1);
				}
			}
		}
	}
}
