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

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.SelectionKey;
import java.nio.channels.Selector;
import java.nio.channels.SocketChannel;
import java.util.ArrayDeque;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;
import java.util.Queue;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import eu.stratosphere.nephele.util.StringUtils;

final class OutgoingConnectionThread extends Thread {

	/**
	 * A connection request is a request to establish a TCP connection to a specific host at a certain point in time.
	 * 
	 * @author warneke
	 */
	private static final class ConnectionRequest {

		/**
		 * The outgoing connection representing the local end point of the TCP connection to be established.
		 */
		private final OutgoingConnection outgoingConnection;

		/**
		 * The earliest point in time, stated as milliseconds since January 1st, 1970, at which the connection shall be
		 * established or -1 to establish the connection immediately.
		 */
		private final long earliestConnectionTime;

		private ConnectionRequest(final OutgoingConnection outgoingConnection, final long earliestConnectionTime) {
			this.outgoingConnection = outgoingConnection;
			this.earliestConnectionTime = earliestConnectionTime;
		}
	}

	/**
	 * The minimum time a TCP connection must be idle it is closed.
	 */
	private static final long MIN_IDLE_TIME_BEFORE_CLOSE = 80000L; // 80 seconds

	private static final Log LOG = LogFactory.getLog(OutgoingConnectionThread.class);

	private final Selector selector;

	private final Queue<ConnectionRequest> pendingConnectionRequests = new ArrayDeque<ConnectionRequest>();

	private final Queue<SelectionKey> pendingWriteEventSubscribeRequests = new ArrayDeque<SelectionKey>();

	private final Map<OutgoingConnection, Long> connectionsToClose = new HashMap<OutgoingConnection, Long>();

	public OutgoingConnectionThread() throws IOException {
		super("Outgoing Connection Thread");

		this.selector = Selector.open();
	}

	/**
	 * {@inheritDoc}
	 */
	@Override
	public void run() {

		while (!isInterrupted()) {

			synchronized (this.pendingConnectionRequests) {

				if (!this.pendingConnectionRequests.isEmpty()) {

					final long now = System.currentTimeMillis();
					final ConnectionRequest connectionRequest = this.pendingConnectionRequests.peek();
					final long earliest = connectionRequest.earliestConnectionTime;
					if (earliest == -1L || earliest <= now) {
						this.pendingConnectionRequests.poll();
						final OutgoingConnection outgoingConnection = connectionRequest.outgoingConnection;

						try {
							final SocketChannel socketChannel = SocketChannel.open();
							socketChannel.configureBlocking(false);
							final SelectionKey key = socketChannel.register(this.selector, SelectionKey.OP_CONNECT);
							socketChannel.connect(outgoingConnection.getConnectionAddress());
							key.attach(outgoingConnection);
						} catch (final IOException ioe) {
							ioe.printStackTrace();
							// IOException is reported by separate thread to avoid deadlocks
							final Runnable reporterThread = new Runnable() {

								@Override
								public void run() {
									outgoingConnection.reportConnectionProblem(ioe);
								}
							};
							new Thread(reporterThread).start();
						}
					}
				}
			}

			synchronized (this.pendingWriteEventSubscribeRequests) {

				if (!this.pendingWriteEventSubscribeRequests.isEmpty()) {
					final SelectionKey oldSelectionKey = this.pendingWriteEventSubscribeRequests.poll();
					final OutgoingConnection outgoingConnection = (OutgoingConnection) oldSelectionKey.attachment();
					final SocketChannel socketChannel = (SocketChannel) oldSelectionKey.channel();

					try {
						final SelectionKey newSelectionKey = socketChannel.register(this.selector, SelectionKey.OP_READ
							| SelectionKey.OP_WRITE);
						newSelectionKey.attach(outgoingConnection);
						outgoingConnection.setSelectionKey(newSelectionKey);
					} catch (final IOException ioe) {
						// IOException is reported by separate thread to avoid deadlocks
						final Runnable reporterThread = new Runnable() {

							@Override
							public void run() {
								outgoingConnection.reportTransmissionProblem(ioe);
							}
						};
						new Thread(reporterThread).start();
					}
				}
			}

			synchronized (this.connectionsToClose) {

				final Iterator<Map.Entry<OutgoingConnection, Long>> closeIt = this.connectionsToClose.entrySet()
					.iterator();
				final long now = System.currentTimeMillis();
				while (closeIt.hasNext()) {

					final Map.Entry<OutgoingConnection, Long> entry = closeIt.next();
					if ((entry.getValue().longValue() + MIN_IDLE_TIME_BEFORE_CLOSE) < now) {
						final OutgoingConnection outgoingConnection = entry.getKey();
						closeIt.remove();
						// Create new thread to close connection to avoid deadlocks
						final Runnable closeThread = new Runnable() {

							@Override
							public void run() {
								try {
									outgoingConnection.closeConnection();
								} catch (IOException ioe) {
									outgoingConnection.reportTransmissionProblem(ioe);
								}
							}
						};

						new Thread(closeThread).start();
					}

				}
			}

			try {
				this.selector.select(10);
			} catch (IOException e) {
				LOG.error(e);
			}

			final Iterator<SelectionKey> iter = this.selector.selectedKeys().iterator();

			while (iter.hasNext()) {
				final SelectionKey key = iter.next();

				iter.remove();
				if (key.isValid()) {
					if (key.isConnectable()) {
						doConnect(key);
					} else {
						if (key.isReadable()) {
							doRead(key);
							// A read will always result in an exception, so the write key will not be valid anymore
							continue;
						}
						if (key.isWritable()) {
							doWrite(key);
						}
					}
				} else {
					LOG.error("Received invalid key: " + key);
				}
			}
		}

		// Finally, try to close the selector
		try {
			this.selector.close();
		} catch (IOException ioe) {
			LOG.debug(StringUtils.stringifyException(ioe));
		}
	}

	private void doConnect(SelectionKey key) {

		final OutgoingConnection outgoingConnection = (OutgoingConnection) key.attachment();
		final SocketChannel socketChannel = (SocketChannel) key.channel();
		try {
			while (!socketChannel.finishConnect()) {
				try {
					Thread.sleep(100);
				} catch (InterruptedException e1) {
					LOG.error(e1);
				}
			}

			final SelectionKey channelKey = socketChannel.register(selector, SelectionKey.OP_WRITE
				| SelectionKey.OP_READ);
			outgoingConnection.setSelectionKey(channelKey);
			channelKey.attach(outgoingConnection);

		} catch (IOException ioe) {
			outgoingConnection.reportConnectionProblem(ioe);
		}
	}

	private void doWrite(SelectionKey key) {

		final OutgoingConnection outgoingConnection = (OutgoingConnection) key.attachment();

		try {

			if (!outgoingConnection.write()) {
				// Try to close the connection
				outgoingConnection.requestClose();
			}

		} catch (IOException ioe) {
			outgoingConnection.reportTransmissionProblem(ioe);
		}
	}

	private void doRead(SelectionKey key) {

		final SocketChannel socketChannel = (SocketChannel) key.channel();
		final OutgoingConnection outgoingConnection = (OutgoingConnection) key.attachment();
		final ByteBuffer buf = ByteBuffer.allocate(8);

		try {

			if (socketChannel.read(buf) == -1) {
				outgoingConnection.reportTransmissionProblem(new IOException(
					"Read unexpected EOF from channel"));
			} else {
				LOG.error("Outgoing connection read real data from channel");
			}
		} catch (IOException ioe) {
			outgoingConnection.reportTransmissionProblem(ioe);
		}
	}

	public void triggerConnect(OutgoingConnection outgoingConnection) {

		triggerConnect(outgoingConnection, -1L);
	}

	public void triggerConnect(OutgoingConnection outgoingConnection, final long earliestConnectionTime) {

		final ConnectionRequest connectionRequest = new ConnectionRequest(outgoingConnection, earliestConnectionTime);

		synchronized (this.pendingConnectionRequests) {
			this.pendingConnectionRequests.add(connectionRequest);
		}
	}

	public void unsubscribeFromWriteEvent(SelectionKey selectionKey) throws IOException {

		final SocketChannel socketChannel = (SocketChannel) selectionKey.channel();
		final OutgoingConnection outgoingConnection = (OutgoingConnection) selectionKey.attachment();

		final SelectionKey newSelectionKey = socketChannel.register(this.selector, SelectionKey.OP_READ);
		newSelectionKey.attach(outgoingConnection);
		outgoingConnection.setSelectionKey(newSelectionKey);

		synchronized (this.connectionsToClose) {
			this.connectionsToClose.put(outgoingConnection, Long.valueOf(System.currentTimeMillis()));
		}
	}

	public void subscribeToWriteEvent(SelectionKey selectionKey) {

		synchronized (this.pendingWriteEventSubscribeRequests) {
			this.pendingWriteEventSubscribeRequests.add(selectionKey);
		}
		synchronized (this.connectionsToClose) {
			this.connectionsToClose.remove((OutgoingConnection) selectionKey.attachment());
		}

	}
}
