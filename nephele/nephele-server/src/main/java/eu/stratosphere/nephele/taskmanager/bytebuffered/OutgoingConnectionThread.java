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

public class OutgoingConnectionThread extends Thread {

	/**
	 * The minimum time a TCP connection must be idle it is closed.
	 */
	private static final long MIN_IDLE_TIME_BEFORE_CLOSE = 300000L; // 300 seconds

	private static final Log LOG = LogFactory.getLog(OutgoingConnectionThread.class);

	private final Selector selector;

	private final Queue<OutgoingConnection> pendingConnectionRequests = new ArrayDeque<OutgoingConnection>();

	private final Queue<SelectionKey> pendingWriteEventSubscribeRequests = new ArrayDeque<SelectionKey>();

	private final Map<OutgoingConnection, Long> connectionsToClose = new HashMap<OutgoingConnection, Long>();

	private final Object synchronizationObject = new Object();

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

			synchronized (this.synchronizationObject) {

				if (!this.pendingConnectionRequests.isEmpty()) {
					final OutgoingConnection outgoingConnection = this.pendingConnectionRequests.poll();
					try {
						final SocketChannel socketChannel = SocketChannel.open();
						socketChannel.configureBlocking(false);
						final SelectionKey key = socketChannel.register(this.selector, SelectionKey.OP_CONNECT);
						socketChannel.connect(outgoingConnection.getConnectionAddress());
						key.attach(outgoingConnection);
					} catch (IOException ioe) {
						outgoingConnection.reportConnectionProblem(ioe);
					}
				}

				if (!this.pendingWriteEventSubscribeRequests.isEmpty()) {
					final SelectionKey oldSelectionKey = this.pendingWriteEventSubscribeRequests.poll();
					final OutgoingConnection outgoingConnection = (OutgoingConnection) oldSelectionKey.attachment();
					final SocketChannel socketChannel = (SocketChannel) oldSelectionKey.channel();

					try {
						final SelectionKey newSelectionKey = socketChannel.register(this.selector, SelectionKey.OP_READ
							| SelectionKey.OP_WRITE);
						newSelectionKey.attach(outgoingConnection);
						outgoingConnection.setSelectionKey(newSelectionKey);
					} catch (IOException ioe) {
						outgoingConnection.reportTransmissionProblem(ioe);
					}
				}

				final Iterator<Map.Entry<OutgoingConnection, Long>> closeIt = this.connectionsToClose.entrySet()
					.iterator();
				final long now = System.currentTimeMillis();
				while (closeIt.hasNext()) {

					final Map.Entry<OutgoingConnection, Long> entry = closeIt.next();
					if ((entry.getValue().longValue() + MIN_IDLE_TIME_BEFORE_CLOSE) < now) {
						final OutgoingConnection outgoingConnection = entry.getKey();
						closeIt.remove();
						try {
							outgoingConnection.closeConnection();
						} catch (IOException ioe) {
							outgoingConnection.reportTransmissionProblem(ioe);
						}
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

		synchronized (this.synchronizationObject) {
			this.pendingConnectionRequests.add(outgoingConnection);
		}
	}

	public void unsubscribeFromWriteEvent(SelectionKey selectionKey) throws IOException {

		final SocketChannel socketChannel = (SocketChannel) selectionKey.channel();
		final OutgoingConnection outgoingConnection = (OutgoingConnection) selectionKey.attachment();

		final SelectionKey newSelectionKey = socketChannel.register(this.selector, SelectionKey.OP_READ);
		newSelectionKey.attach(outgoingConnection);
		outgoingConnection.setSelectionKey(newSelectionKey);

		synchronized (this.synchronizationObject) {
			this.connectionsToClose.put(outgoingConnection, Long.valueOf(System.currentTimeMillis()));
		}
	}

	public void subscribeToWriteEvent(SelectionKey selectionKey) {

		synchronized (this.synchronizationObject) {
			this.pendingWriteEventSubscribeRequests.add(selectionKey);
			this.connectionsToClose.remove((OutgoingConnection) selectionKey.attachment());
		}

	}
}
