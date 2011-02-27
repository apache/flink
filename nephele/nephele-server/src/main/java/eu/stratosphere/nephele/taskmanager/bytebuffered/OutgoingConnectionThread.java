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
import java.util.Iterator;
import java.util.Queue;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import eu.stratosphere.nephele.util.StringUtils;

public class OutgoingConnectionThread extends Thread {

	private static final Log LOG = LogFactory.getLog(OutgoingConnectionThread.class);

	private final Selector selector;

	private final Queue<OutgoingConnection> pendingOutgoingConnections = new ArrayDeque<OutgoingConnection>();

	public OutgoingConnectionThread()
										throws IOException {
		super("Outgoing Connection Thread");

		this.selector = Selector.open();
	}

	/**
	 * {@inheritDoc}
	 */
	@Override
	public void run() {

		while (!isInterrupted()) {

			synchronized (this.pendingOutgoingConnections) {

				if (!pendingOutgoingConnections.isEmpty()) {
					final OutgoingConnection outgoingConnection = pendingOutgoingConnections.poll();
					try {
						final SocketChannel socketChannel = SocketChannel.open();
						socketChannel.configureBlocking(false);
						final SelectionKey key = socketChannel.register(this.selector, SelectionKey.OP_CONNECT);
						socketChannel.connect(outgoingConnection.getConnectionAddress());
						key.attach(outgoingConnection);
					} catch (IOException ioe) {
						outgoingConnection.reportConnectionProblem(null, null, ioe);
					}
				}
			}

			try {
				selector.select(500);
			} catch (IOException e) {
				e.printStackTrace();
			}

			Iterator<SelectionKey> iter = selector.selectedKeys().iterator();

			while (iter.hasNext()) {
				SelectionKey key = iter.next();

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
					e1.printStackTrace();
				}
			}

			final SelectionKey channelKey = socketChannel.register(selector, SelectionKey.OP_WRITE
				| SelectionKey.OP_READ);
			channelKey.attach(outgoingConnection);

		} catch (IOException ioe) {
			outgoingConnection.reportConnectionProblem(socketChannel, key, ioe);
		}
	}

	private void doWrite(SelectionKey key) {

		final SocketChannel socketChannel = (SocketChannel) key.channel();
		final OutgoingConnection outgoingConnection = (OutgoingConnection) key.attachment();

		try {

			if (!outgoingConnection.write(socketChannel)) {
				// Try to close the connection
//				outgoingConnection.closeConnection(socketChannel, key);
			}

		} catch (IOException ioe) {
			outgoingConnection.reportTransmissionProblem(socketChannel, key, ioe);
		}
	}

	private void doRead(SelectionKey key) {

		final SocketChannel socketChannel = (SocketChannel) key.channel();
		final OutgoingConnection outgoingConnection = (OutgoingConnection) key.attachment();
		final ByteBuffer buf = ByteBuffer.allocate(8);

		try {

			if (socketChannel.read(buf) == -1) {
				outgoingConnection.reportTransmissionProblem(socketChannel, key, new IOException(
					"Read unexpected EOF from channel"));
			} else {
				LOG.error("Outgoing connection read real data from channel");
			}
		} catch (IOException ioe) {
			outgoingConnection.reportTransmissionProblem(socketChannel, key, ioe);
		}
	}

	public void triggerConnect(OutgoingConnection outgoingConnection) {

		synchronized (this.pendingOutgoingConnections) {
			this.pendingOutgoingConnections.add(outgoingConnection);
		}
	}
}
