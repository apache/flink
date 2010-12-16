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
import java.nio.channels.ReadableByteChannel;
import java.nio.channels.SelectionKey;
import java.nio.channels.SocketChannel;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import eu.stratosphere.nephele.util.StringUtils;

public class IncomingConnection {

	private static final Log LOG = LogFactory.getLog(IncomingConnection.class);

	private SocketChannel socketChannel;

	private final TransferEnvelopeDeserializer deserializer;

	private final ByteBufferedChannelManager byteBufferedChannelManager;

	private final boolean readsFromCheckpoint;

	private IncomingConnection previousConnection = null;

	private int sequenceNumberToExpectNext = 0;

	public IncomingConnection(ByteBufferedChannelManager byteBufferedChannelManager, SocketChannel socketChannel,
			boolean readsFromCheckpoint) {
		this.byteBufferedChannelManager = byteBufferedChannelManager;
		this.deserializer = new TransferEnvelopeDeserializer(byteBufferedChannelManager, readsFromCheckpoint);
		this.socketChannel = socketChannel;
		this.readsFromCheckpoint = readsFromCheckpoint;
	}

	public SocketChannel getSockeChannel() {
		return this.socketChannel;
	}

	public void reportTransmissionProblem(SelectionKey key, IOException ioe) {

		// First, write IOException to log
		LOG.error("Connection from " + this.socketChannel.socket().getRemoteSocketAddress()
			+ " encountered an IOException");
		LOG.error(ioe);
		ioe.printStackTrace();

		System.out.println("INCOMING: Error occurred during " + this.sequenceNumberToExpectNext);

		try {
			socketChannel.close();
		} catch (IOException e) {
			LOG.debug("An error occurred while closing the socket");
		}

		// Cancel key
		if (key != null) {
			key.cancel();
		}

		// Recycle read buffer
		if (this.deserializer.getBuffer() != null) {
			this.deserializer.getBuffer().recycleBuffer();
		}

		this.deserializer.reset();
		// Unregister socket connection
		this.byteBufferedChannelManager.unregisterIncomingConnection(this.socketChannel);
		this.socketChannel = null;
	}

	public void read(ReadableByteChannel readableByteChannel) throws IOException, EOFException {

		if (!isActiveConnection()) {
			System.out.println("Is not active connection");
			return;
		}

		this.deserializer.read(readableByteChannel);

		final TransferEnvelope transferEnvelope = this.deserializer.getFullyDeserializedTransferEnvelope();
		if (transferEnvelope != null) {

			if (transferEnvelope.getSequenceNumber() <= this.sequenceNumberToExpectNext) {
				/*
				 * A smaller sequence is fine. It indicates that the sender has generated a new object
				 * for an outgoing connection. However, this only happens if no data is queued to be sent.
				 */
				// this.sequenceNumberToExpectNext = transferEnvelope.getSequenceNumber() + 1;
			} else {
				/**
				 * This is a severe error. Since we do not know which channel is affected by the
				 * data loss, the problem must be propergates to alll network input channels.
				 */
				// IOException ioe = new IOException("Received transfer envelope " +
				// transferEnvelope.getSequenceNumber() + ", but expected " + this.sequenceNumberToExpectNext);
				// networkChannelManager.reportIOExceptionForAllInputChannels(ioe);
				// throw ioe;
			}

			this.byteBufferedChannelManager.queueIncomingTransferEnvelope(transferEnvelope);
		}

	}

	public boolean isCloseUnexpected() {

		return this.deserializer.hasUnfinishedData();
	}

	public void setPreviousConnection(IncomingConnection previousConnection) {
		this.previousConnection = previousConnection;
	}

	public boolean isActiveConnection() {

		if (this.readsFromCheckpoint) {
			return true;
		}

		if (this.socketChannel == null) {
			return false;
		}

		// Channel is connected, if there is no previous connection, this is the active connection
		if (this.previousConnection == null) {
			return true;
		}

		// This cannot be the active connection if it is not connected
		if (!this.socketChannel.isConnected() || !this.socketChannel.isOpen()) {
			return false;
		}

		// If the previous connection still considers itself as the active connection, wait for the previous connection
		// to finish first
		if (this.previousConnection.isActiveConnection()) {
			return false;
		} else {
			this.previousConnection = null;
		}

		return true;
	}

	public void closeConnection(SelectionKey key) {

		try {
			this.socketChannel.close();
		} catch (IOException ioe) {
			LOG.error("On IOException occured while closing the socket: + " + StringUtils.stringifyException(ioe));
		}

		// Cancel key
		key.cancel();

		byteBufferedChannelManager.unregisterIncomingConnection(this.socketChannel);
		this.socketChannel = null;
	}

	public int getSequenceNumberToExpectNext() {
		return this.sequenceNumberToExpectNext;
	}

	public void setSequenceNumberToExpectNext(int sequenceNumberToExpectNext) {
		this.sequenceNumberToExpectNext = sequenceNumberToExpectNext;
	}
}
