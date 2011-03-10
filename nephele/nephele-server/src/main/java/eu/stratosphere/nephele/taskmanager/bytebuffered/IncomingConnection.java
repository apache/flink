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
import java.nio.channels.FileChannel;
import java.nio.channels.ReadableByteChannel;
import java.nio.channels.SelectionKey;
import java.nio.channels.SocketChannel;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import eu.stratosphere.nephele.taskmanager.bufferprovider.ReadBufferProvider;
import eu.stratosphere.nephele.taskmanager.transferenvelope.TransferEnvelope;
import eu.stratosphere.nephele.taskmanager.transferenvelope.TransferEnvelopeDeserializer;
import eu.stratosphere.nephele.taskmanager.transferenvelope.TransferEnvelopeDispatcher;
import eu.stratosphere.nephele.util.StringUtils;

/**
 * This class represents an incoming data connection through which data streams are read and transformed into
 * {@link TransferEnvelope} objects. The source of the data stream can be either a TCP connection or a recovery
 * checkpoint.
 * 
 * @author warneke
 */
public class IncomingConnection {

	/**
	 * The log object used to report debug information and possible errors.
	 */
	private static final Log LOG = LogFactory.getLog(IncomingConnection.class);

	/**
	 * The readable byte channel through which the input data is retrieved.
	 */
	private final ReadableByteChannel readableByteChannel;

	/**
	 * The {@link TransferEnvelopeDeserializer} used to transform the read bytes into transfer envelopes which can be
	 * passed on to the respective channels.
	 */
	private final TransferEnvelopeDeserializer deserializer;

	private final TransferEnvelopeDispatcher transferEnvelopeDispatcher;

	private final NetworkConnectionManager networkConnectionManager;

	/**
	 * Indicates if this incoming connection object reads from a checkpoint or a TCP connection.
	 */
	private final boolean readsFromCheckpoint;

	private final IncomingConnectionID incomingConnectionID;

	private IncomingConnection previousConnection = null;

	public IncomingConnection(IncomingConnectionID incomingConnectionID,
			NetworkConnectionManager networkConnectionManager, TransferEnvelopeDispatcher transferEnvelopeDispatcher,
			ReadBufferProvider readBufferProvider, ReadableByteChannel readableByteChannel) {
		this.incomingConnectionID = incomingConnectionID;
		this.networkConnectionManager = networkConnectionManager;
		this.transferEnvelopeDispatcher = transferEnvelopeDispatcher;
		this.readsFromCheckpoint = (this.readableByteChannel instanceof FileChannel);
		this.deserializer = new TransferEnvelopeDeserializer(readBufferProvider, readsFromCheckpoint);
		this.readableByteChannel = readableByteChannel;
	}

	public void reportTransmissionProblem(SelectionKey key, IOException ioe) {

		// First, write IOException to log
		if (!this.readsFromCheckpoint) {
			final SocketChannel socketChannel = (SocketChannel) this.readableByteChannel;
			LOG.error("Connection from " + socketChannel.socket().getRemoteSocketAddress()
				+ " encountered an IOException");
		}
		LOG.error(ioe);

		try {
			this.readableByteChannel.close();
		} catch (IOException e) {
			LOG.debug("An error occurred while closing the byte channel");
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
		// Unregister incoming connection
		this.networkConnectionManager.unregisterIncomingConnection(this.incomingConnectionID, this.readableByteChannel);
	}

	public void read() throws IOException, EOFException {

		if (!isActiveConnection()) {
			System.out.println("Is not active connection");
			return;
		}

		this.deserializer.read(this.readableByteChannel);

		final TransferEnvelope transferEnvelope = this.deserializer.getFullyDeserializedTransferEnvelope();
		if (transferEnvelope != null) {
			this.transferEnvelopeDispatcher.processEnvelopeFromNetworkOrCheckpoint(transferEnvelope);
		}

	}

	public boolean isCloseUnexpected() {

		return this.deserializer.hasUnfinishedData();
	}

	public void setPreviousConnection(IncomingConnection previousConnection) {
		this.previousConnection = previousConnection;
	}

	private boolean isActiveConnection() {

		if (this.readsFromCheckpoint) {
			return true;
		}

		// Channel is connected, if there is no previous connection, this is the active connection
		if (this.previousConnection == null) {
			return true;
		}

		// This cannot be the active connection if corresponding byte channel is closed
		if (!this.readableByteChannel.isOpen()) {
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

	public ReadableByteChannel getReadableByteChannel() {
		return this.readableByteChannel;
	}

	public void closeConnection(SelectionKey key) {

		try {
			this.readableByteChannel.close();
		} catch (IOException ioe) {
			LOG.error("On IOException occured while closing the socket: + " + StringUtils.stringifyException(ioe));
		}

		// Cancel key
		if (key != null) {
			key.cancel();
		}

		this.networkConnectionManager.unregisterIncomingConnection(this.incomingConnectionID,
			this.readableByteChannel);
	}
}
