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
import java.nio.channels.ReadableByteChannel;
import java.nio.channels.SelectionKey;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import eu.stratosphere.nephele.taskmanager.bufferprovider.BufferProvider;
import eu.stratosphere.nephele.taskmanager.routing.ByteBufferedChannelManager;
import eu.stratosphere.nephele.taskmanager.transferenvelope.DefaultDeserializer;
import eu.stratosphere.nephele.taskmanager.transferenvelope.NoBufferAvailableException;
import eu.stratosphere.nephele.taskmanager.transferenvelope.TransferEnvelope;
import eu.stratosphere.nephele.util.StringUtils;

/**
 * This class represents an incoming data connection through which data streams are read and transformed into
 * {@link TransferEnvelope} objects.
 * 
 * @author warneke
 */
final class IncomingConnection {

	/**
	 * The log object used to report debug information and possible errors.
	 */
	private static final Log LOG = LogFactory.getLog(IncomingConnection.class);

	/**
	 * The readable byte channel through which the input data is retrieved.
	 */
	private final ReadableByteChannel readableByteChannel;

	/**
	 * The {@link DefaultDeserializer} used to transform the read bytes into transfer envelopes which can be
	 * passed on to the respective channels.
	 */
	private final DefaultDeserializer deserializer;

	/**
	 * The byte buffered channel manager which handles and dispatches the received transfer envelopes.
	 */
	private final ByteBufferedChannelManager byteBufferedChannelManager;

	IncomingConnection(ByteBufferedChannelManager byteBufferedChannelManager,
			ReadableByteChannel readableByteChannel) {
		this.byteBufferedChannelManager = byteBufferedChannelManager;
		this.deserializer = new DefaultDeserializer(byteBufferedChannelManager);
		this.readableByteChannel = readableByteChannel;
	}

	void reportTransmissionProblem(SelectionKey key, IOException ioe) {

		LOG.error(StringUtils.stringifyException(ioe));

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
	}

	void read() throws IOException, InterruptedException, NoBufferAvailableException {

		this.deserializer.read(this.readableByteChannel);

		final TransferEnvelope transferEnvelope = this.deserializer.getFullyDeserializedTransferEnvelope();
		if (transferEnvelope != null) {

			final BufferProvider bufferProvider = this.deserializer.getBufferProvider();
			if (bufferProvider == null) {
				this.byteBufferedChannelManager.processEnvelopeFromNetwork(transferEnvelope, false);
			} else {
				this.byteBufferedChannelManager.processEnvelopeFromNetwork(transferEnvelope, bufferProvider.isShared());
			}
		}
	}

	boolean isCloseUnexpected() {

		return this.deserializer.hasUnfinishedData();
	}

	ReadableByteChannel getReadableByteChannel() {
		return this.readableByteChannel;
	}

	void closeConnection(SelectionKey key) {

		try {
			this.readableByteChannel.close();
		} catch (IOException ioe) {
			LOG.error("On IOException occured while closing the socket: + " + StringUtils.stringifyException(ioe));
		}

		// Cancel key
		if (key != null) {
			key.cancel();
		}
	}
}