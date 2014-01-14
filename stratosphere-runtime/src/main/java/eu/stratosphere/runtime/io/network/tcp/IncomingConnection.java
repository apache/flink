/***********************************************************************************************************************
 * Copyright (C) 2010-2013 by the Stratosphere project (http://stratosphere.eu)
 *
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software distributed under the License is distributed on
 * an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the License for the
 * specific language governing permissions and limitations under the License.
 **********************************************************************************************************************/

package eu.stratosphere.runtime.io.network.tcp;

import eu.stratosphere.runtime.io.Buffer;
import eu.stratosphere.runtime.io.network.ChannelManager;
import eu.stratosphere.runtime.io.network.envelope.Envelope;
import eu.stratosphere.runtime.io.network.envelope.EnvelopeReader;
import eu.stratosphere.runtime.io.network.envelope.EnvelopeReader.DeserializationState;
import eu.stratosphere.runtime.io.network.envelope.NoBufferAvailableException;
import eu.stratosphere.util.StringUtils;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import java.io.IOException;
import java.nio.channels.ReadableByteChannel;
import java.nio.channels.SelectionKey;

/**
 * An incoming TCP connection through which data is read and transformed into {@link Envelope} objects.
 */
public class IncomingConnection {

	private static final Log LOG = LogFactory.getLog(IncomingConnection.class);

	/** Readable byte channel (TCP socket) to read data from */
	private final ReadableByteChannel channel;

	/** Channel manager to dispatch complete envelopes */
	private final ChannelManager channelManager;

	/** Envelope reader to turn the channel data into envelopes */
	private final EnvelopeReader reader;

	// -----------------------------------------------------------------------------------------------------------------

	public IncomingConnection(ReadableByteChannel channel, ChannelManager channelManager) {
		this.channel = channel;
		this.channelManager = channelManager;
		this.reader = new EnvelopeReader(channelManager);
	}

	// -----------------------------------------------------------------------------------------------------------------

	public void read() throws IOException, InterruptedException, NoBufferAvailableException {
		DeserializationState deserializationState = this.reader.readNextChunk(this.channel);

		switch (deserializationState) {
			case COMPLETE:
				Envelope envelope = this.reader.getFullyDeserializedTransferEnvelope();
				this.channelManager.dispatchFromNetwork(envelope);
				this.reader.reset();
				break;

			case NO_BUFFER_AVAILABLE:
				throw new NoBufferAvailableException(this.reader.getBufferProvider());

			case PENDING:
				break;
		}
	}

	public void reportTransmissionProblem(SelectionKey key, IOException ioe) {
		LOG.error(StringUtils.stringifyException(ioe));

		try {
			this.channel.close();
		} catch (IOException e) {
			LOG.debug("An error occurred while closing the byte channel");
		}

		if (key != null) {
			key.cancel();
		}

		Envelope pendingEnvelope = this.reader.getPendingEnvelope();
		if (pendingEnvelope != null) {
			if (pendingEnvelope.hasBuffer()) {
				Buffer buffer = pendingEnvelope.getBuffer();
				if (buffer != null) {
					buffer.recycleBuffer();
				}
			}
		}

		this.reader.reset();
	}

	public boolean isCloseUnexpected() {
		return this.reader.hasUnfinishedData();
	}

	public void closeConnection(SelectionKey key) {
		try {
			this.channel.close();
		} catch (IOException ioe) {
			LOG.error("An IOException occurred while closing the socket: + " + StringUtils.stringifyException(ioe));
		}

		if (key != null) {
			key.cancel();
		}
	}
}
