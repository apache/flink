/***********************************************************************************************************************
 * Copyright (C) 2010-2014 by the Stratosphere project (http://stratosphere.eu)
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

package eu.stratosphere.runtime.io.network.envelope;

import eu.stratosphere.runtime.io.Buffer;
import eu.stratosphere.runtime.io.channels.ChannelID;
import eu.stratosphere.nephele.jobgraph.JobID;
import eu.stratosphere.runtime.io.network.bufferprovider.BufferProvider;
import eu.stratosphere.runtime.io.network.bufferprovider.BufferProviderBroker;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.nio.channels.ReadableByteChannel;

public class EnvelopeReader {

	public enum DeserializationState {
		COMPLETE,
		PENDING,
		NO_BUFFER_AVAILABLE;
	}

	private final BufferProviderBroker bufferProviderBroker;

	private final ByteBuffer headerBuffer;

	private ByteBuffer currentHeaderBuffer;

	private ByteBuffer currentEventsList;

	private ByteBuffer currentDataBuffer;

	private int bufferRequestPendingWithSize;


	private Envelope pendingEnvelope;

	private Envelope constructedEnvelope;


	public BufferProvider bufferProvider;

	private JobID lastDeserializedJobID;

	private ChannelID lastDeserializedSourceID;


	public EnvelopeReader(BufferProviderBroker bufferProviderBroker) {
		this.bufferProviderBroker = bufferProviderBroker;

		this.headerBuffer = ByteBuffer.allocateDirect(EnvelopeWriter.HEADER_SIZE);
		this.headerBuffer.order(ByteOrder.LITTLE_ENDIAN);

		this.currentHeaderBuffer = this.headerBuffer;
	}

	public DeserializationState readNextChunk(ReadableByteChannel channel) throws IOException {

		// 1) check if the header is pending
		if (this.currentHeaderBuffer != null) {
			ByteBuffer header = this.currentHeaderBuffer;

			channel.read(header);
			if (header.hasRemaining()) {
				// not finished with the header
				return DeserializationState.PENDING;
			} else {
				// header done, construct the envelope
				this.currentHeaderBuffer = null;

				Envelope env = constructEnvelopeFromHeader(header);
				this.pendingEnvelope = env;

				// check for events and data
				int eventsSize = getEventListSize(header);
				int bufferSize = getBufferSize(header);

				// make the events list the next buffer to be read
				if (eventsSize > 0) {
					this.currentEventsList = ByteBuffer.allocate(eventsSize);
				}

				// if we have a data buffer, we need memory segment for it
				// we may not immediately get the memory segment, though, so we first record
				// that we need it
				if (bufferSize > 0) {
					this.bufferRequestPendingWithSize = bufferSize;
				}
			}
		}

		// 2) read the eventList, if it should have one
		if (this.currentEventsList != null) {
			channel.read(this.currentEventsList);
			if (this.currentEventsList.hasRemaining()) {
				// events list still incomplete
				return DeserializationState.PENDING;
			} else {
				this.currentEventsList.flip();
				this.pendingEnvelope.setEventsSerialized(this.currentEventsList);
				this.currentEventsList = null;
			}
		}

		// 3) check if we need to get a buffer
		if (this.bufferRequestPendingWithSize > 0) {
			Buffer b = getBufferForTarget(this.pendingEnvelope.getJobID(), this.pendingEnvelope.getSource(), this.bufferRequestPendingWithSize);
			if (b == null) {
				// no buffer available at this time. come back later
				return DeserializationState.NO_BUFFER_AVAILABLE;
			} else {
				// buffer is available. set the field so the buffer will be filled
				this.pendingEnvelope.setBuffer(b);
				this.currentDataBuffer = b.getMemorySegment().wrap(0, this.bufferRequestPendingWithSize);
				this.bufferRequestPendingWithSize = 0;
			}
		}

		// 4) fill the buffer
		if (this.currentDataBuffer != null) {
			channel.read(this.currentDataBuffer);
			if (this.currentDataBuffer.hasRemaining()) {
				// data buffer incomplete
				return DeserializationState.PENDING;
			} else {
				this.currentDataBuffer = null;
			}
		}

		// if we get here, we completed our job, or did nothing, if the deserializer was not
		// reset after the previous envelope
		if (this.pendingEnvelope != null) {
			this.constructedEnvelope = this.pendingEnvelope;
			this.pendingEnvelope = null;
			return DeserializationState.COMPLETE;
		} else {
			throw new IllegalStateException("Error: read() was called before reserializer was reset after the last envelope.");
		}
	}

	private Envelope constructEnvelopeFromHeader(ByteBuffer header) throws IOException {
		int magicNumber = header.getInt(EnvelopeWriter.MAGIC_NUMBER_OFFSET);

		if (magicNumber != EnvelopeWriter.MAGIC_NUMBER) {
			throw new IOException("Network stream corrupted: invalid magic number in envelope header.");
		}

		int seqNum = header.getInt(EnvelopeWriter.SEQUENCE_NUMBER_OFFSET);
		JobID jid = JobID.fromByteBuffer(header, EnvelopeWriter.JOB_ID_OFFSET);
		ChannelID cid = ChannelID.fromByteBuffer(header, EnvelopeWriter.CHANNEL_ID_OFFSET);
		return new Envelope(seqNum, jid, cid);
	}

	private int getBufferSize(ByteBuffer header) {
		return header.getInt(EnvelopeWriter.BUFFER_SIZE_OFFSET);
	}

	private int getEventListSize(ByteBuffer header) {
		return header.getInt(EnvelopeWriter.EVENTS_SIZE_OFFSET);
	}

	private Buffer getBufferForTarget(JobID jid, ChannelID cid, int size) throws IOException {
		if (!(jid.equals(this.lastDeserializedJobID) && cid.equals(this.lastDeserializedSourceID))) {
			this.bufferProvider = this.bufferProviderBroker.getBufferProvider(jid, cid);
			this.lastDeserializedJobID = jid;
			this.lastDeserializedSourceID = cid;
		}

		return this.bufferProvider.requestBuffer(size);
	}


	public Envelope getFullyDeserializedTransferEnvelope() {
		Envelope t = this.constructedEnvelope;
		if (t == null) {
			throw new IllegalStateException("Envelope has not yet been fully constructed.");
		}

		this.constructedEnvelope = null;
		return t;
	}

	public void reset() {
		this.headerBuffer.clear();
		this.currentHeaderBuffer = this.headerBuffer;
		this.constructedEnvelope = null;
	}

	public boolean hasUnfinishedData() {
		return this.pendingEnvelope != null || this.currentHeaderBuffer != null;
	}

	public BufferProvider getBufferProvider() {
		return bufferProvider;
	}

	public Envelope getPendingEnvelope() {
		return pendingEnvelope;
	}
}
