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

package eu.stratosphere.nephele.taskmanager.transferenvelope;

import java.io.EOFException;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.ReadableByteChannel;

import eu.stratosphere.nephele.event.task.EventList;
import eu.stratosphere.nephele.io.DefaultRecordDeserializer;
import eu.stratosphere.nephele.io.channels.Buffer;
import eu.stratosphere.nephele.io.channels.ChannelID;
import eu.stratosphere.nephele.io.channels.DeserializationBuffer;
import eu.stratosphere.nephele.jobgraph.JobID;

public abstract class AbstractDeserializer {

	private enum DeserializationState {
		NOTDESERIALIZED,
		SEQUENCENUMBERDESERIALIZED,
		JOBIDDESERIALIZED,
		SOURCEDESERIALIZED,
		NOTIFICATIONSDESERIALIZED,
		FULLYDESERIALIZED
	};

	private static final int SIZEOFINT = 4;

	private TransferEnvelope transferEnvelope = null;

	private DeserializationState deserializationState = DeserializationState.NOTDESERIALIZED;

	private final DeserializationBuffer<ChannelID> channelIDDeserializationBuffer = new DeserializationBuffer<ChannelID>(
		new DefaultRecordDeserializer<ChannelID>(ChannelID.class), true);

	private final DeserializationBuffer<JobID> jobIDDeserializationBuffer = new DeserializationBuffer<JobID>(
			new DefaultRecordDeserializer<JobID>(JobID.class), true);

	private final DeserializationBuffer<EventList> notificationListDeserializationBuffer = new DeserializationBuffer<EventList>(
		new DefaultRecordDeserializer<EventList>(EventList.class), true);

	private final ByteBuffer existanceBuffer = ByteBuffer.allocate(1); // 1 byte for existence of buffer

	private final ByteBuffer lengthBuffer = ByteBuffer.allocate(SIZEOFINT);

	private boolean bufferExistanceDeserialized = false;

	private boolean eventListExistanceDeserialized = false;

	private boolean sequenceNumberDeserializationStarted = false;

	private int sizeOfBuffer = -1;

	private int deserializedSequenceNumber = -1;

	private Buffer buffer = null;

	private JobID deserializedJobID = null;

	private ChannelID deserializedSourceID = null;

	private EventList deserializedEventList = null;

	public void read(ReadableByteChannel readableByteChannel) throws IOException {

		while (true) {

			// System.out.println("INCOMING State: " + this.deserializationState);

			boolean waitingForMoreData = false;

			switch (deserializationState) {
			case NOTDESERIALIZED:
				waitingForMoreData = readSequenceNumber(readableByteChannel);
				break;
			case SEQUENCENUMBERDESERIALIZED:
				waitingForMoreData = readID(readableByteChannel);
				break;
			case JOBIDDESERIALIZED:
				waitingForMoreData = readID(readableByteChannel);
				break;
			case SOURCEDESERIALIZED:
				waitingForMoreData = readNotificationList(readableByteChannel);
				break;
			case NOTIFICATIONSDESERIALIZED:
				waitingForMoreData = readBuffer(readableByteChannel);
				break;
			case FULLYDESERIALIZED:
				return;
			}

			if (waitingForMoreData) {
				return;
			}

		}
	}

	protected void setBuffer(final Buffer buffer) {
		this.buffer = buffer;
	}
	
	protected int getSizeOfBuffer() {
		return this.sizeOfBuffer;
	}
	
	protected JobID getDeserializedJobID() {
		return this.deserializedJobID;
	}
	
	protected ChannelID getDeserializedSourceID() {
		return this.deserializedSourceID;
	}
	
	private boolean readSequenceNumber(ReadableByteChannel readableByteChannel) throws IOException {

		if (!this.sequenceNumberDeserializationStarted) {
			this.lengthBuffer.clear();
			this.sequenceNumberDeserializationStarted = true;
		}

		if (readableByteChannel.read(this.lengthBuffer) == -1) {

			if (this.lengthBuffer.position() == 0) {
				// Regular end of stream
				throw new EOFException();
			} else {
				throw new IOException("Unexpected end of stream while deserializing the sequence number");
			}
		}

		if (!this.lengthBuffer.hasRemaining()) {

			this.deserializedSequenceNumber = byteBufferToInteger(this.lengthBuffer, 0);
			if (this.deserializedSequenceNumber < 0) {
				throw new IOException("Received invalid sequence number: " + this.deserializedSequenceNumber);
			}

			this.deserializationState = DeserializationState.SEQUENCENUMBERDESERIALIZED;
			this.sequenceNumberDeserializationStarted = false;
			this.transferEnvelope = null;
			this.sizeOfBuffer = -1;
			this.bufferExistanceDeserialized = false;
			this.eventListExistanceDeserialized = false;
			this.existanceBuffer.clear();
			this.lengthBuffer.clear();
			this.jobIDDeserializationBuffer.clear();
			this.channelIDDeserializationBuffer.clear();
			this.deserializedEventList = null;
			return false;
		}

		return true;
	}

	private boolean readID(ReadableByteChannel readableByteChannel) throws IOException {

		if (this.deserializationState == DeserializationState.SEQUENCENUMBERDESERIALIZED) {

			this.deserializedJobID = this.jobIDDeserializationBuffer.readData(null, readableByteChannel);
			if (this.deserializedJobID == null) {
				return true;
			}

			this.deserializationState = DeserializationState.JOBIDDESERIALIZED;

		} else {

			this.deserializedSourceID = this.channelIDDeserializationBuffer.readData(null, readableByteChannel);
			if (this.deserializedSourceID == null) {
				return true;
			}

			this.deserializationState = DeserializationState.SOURCEDESERIALIZED;
		}

		return false;
	}

	private boolean readNotificationList(ReadableByteChannel readableByteChannel) throws IOException {

		if (!this.eventListExistanceDeserialized) {
			readableByteChannel.read(this.existanceBuffer);

			if (this.existanceBuffer.hasRemaining()) {
				return true;
			}

			this.eventListExistanceDeserialized = true;
			final boolean eventListFollows = (this.existanceBuffer.get(0) == (byte) 1);
			this.existanceBuffer.clear();

			if (!eventListFollows) {
				// No event list here
				this.transferEnvelope = new TransferEnvelope(this.deserializedSequenceNumber, this.deserializedJobID,
					this.deserializedSourceID, this.deserializedEventList);
				this.deserializationState = DeserializationState.NOTIFICATIONSDESERIALIZED;
				return false;
			}
		}

		this.deserializedEventList = this.notificationListDeserializationBuffer.readData(null, readableByteChannel);
		if (this.deserializedEventList == null) {
			return true;
		} else {
			this.transferEnvelope = new TransferEnvelope(this.deserializedSequenceNumber, this.deserializedJobID,
				this.deserializedSourceID, this.deserializedEventList);
			this.deserializationState = DeserializationState.NOTIFICATIONSDESERIALIZED;
			return false;
		}
	}

	/**
	 * Reconstructs the transfer envelope's buffer from the stream.
	 * 
	 * @param readableByteChannel
	 *        the stream to reconstruct the buffer from.
	 * @return <code>true</code> if the buffer has been successfully reconstructed, <code>false</code> if more data from
	 *         the stream is required
	 * 
	 * @throws IOException thrown if an I/O error occurred while reading data from the stream
	 */
	protected abstract boolean reconstructBufferFromStream(ReadableByteChannel readableByteChannel) throws IOException;

	private boolean readBuffer(ReadableByteChannel readableByteChannel) throws IOException {

		if (!this.bufferExistanceDeserialized) {

			final int bytesRead = readableByteChannel.read(this.existanceBuffer);
			if (bytesRead == -1) {
				if (this.existanceBuffer.get(0) == 0 && this.existanceBuffer.position() == 1) { // Regular end, no
					// buffer will follow
					throw new EOFException();
				} else {
					throw new IOException("Deserialization error: Expected at least "
						+ this.existanceBuffer.remaining() + " more bytes to follow");
				}
			} else if (bytesRead == 0) {
				try {
					Thread.sleep(50);
				} catch (InterruptedException e) {
				}
			}

			if (!this.existanceBuffer.hasRemaining()) {
				this.bufferExistanceDeserialized = true;
				if (this.existanceBuffer.get(0) == 0) {
					// No buffer will follow, we are done
					this.transferEnvelope.setBuffer(null);
					this.deserializationState = DeserializationState.FULLYDESERIALIZED;
					return false;
				}
			} else {
				return true;
			}
		}

		if (this.sizeOfBuffer < 0) {

			// We need to deserialize the size of the buffer
			final int bytesRead = readableByteChannel.read(this.lengthBuffer);
			if (bytesRead == -1) {
				throw new IOException("Deserialization error: Expected at least " + this.existanceBuffer.remaining()
					+ " more bytes to follow");
			}

			if (!this.lengthBuffer.hasRemaining()) {
				this.sizeOfBuffer = byteBufferToInteger(this.lengthBuffer, 0);
				// System.out.println("INCOMING: Buffer size is " + this.sizeOfBuffer);

				if (this.sizeOfBuffer <= 0) {
					throw new IOException("Invalid buffer size: " + this.sizeOfBuffer);
				}
			} else {
				return true;
			}
		}

		if (!reconstructBufferFromStream(readableByteChannel)) {
			return true;
		}

		this.transferEnvelope.setBuffer(this.buffer);
		this.deserializationState = DeserializationState.FULLYDESERIALIZED;
		return false;
	}

	public TransferEnvelope getFullyDeserializedTransferEnvelope() {

		if (this.deserializationState == DeserializationState.FULLYDESERIALIZED) {
			this.deserializationState = DeserializationState.NOTDESERIALIZED;
			return this.transferEnvelope;
		}

		return null;
	}

	public Buffer getBuffer() {
		return this.buffer;
	}

	public void reset() {
		this.deserializationState = DeserializationState.NOTDESERIALIZED;
		this.sequenceNumberDeserializationStarted = false;
	}

	public boolean hasUnfinishedData() {

		if (this.deserializationState != DeserializationState.NOTDESERIALIZED) {
			return true;
		}

		return this.channelIDDeserializationBuffer.hasUnfinishedData();
	}

	private int byteBufferToInteger(ByteBuffer byteBuffer, int offset) throws IOException {

		int integer = 0;

		if ((offset + SIZEOFINT) > byteBuffer.limit()) {
			throw new IOException("Cannot convert byte buffer to integer, not enough data in byte buffer ("
				+ byteBuffer.limit() + ")");
		}

		for (int i = 0; i < SIZEOFINT; ++i) {
			integer |= (byteBuffer.get((offset + SIZEOFINT - 1) - i) & 0xff) << (i << 3);
		}

		return integer;
	}
}
