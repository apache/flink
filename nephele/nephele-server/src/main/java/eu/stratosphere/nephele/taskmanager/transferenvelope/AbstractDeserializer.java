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
import java.util.List;

import eu.stratosphere.nephele.event.task.AbstractEvent;
import eu.stratosphere.nephele.io.channels.Buffer;
import eu.stratosphere.nephele.io.channels.ChannelID;
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

	private final ObjectDeserializer objectDeserializer = new ObjectDeserializer();

	private final ByteBuffer tempBuffer = ByteBuffer.allocate(8); // TODO: Make this configurable

	private boolean bufferExistanceDeserialized = false;

	private boolean eventListExistanceDeserialized = false;

	private boolean sequenceNumberDeserializationStarted = false;

	private int sizeOfBuffer = -1;

	private int deserializedSequenceNumber = -1;

	private Buffer buffer = null;

	private JobID deserializedJobID = null;

	private ChannelID deserializedSourceID = null;

	private List<AbstractEvent> deserializedEventList = null;

	public void read(ReadableByteChannel readableByteChannel) throws IOException, NoBufferAvailableException {

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

	protected final ByteBuffer getTempBuffer() {
		return this.tempBuffer;
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
			this.tempBuffer.position(0);
			this.tempBuffer.limit(SIZEOFINT);
			this.sequenceNumberDeserializationStarted = true;
		}

		if (readableByteChannel.read(this.tempBuffer) == -1) {

			if (this.tempBuffer.position() == 0) {
				// Regular end of stream
				throw new EOFException();
			} else {
				throw new IOException("Unexpected end of stream while deserializing the sequence number");
			}
		}

		if (!this.tempBuffer.hasRemaining()) {

			this.deserializedSequenceNumber = byteBufferToInteger(this.tempBuffer, 0);
			if (this.deserializedSequenceNumber < 0) {
				throw new IOException("Received invalid sequence number: " + this.deserializedSequenceNumber);
			}

			this.deserializationState = DeserializationState.SEQUENCENUMBERDESERIALIZED;
			this.sequenceNumberDeserializationStarted = false;
			this.transferEnvelope = null;
			this.sizeOfBuffer = -1;
			this.bufferExistanceDeserialized = false;
			this.eventListExistanceDeserialized = false;
			this.tempBuffer.clear();
			this.buffer = null;
			this.objectDeserializer.clear();
			this.deserializedEventList = null;
			return false;
		}

		return true;
	}

	private boolean readID(final ReadableByteChannel readableByteChannel) throws IOException {

		if (this.deserializationState == DeserializationState.SEQUENCENUMBERDESERIALIZED) {

			this.deserializedJobID = this.objectDeserializer.deserialize(readableByteChannel, JobID.class);
			if (this.deserializedJobID == null) {
				return true;
			}

			this.deserializationState = DeserializationState.JOBIDDESERIALIZED;

		} else {

			this.deserializedSourceID = this.objectDeserializer.deserialize(readableByteChannel, ChannelID.class);
			if (this.deserializedSourceID == null) {
				return true;
			}

			this.deserializationState = DeserializationState.SOURCEDESERIALIZED;
		}

		return false;
	}

	@SuppressWarnings("unchecked")
	private boolean readNotificationList(ReadableByteChannel readableByteChannel) throws IOException {

		if (!this.eventListExistanceDeserialized) {
			this.tempBuffer.position(0);
			this.tempBuffer.limit(1);
			readableByteChannel.read(this.tempBuffer);

			if (this.tempBuffer.hasRemaining()) {
				return true;
			}

			this.eventListExistanceDeserialized = true;
			final boolean eventListFollows = (this.tempBuffer.get(0) == (byte) 1);
			this.tempBuffer.clear();

			if (!eventListFollows) {
				// No event list here
				this.transferEnvelope = new TransferEnvelope(this.deserializedSequenceNumber, this.deserializedJobID,
					this.deserializedSourceID, this.deserializedEventList);
				this.deserializationState = DeserializationState.NOTIFICATIONSDESERIALIZED;
				return false;
			}
		}

		this.deserializedEventList = this.objectDeserializer.deserialize(readableByteChannel, List.class);
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
	 * Read the buffer's actual data from the stream.
	 * 
	 * @param readableByteChannel
	 *        the stream to read the buffer data from
	 * @return <code>true</code> if more buffer data need to be read from the stream, <code>false</code> otherwise
	 * @throws IOException
	 *         thrown if an I/O error occurred while reading data from the stream
	 * @throws NoBufferAvailableException
	 *         thrown if the deserialization process could not be continued due to a lack of buffers
	 */
	protected abstract boolean readBufferData(ReadableByteChannel readableByteChannel) throws IOException,
			NoBufferAvailableException;

	private boolean readBuffer(final ReadableByteChannel readableByteChannel) throws IOException,
			NoBufferAvailableException {

		if (!this.bufferExistanceDeserialized) {

			this.tempBuffer.position(0);
			this.tempBuffer.limit(1);

			final int bytesRead = readableByteChannel.read(this.tempBuffer);
			if (bytesRead == -1) {
				if (this.tempBuffer.get(0) == 0 && this.tempBuffer.position() == 1) { // Regular end, no
					// buffer will follow
					throw new EOFException();
				} else {
					throw new IOException("Deserialization error: Expected at least "
						+ this.tempBuffer.remaining() + " more bytes to follow");
				}
			} else if (bytesRead == 0) {
				try {
					Thread.sleep(50);
				} catch (InterruptedException e) {
				}
			}

			if (!this.tempBuffer.hasRemaining()) {
				this.bufferExistanceDeserialized = true;
				this.tempBuffer.position(0);
				this.tempBuffer.limit(SIZEOFINT);
				if (this.tempBuffer.get(0) == 0) {
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
			final int bytesRead = readableByteChannel.read(this.tempBuffer);
			if (bytesRead == -1) {
				throw new IOException("Deserialization error: Expected at least " + this.tempBuffer.remaining()
					+ " more bytes to follow");
			}

			if (!this.tempBuffer.hasRemaining()) {
				this.sizeOfBuffer = byteBufferToInteger(this.tempBuffer, 0);
				// System.out.println("INCOMING: Buffer size is " + this.sizeOfBuffer);

				if (this.sizeOfBuffer <= 0) {
					throw new IOException("Invalid buffer size: " + this.sizeOfBuffer);
				}
			} else {
				return true;
			}
		}

		if (readBufferData(readableByteChannel)) {
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

		return this.objectDeserializer.hasUnfinishedData();
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
