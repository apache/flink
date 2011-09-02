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
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.channels.ReadableByteChannel;

import eu.stratosphere.nephele.event.task.EventList;
import eu.stratosphere.nephele.io.DefaultRecordDeserializer;
import eu.stratosphere.nephele.io.channels.Buffer;
import eu.stratosphere.nephele.io.channels.BufferFactory;
import eu.stratosphere.nephele.io.channels.ChannelID;
import eu.stratosphere.nephele.io.channels.DeserializationBuffer;
import eu.stratosphere.nephele.io.channels.FileBufferManager;

public class TransferEnvelopeDeserializer {

	private enum DeserializationState {
		NOTDESERIALIZED,
		SEQUENCENUMBERDESERIALIZED,
		SOURCEDESERIALIZED,
		TARGETDESERIALIZED,
		NOTIFICATIONSDESERIALIZED,
		FULLYDESERIALIZED
	};

	private static final int SIZEOFINT = 4;

	private TransferEnvelope transferEnvelope = null;

	private DeserializationState deserializationState = DeserializationState.NOTDESERIALIZED;

	private final ByteBufferedChannelManager byteBufferedChannelManager;

	private final DeserializationBuffer<ChannelID> channelIDDeserializationBuffer = new DeserializationBuffer<ChannelID>(
		new DefaultRecordDeserializer<ChannelID>(ChannelID.class), true);

	private final DeserializationBuffer<EventList> notificationListDeserializationBuffer = new DeserializationBuffer<EventList>(
		new DefaultRecordDeserializer<EventList>(EventList.class), true);

	private final ByteBuffer existanceBuffer = ByteBuffer.allocate(1); // 1 byte for existence of buffer

	private final ByteBuffer lengthBuffer = ByteBuffer.allocate(SIZEOFINT);

	private final boolean readsFromCheckpoint;

	private Buffer buffer = null;

	private boolean bufferExistanceDeserialized = false;

	private boolean sequenceNumberDeserializationStarted = false;

	private int sizeOfBuffer = -1;

	public TransferEnvelopeDeserializer(ByteBufferedChannelManager byteBufferedChannelManager,
			boolean readsFromCheckpoint) {
		this.byteBufferedChannelManager = byteBufferedChannelManager;
		this.readsFromCheckpoint = readsFromCheckpoint;
	}

	public void read(ReadableByteChannel readableByteChannel) throws IOException {

		while (true) {

			// System.out.println("INCOMING State: " + this.deserializationState);

			boolean waitingForMoreData = false;

			switch (deserializationState) {
			case NOTDESERIALIZED:
				waitingForMoreData = readSequenceNumber(readableByteChannel);
				break;
			case SEQUENCENUMBERDESERIALIZED:
				waitingForMoreData = readChannelID(readableByteChannel);
				break;
			case SOURCEDESERIALIZED:
				waitingForMoreData = readChannelID(readableByteChannel);
				break;
			case TARGETDESERIALIZED:
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

	private boolean readSequenceNumber(ReadableByteChannel readableByteChannel) throws IOException {

		if (!this.sequenceNumberDeserializationStarted) {
			this.lengthBuffer.clear();
			this.sequenceNumberDeserializationStarted = true;
		}

		if (readableByteChannel.read(this.lengthBuffer) == -1) {

			if (this.lengthBuffer.position() == 0) {
				// Regular end of stream
				throw new EOFException("EOF");
			} else {
				throw new IOException("Unexpected end of stream while deserializing the sequence number");
			}
		}

		if (!this.lengthBuffer.hasRemaining()) {

			final int sequenceNumber = byteBufferToInteger(this.lengthBuffer, 0);
			if (sequenceNumber < 0) {
				throw new IOException("Received invalid sequence number: " + sequenceNumber);
			}

			this.deserializationState = DeserializationState.SEQUENCENUMBERDESERIALIZED;
			this.sequenceNumberDeserializationStarted = false;
			this.transferEnvelope = new TransferEnvelope();
			this.transferEnvelope.setSequenceNumber(sequenceNumber);
			this.sizeOfBuffer = -1;
			this.bufferExistanceDeserialized = false;
			this.existanceBuffer.clear();
			this.lengthBuffer.clear();
			this.channelIDDeserializationBuffer.clear();
			this.buffer = null;
			return false;
		}

		return true;
	}

	private boolean readChannelID(ReadableByteChannel readableByteChannel) throws IOException {

		final ChannelID channelID = this.channelIDDeserializationBuffer.readData(readableByteChannel);
		if (channelID == null) {
			return true;
		}
		if (this.deserializationState == DeserializationState.SEQUENCENUMBERDESERIALIZED) {
			// System.out.println("INCOMING: Source " + channelID);
			this.transferEnvelope.setSource(channelID);
			this.deserializationState = DeserializationState.SOURCEDESERIALIZED;
		} else {
			// System.out.println("INCOMING: Target " + channelID);
			this.transferEnvelope.setTarget(channelID);
			this.deserializationState = DeserializationState.TARGETDESERIALIZED;
		}

		return false;
	}

	private boolean readNotificationList(ReadableByteChannel readableByteChannel) throws IOException {

		final EventList eventList = this.notificationListDeserializationBuffer.readData(readableByteChannel);
		if (eventList == null) {
			return true;
		} else {
			this.transferEnvelope.setEventList(eventList);
			this.deserializationState = DeserializationState.NOTIFICATIONSDESERIALIZED;
			return false;
		}
	}

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
			} else if(bytesRead == 0) {
				try {
					Thread.sleep(50);
				} catch(InterruptedException e) {
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

		if (this.buffer == null) {

			if (this.readsFromCheckpoint) {

				if (!(readableByteChannel instanceof FileChannel)) {
					throw new IOException(
						"Reading from checkpoint, but readableByteChannel is no instance of FileChannel!");
				}

				final FileChannel fileChannel = (FileChannel) readableByteChannel;
				final FileBufferManager fileBufferManager = this.byteBufferedChannelManager.getFileBufferManager();
				this.buffer = BufferFactory.createFromCheckpoint(this.sizeOfBuffer, this.transferEnvelope.getSource(),
					fileChannel.position(), fileBufferManager);
				// Skip over buffer and finish deserialization step
				fileChannel.position(fileChannel.position() + sizeOfBuffer);
				this.transferEnvelope.setBuffer(this.buffer);
				this.deserializationState = DeserializationState.FULLYDESERIALIZED;
				return false;

			} else {
				// Request read buffer from network channelManager
				this.buffer = this.byteBufferedChannelManager.requestEmptyReadBuffer(this.sizeOfBuffer,
					this.transferEnvelope.getSource());

				if (this.buffer == null) {

					try {
						Thread.sleep(100);
						// Wait for 100 milliseconds, so the NIO thread won't do busy
						// waiting...
					} catch (InterruptedException e) {
						return true;
					}

					return true;
				}
			}

		} else {

			final int bytesWritten = this.buffer.write(readableByteChannel);

			if (!this.buffer.hasRemaining()) {
				// We are done, the buffer has been fully read
				this.buffer.finishWritePhase();
				this.transferEnvelope.setBuffer(this.buffer);
				this.deserializationState = DeserializationState.FULLYDESERIALIZED;
				return false;
			} else {
				if (bytesWritten == -1) {
					throw new IOException("Deserialization error: Expected at least " + this.buffer.remaining()
						+ " more bytes to follow");
				}
			}
		}

		return true;
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

	public ChannelID getTarget() {
		if (this.transferEnvelope != null) {
			return this.transferEnvelope.getTarget();
		}

		return null;
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
