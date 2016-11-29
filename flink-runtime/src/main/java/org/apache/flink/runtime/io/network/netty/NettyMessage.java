/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.flink.runtime.io.network.netty;

import io.netty.buffer.ByteBuf;
import io.netty.buffer.ByteBufAllocator;
import io.netty.buffer.ByteBufInputStream;
import io.netty.buffer.ByteBufOutputStream;
import io.netty.channel.ChannelHandler;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.ChannelOutboundHandlerAdapter;
import io.netty.channel.ChannelPromise;
import io.netty.handler.codec.LengthFieldBasedFrameDecoder;
import io.netty.handler.codec.MessageToMessageDecoder;

import org.apache.flink.runtime.event.TaskEvent;
import org.apache.flink.runtime.executiongraph.ExecutionAttemptID;
import org.apache.flink.runtime.io.network.api.serialization.EventSerializer;
import org.apache.flink.runtime.io.network.buffer.Buffer;
import org.apache.flink.runtime.io.network.partition.ResultPartitionID;
import org.apache.flink.runtime.io.network.partition.consumer.InputChannel;
import org.apache.flink.runtime.io.network.partition.consumer.InputChannelID;
import org.apache.flink.runtime.jobgraph.IntermediateResultPartitionID;

import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.nio.ByteBuffer;
import java.util.List;

/**
 * A simple and generic interface to serialize messages to Netty's buffer space.
 */
abstract class NettyMessage {

	// ------------------------------------------------------------------------
	// Note: Every NettyMessage subtype needs to have a public 0-argument
	// constructor in order to work with the generic deserializer.
	// ------------------------------------------------------------------------

	static final int HEADER_LENGTH = 4 + 4 + 1; // frame length (4), magic number (4), msg ID (1)

	static final int MAGIC_NUMBER = 0xBADC0FFE;

	abstract ByteBuf write(ByteBufAllocator allocator) throws Exception;

	abstract void readFrom(ByteBuf buffer) throws Exception;

	// ------------------------------------------------------------------------

	private static ByteBuf allocateBuffer(ByteBufAllocator allocator, byte id) {
		return allocateBuffer(allocator, id, 0);
	}

	private static ByteBuf allocateBuffer(ByteBufAllocator allocator, byte id, int length) {
		final ByteBuf buffer = length != 0 ? allocator.directBuffer(HEADER_LENGTH + length) : allocator.directBuffer();
		buffer.writeInt(HEADER_LENGTH + length);
		buffer.writeInt(MAGIC_NUMBER);
		buffer.writeByte(id);

		return buffer;
	}

	// ------------------------------------------------------------------------
	// Generic NettyMessage encoder and decoder
	// ------------------------------------------------------------------------

	@ChannelHandler.Sharable
	static class NettyMessageEncoder extends ChannelOutboundHandlerAdapter {

		@Override
		public void write(ChannelHandlerContext ctx, Object msg, ChannelPromise promise) throws Exception {
			if (msg instanceof NettyMessage) {

				ByteBuf serialized = null;

				try {
					serialized = ((NettyMessage) msg).write(ctx.alloc());
				}
				catch (Throwable t) {
					throw new IOException("Error while serializing message: " + msg, t);
				}
				finally {
					if (serialized != null) {
						ctx.write(serialized, promise);
					}
				}
			}
			else {
				ctx.write(msg, promise);
			}
		}

		// Create the frame length decoder here as it depends on the encoder
		//
		// +------------------+------------------+--------++----------------+
		// | FRAME LENGTH (4) | MAGIC NUMBER (4) | ID (1) || CUSTOM MESSAGE |
		// +------------------+------------------+--------++----------------+
		static LengthFieldBasedFrameDecoder createFrameLengthDecoder() {
			return new LengthFieldBasedFrameDecoder(Integer.MAX_VALUE, 0, 4, -4, 4);
		}
	}

	@ChannelHandler.Sharable
	static class NettyMessageDecoder extends MessageToMessageDecoder<ByteBuf> {

		@Override
		protected void decode(ChannelHandlerContext ctx, ByteBuf msg, List<Object> out) throws Exception {
			int magicNumber = msg.readInt();

			if (magicNumber != MAGIC_NUMBER) {
				throw new IllegalStateException("Network stream corrupted: received incorrect magic number.");
			}

			byte msgId = msg.readByte();

			NettyMessage decodedMsg = null;

			if (msgId == BufferResponse.ID) {
				decodedMsg = new BufferResponse();
			}
			else if (msgId == PartitionRequest.ID) {
				decodedMsg = new PartitionRequest();
			}
			else if (msgId == TaskEventRequest.ID) {
				decodedMsg = new TaskEventRequest();
			}
			else if (msgId == ErrorResponse.ID) {
				decodedMsg = new ErrorResponse();
			}
			else if (msgId == CancelPartitionRequest.ID) {
				decodedMsg = new CancelPartitionRequest();
			}
			else if (msgId == CloseRequest.ID) {
				decodedMsg = new CloseRequest();
			}
			else {
				throw new IllegalStateException("Received unknown message from producer: " + msg);
			}

			if (decodedMsg != null) {
				decodedMsg.readFrom(msg);
				out.add(decodedMsg);
			}
		}
	}

	// ------------------------------------------------------------------------
	// Server responses
	// ------------------------------------------------------------------------

	static class BufferResponse extends NettyMessage {

		private static final byte ID = 0;

		final Buffer buffer;

		InputChannelID receiverId;

		int sequenceNumber;

		// ---- Deserialization -----------------------------------------------

		boolean isBuffer;

		int size;

		ByteBuf retainedSlice;

		public BufferResponse() {
			// When deserializing we first have to request a buffer from the respective buffer
			// provider (at the handler) and copy the buffer from Netty's space to ours.
			buffer = null;
		}

		public BufferResponse(Buffer buffer, int sequenceNumber, InputChannelID receiverId) {
			this.buffer = buffer;
			this.sequenceNumber = sequenceNumber;
			this.receiverId = receiverId;
		}

		boolean isBuffer() {
			return isBuffer;
		}

		int getSize() {
			return size;
		}

		ByteBuf getNettyBuffer() {
			return retainedSlice;
		}

		void releaseBuffer() {
			if (retainedSlice != null) {
				retainedSlice.release();
				retainedSlice = null;
			}
		}

		// --------------------------------------------------------------------
		// Serialization
		// --------------------------------------------------------------------

		@Override
		ByteBuf write(ByteBufAllocator allocator) throws IOException {
			int length = 16 + 4 + 1 + 4 + buffer.getSize();

			ByteBuf result = null;
			try {
				result = allocateBuffer(allocator, ID, length);

				receiverId.writeTo(result);
				result.writeInt(sequenceNumber);
				result.writeBoolean(buffer.isBuffer());
				result.writeInt(buffer.getSize());
				result.writeBytes(buffer.getNioBuffer());

				return result;
			}
			catch (Throwable t) {
				if (result != null) {
					result.release();
				}

				throw new IOException(t);
			}
			finally {
				if (buffer != null) {
					buffer.recycle();
				}
			}
		}

		@Override
		void readFrom(ByteBuf buffer) {
			receiverId = InputChannelID.fromByteBuf(buffer);
			sequenceNumber = buffer.readInt();
			isBuffer = buffer.readBoolean();
			size = buffer.readInt();

			retainedSlice = buffer.readSlice(size);
			retainedSlice.retain();
		}
	}

	static class ErrorResponse extends NettyMessage {

		private static final byte ID = 1;

		Throwable cause;

		InputChannelID receiverId;

		public ErrorResponse() {
		}

		ErrorResponse(Throwable cause) {
			this.cause = cause;
		}

		ErrorResponse(Throwable cause, InputChannelID receiverId) {
			this.cause = cause;
			this.receiverId = receiverId;
		}

		boolean isFatalError() {
			return receiverId == null;
		}

		@Override
		ByteBuf write(ByteBufAllocator allocator) throws IOException {
			final ByteBuf result = allocateBuffer(allocator, ID);

			try (ObjectOutputStream oos = new ObjectOutputStream(new ByteBufOutputStream(result))) {
				oos.writeObject(cause);

				if (receiverId != null) {
					result.writeBoolean(true);
					receiverId.writeTo(result);
				} else {
					result.writeBoolean(false);
				}

				// Update frame length...
				result.setInt(0, result.readableBytes());
				return result;
			}
			catch (Throwable t) {
				result.release();

				if (t instanceof IOException) {
					throw (IOException) t;
				} else {
					throw new IOException(t);
				}
			}
		}

		@Override
		void readFrom(ByteBuf buffer) throws Exception {
			try (ObjectInputStream ois = new ObjectInputStream(new ByteBufInputStream(buffer))) {
				Object obj = ois.readObject();

				if (!(obj instanceof Throwable)) {
					throw new ClassCastException("Read object expected to be of type Throwable, " +
							"actual type is " + obj.getClass() + ".");
				} else {
					cause = (Throwable) obj;

					if (buffer.readBoolean()) {
						receiverId = InputChannelID.fromByteBuf(buffer);
					}
				}
			}
		}
	}

	// ------------------------------------------------------------------------
	// Client requests
	// ------------------------------------------------------------------------

	static class PartitionRequest extends NettyMessage {

		final static byte ID = 2;

		ResultPartitionID partitionId;

		int queueIndex;

		InputChannelID receiverId;

		public PartitionRequest() {
		}

		PartitionRequest(ResultPartitionID partitionId, int queueIndex, InputChannelID receiverId) {
			this.partitionId = partitionId;
			this.queueIndex = queueIndex;
			this.receiverId = receiverId;
		}

		@Override
		ByteBuf write(ByteBufAllocator allocator) throws IOException {
			ByteBuf result = null;

			try {
				result = allocateBuffer(allocator, ID, 16 + 16 + 4 + 16);

				partitionId.getPartitionId().writeTo(result);
				partitionId.getProducerId().writeTo(result);
				result.writeInt(queueIndex);
				receiverId.writeTo(result);

				return result;
			}
			catch (Throwable t) {
				if (result != null) {
					result.release();
				}

				throw new IOException(t);
			}
		}

		@Override
		public void readFrom(ByteBuf buffer) {
			partitionId = new ResultPartitionID(IntermediateResultPartitionID.fromByteBuf(buffer), ExecutionAttemptID.fromByteBuf(buffer));
			queueIndex = buffer.readInt();
			receiverId = InputChannelID.fromByteBuf(buffer);
		}

		@Override
		public String toString() {
			return String.format("PartitionRequest(%s:%d)", partitionId, queueIndex);
		}
	}

	static class TaskEventRequest extends NettyMessage {

		final static byte ID = 3;

		TaskEvent event;

		InputChannelID receiverId;

		ResultPartitionID partitionId;

		public TaskEventRequest() {
		}

		TaskEventRequest(TaskEvent event, ResultPartitionID partitionId, InputChannelID receiverId) {
			this.event = event;
			this.receiverId = receiverId;
			this.partitionId = partitionId;
		}

		@Override
		ByteBuf write(ByteBufAllocator allocator) throws IOException {
			ByteBuf result = null;

			try {
				// TODO Directly serialize to Netty's buffer
				ByteBuffer serializedEvent = EventSerializer.toSerializedEvent(event);

				result = allocateBuffer(allocator, ID, 4 + serializedEvent.remaining() + 16 + 16 + 16);

				result.writeInt(serializedEvent.remaining());
				result.writeBytes(serializedEvent);

				partitionId.getPartitionId().writeTo(result);
				partitionId.getProducerId().writeTo(result);

				receiverId.writeTo(result);

				return result;
			}
			catch (Throwable t) {
				if (result != null) {
					result.release();
				}

				throw new IOException(t);
			}
		}

		@Override
		public void readFrom(ByteBuf buffer) throws IOException {
			// TODO Directly deserialize fromNetty's buffer
			int length = buffer.readInt();
			ByteBuffer serializedEvent = ByteBuffer.allocate(length);

			buffer.readBytes(serializedEvent);
			serializedEvent.flip();

			event = (TaskEvent) EventSerializer.fromSerializedEvent(serializedEvent, getClass().getClassLoader());

			partitionId = new ResultPartitionID(IntermediateResultPartitionID.fromByteBuf(buffer), ExecutionAttemptID.fromByteBuf(buffer));

			receiverId = InputChannelID.fromByteBuf(buffer);
		}
	}

	/**
	 * Cancels the partition request of the {@link InputChannel} identified by
	 * {@link InputChannelID}.
	 *
	 * <p> There is a 1:1 mapping between the input channel and partition per physical channel.
	 * Therefore, the {@link InputChannelID} instance is enough to identify which request to cancel.
	 */
	static class CancelPartitionRequest extends NettyMessage {

		final static byte ID = 4;

		InputChannelID receiverId;

		public CancelPartitionRequest() {
		}

		public CancelPartitionRequest(InputChannelID receiverId) {
			this.receiverId = receiverId;
		}

		@Override
		ByteBuf write(ByteBufAllocator allocator) throws Exception {
			ByteBuf result = null;

			try {
				result = allocateBuffer(allocator, ID, 16);
				receiverId.writeTo(result);
			}
			catch (Throwable t) {
				if (result != null) {
					result.release();
				}

				throw new IOException(t);
			}

			return result;
		}

		@Override
		void readFrom(ByteBuf buffer) throws Exception {
			receiverId = InputChannelID.fromByteBuf(buffer);
		}
	}

	static class CloseRequest extends NettyMessage {

		private static final byte ID = 5;

		public CloseRequest() {
		}

		@Override
		ByteBuf write(ByteBufAllocator allocator) throws Exception {
			return allocateBuffer(allocator, ID, 0);
		}

		@Override
		void readFrom(ByteBuf buffer) throws Exception {
		}
	}
}
