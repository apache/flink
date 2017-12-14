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

import org.apache.flink.runtime.event.TaskEvent;
import org.apache.flink.runtime.executiongraph.ExecutionAttemptID;
import org.apache.flink.runtime.io.network.api.serialization.EventSerializer;
import org.apache.flink.runtime.io.network.buffer.Buffer;
import org.apache.flink.runtime.io.network.partition.ResultPartitionID;
import org.apache.flink.runtime.io.network.partition.consumer.InputChannel;
import org.apache.flink.runtime.io.network.partition.consumer.InputChannelID;
import org.apache.flink.runtime.jobgraph.IntermediateResultPartitionID;

import org.apache.flink.shaded.netty4.io.netty.buffer.ByteBuf;
import org.apache.flink.shaded.netty4.io.netty.buffer.ByteBufAllocator;
import org.apache.flink.shaded.netty4.io.netty.buffer.ByteBufInputStream;
import org.apache.flink.shaded.netty4.io.netty.buffer.ByteBufOutputStream;
import org.apache.flink.shaded.netty4.io.netty.channel.ChannelHandler;
import org.apache.flink.shaded.netty4.io.netty.channel.ChannelHandlerContext;
import org.apache.flink.shaded.netty4.io.netty.channel.ChannelOutboundHandlerAdapter;
import org.apache.flink.shaded.netty4.io.netty.channel.ChannelPromise;
import org.apache.flink.shaded.netty4.io.netty.handler.codec.LengthFieldBasedFrameDecoder;
import org.apache.flink.shaded.netty4.io.netty.handler.codec.MessageToMessageDecoder;

import javax.annotation.Nullable;

import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.net.ProtocolException;
import java.nio.ByteBuffer;
import java.util.List;

import static org.apache.flink.util.Preconditions.checkArgument;
import static org.apache.flink.util.Preconditions.checkNotNull;

/**
 * A simple and generic interface to serialize messages to Netty's buffer space.
 *
 * <p>This class must be public as long as we are using a Netty version prior to 4.0.45. Please check FLINK-7845 for
 * more information.
 */
public abstract class NettyMessage {

	// ------------------------------------------------------------------------
	// Note: Every NettyMessage subtype needs to have a public 0-argument
	// constructor in order to work with the generic deserializer.
	// ------------------------------------------------------------------------

	static final int HEADER_LENGTH = 4 + 4 + 1; // frame length (4), magic number (4), msg ID (1)

	static final int MAGIC_NUMBER = 0xBADC0FFE;

	abstract ByteBuf write(ByteBufAllocator allocator) throws Exception;

	// ------------------------------------------------------------------------

	/**
	 * Allocates a new (header and contents) buffer and adds some header information for the frame
	 * decoder.
	 *
	 * <p>Before sending the buffer, you must write the actual length after adding the contents as
	 * an integer to position <tt>0</tt>!
	 *
	 * @param allocator
	 * 		byte buffer allocator to use
	 * @param id
	 * 		{@link NettyMessage} subclass ID
	 *
	 * @return a newly allocated direct buffer with header data written for {@link
	 * NettyMessageDecoder}
	 */
	private static ByteBuf allocateBuffer(ByteBufAllocator allocator, byte id) {
		return allocateBuffer(allocator, id, -1);
	}

	/**
	 * Allocates a new (header and contents) buffer and adds some header information for the frame
	 * decoder.
	 *
	 * <p>If the <tt>length</tt> is unknown, you must write the actual length after adding the
	 * contents as an integer to position <tt>0</tt>!
	 *
	 * @param allocator
	 * 		byte buffer allocator to use
	 * @param id
	 * 		{@link NettyMessage} subclass ID
	 * @param length
	 * 		content length (or <tt>-1</tt> if unknown)
	 *
	 * @return a newly allocated direct buffer with header data written for {@link
	 * NettyMessageDecoder}
	 */
	private static ByteBuf allocateBuffer(ByteBufAllocator allocator, byte id, int length) {
		checkArgument(length <= Integer.MAX_VALUE - HEADER_LENGTH);

		final ByteBuf buffer;
		if (length != -1) {
			buffer = allocator.directBuffer(HEADER_LENGTH + length);
		} else {
			// content length unknown -> start with the default initial size (rather than HEADER_LENGTH only):
			buffer = allocator.directBuffer();
		}

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

			final NettyMessage decodedMsg;
			switch (msgId) {
				case BufferResponse.ID:
					decodedMsg = BufferResponse.readFrom(msg);
					break;
				case PartitionRequest.ID:
					decodedMsg = PartitionRequest.readFrom(msg);
					break;
				case TaskEventRequest.ID:
					decodedMsg = TaskEventRequest.readFrom(msg, getClass().getClassLoader());
					break;
				case ErrorResponse.ID:
					decodedMsg = ErrorResponse.readFrom(msg);
					break;
				case CancelPartitionRequest.ID:
					decodedMsg = CancelPartitionRequest.readFrom(msg);
					break;
				case CloseRequest.ID:
					decodedMsg = CloseRequest.readFrom(msg);
					break;
				default:
					throw new ProtocolException("Received unknown message from producer: " + msg);
			}

			out.add(decodedMsg);
		}
	}

	// ------------------------------------------------------------------------
	// Server responses
	// ------------------------------------------------------------------------

	static class BufferResponse extends NettyMessage {

		private static final byte ID = 0;

		@Nullable
		final Buffer buffer;

		final InputChannelID receiverId;

		final int sequenceNumber;

		// ---- Deserialization -----------------------------------------------

		final boolean isBuffer;

		final int size;

		@Nullable
		ByteBuf retainedSlice;

		private BufferResponse(
				ByteBuf retainedSlice, boolean isBuffer, int sequenceNumber,
				InputChannelID receiverId) {
			// When deserializing we first have to request a buffer from the respective buffer
			// provider (at the handler) and copy the buffer from Netty's space to ours. Only
			// retainedSlice is set in this case.
			this.buffer = null;
			this.retainedSlice = checkNotNull(retainedSlice);
			this.size = retainedSlice.writerIndex();
			this.isBuffer = isBuffer;
			this.sequenceNumber = sequenceNumber;
			this.receiverId = checkNotNull(receiverId);
		}

		BufferResponse(Buffer buffer, int sequenceNumber, InputChannelID receiverId) {
			this.buffer = checkNotNull(buffer);
			this.retainedSlice = null;
			this.isBuffer = buffer.isBuffer();
			this.size = buffer.getSize();
			this.sequenceNumber = sequenceNumber;
			this.receiverId = checkNotNull(receiverId);
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
			checkNotNull(buffer, "No buffer instance to serialize.");

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
				buffer.recycle();
			}
		}

		static BufferResponse readFrom(ByteBuf buffer) {
			InputChannelID receiverId = InputChannelID.fromByteBuf(buffer);
			int sequenceNumber = buffer.readInt();
			boolean isBuffer = buffer.readBoolean();
			int size = buffer.readInt();

			ByteBuf retainedSlice = buffer.readSlice(size).retain();

			return new BufferResponse(retainedSlice, isBuffer, sequenceNumber, receiverId);
		}
	}

	static class ErrorResponse extends NettyMessage {

		private static final byte ID = 1;

		final Throwable cause;

		@Nullable
		final InputChannelID receiverId;

		ErrorResponse(Throwable cause) {
			this.cause = checkNotNull(cause);
			this.receiverId = null;
		}

		ErrorResponse(Throwable cause, InputChannelID receiverId) {
			this.cause = checkNotNull(cause);
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

		static ErrorResponse readFrom(ByteBuf buffer) throws Exception {
			try (ObjectInputStream ois = new ObjectInputStream(new ByteBufInputStream(buffer))) {
				Object obj = ois.readObject();

				if (!(obj instanceof Throwable)) {
					throw new ClassCastException("Read object expected to be of type Throwable, " +
							"actual type is " + obj.getClass() + ".");
				} else {
					if (buffer.readBoolean()) {
						InputChannelID receiverId = InputChannelID.fromByteBuf(buffer);
						return new ErrorResponse((Throwable) obj, receiverId);
					} else {
						return new ErrorResponse((Throwable) obj);
					}
				}
			}
		}
	}

	// ------------------------------------------------------------------------
	// Client requests
	// ------------------------------------------------------------------------

	static class PartitionRequest extends NettyMessage {

		private static final byte ID = 2;

		final ResultPartitionID partitionId;

		final int queueIndex;

		final InputChannelID receiverId;

		final int credit;

		PartitionRequest(ResultPartitionID partitionId, int queueIndex, InputChannelID receiverId, int credit) {
			this.partitionId = checkNotNull(partitionId);
			this.queueIndex = queueIndex;
			this.receiverId = checkNotNull(receiverId);
			this.credit = credit;
		}

		@Override
		ByteBuf write(ByteBufAllocator allocator) throws IOException {
			ByteBuf result = null;

			try {
				result = allocateBuffer(allocator, ID, 16 + 16 + 4 + 16 + 4);

				partitionId.getPartitionId().writeTo(result);
				partitionId.getProducerId().writeTo(result);
				result.writeInt(queueIndex);
				receiverId.writeTo(result);
				result.writeInt(credit);

				return result;
			}
			catch (Throwable t) {
				if (result != null) {
					result.release();
				}

				throw new IOException(t);
			}
		}

		static PartitionRequest readFrom(ByteBuf buffer) {
			ResultPartitionID partitionId =
				new ResultPartitionID(
					IntermediateResultPartitionID.fromByteBuf(buffer),
					ExecutionAttemptID.fromByteBuf(buffer));
			int queueIndex = buffer.readInt();
			InputChannelID receiverId = InputChannelID.fromByteBuf(buffer);
			int credit = buffer.readInt();

			return new PartitionRequest(partitionId, queueIndex, receiverId, credit);
		}

		@Override
		public String toString() {
			return String.format("PartitionRequest(%s:%d)", partitionId, queueIndex);
		}
	}

	static class TaskEventRequest extends NettyMessage {

		private static final byte ID = 3;

		final TaskEvent event;

		final InputChannelID receiverId;

		final ResultPartitionID partitionId;

		TaskEventRequest(TaskEvent event, ResultPartitionID partitionId, InputChannelID receiverId) {
			this.event = checkNotNull(event);
			this.receiverId = checkNotNull(receiverId);
			this.partitionId = checkNotNull(partitionId);
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

		static TaskEventRequest readFrom(ByteBuf buffer, ClassLoader classLoader) throws IOException {
			// directly deserialize fromNetty's buffer
			int length = buffer.readInt();
			ByteBuffer serializedEvent = buffer.nioBuffer(buffer.readerIndex(), length);
			// assume this event's content is read from the ByteBuf (positions are not shared!)
			buffer.readerIndex(buffer.readerIndex() + length);

			TaskEvent event =
				(TaskEvent) EventSerializer.fromSerializedEvent(serializedEvent, classLoader);

			ResultPartitionID partitionId =
				new ResultPartitionID(
					IntermediateResultPartitionID.fromByteBuf(buffer),
					ExecutionAttemptID.fromByteBuf(buffer));

			InputChannelID receiverId = InputChannelID.fromByteBuf(buffer);

			return new TaskEventRequest(event, partitionId, receiverId);
		}
	}

	/**
	 * Cancels the partition request of the {@link InputChannel} identified by
	 * {@link InputChannelID}.
	 *
	 * <p>There is a 1:1 mapping between the input channel and partition per physical channel.
	 * Therefore, the {@link InputChannelID} instance is enough to identify which request to cancel.
	 */
	static class CancelPartitionRequest extends NettyMessage {

		private static final byte ID = 4;

		final InputChannelID receiverId;

		CancelPartitionRequest(InputChannelID receiverId) {
			this.receiverId = checkNotNull(receiverId);
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

		static CancelPartitionRequest readFrom(ByteBuf buffer) throws Exception {
			return new CancelPartitionRequest(InputChannelID.fromByteBuf(buffer));
		}
	}

	static class CloseRequest extends NettyMessage {

		private static final byte ID = 5;

		CloseRequest() {
		}

		@Override
		ByteBuf write(ByteBufAllocator allocator) throws Exception {
			return allocateBuffer(allocator, ID, 0);
		}

		static CloseRequest readFrom(@SuppressWarnings("unused") ByteBuf buffer) throws Exception {
			return new CloseRequest();
		}
	}
}
