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
import org.apache.flink.util.ExceptionUtils;

import org.apache.flink.shaded.netty4.io.netty.buffer.ByteBuf;
import org.apache.flink.shaded.netty4.io.netty.buffer.ByteBufAllocator;
import org.apache.flink.shaded.netty4.io.netty.buffer.ByteBufInputStream;
import org.apache.flink.shaded.netty4.io.netty.buffer.ByteBufOutputStream;
import org.apache.flink.shaded.netty4.io.netty.buffer.CompositeByteBuf;
import org.apache.flink.shaded.netty4.io.netty.channel.ChannelHandler;
import org.apache.flink.shaded.netty4.io.netty.channel.ChannelHandlerContext;
import org.apache.flink.shaded.netty4.io.netty.channel.ChannelOutboundHandlerAdapter;
import org.apache.flink.shaded.netty4.io.netty.channel.ChannelPromise;
import org.apache.flink.shaded.netty4.io.netty.handler.codec.LengthFieldBasedFrameDecoder;

import javax.annotation.Nullable;

import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.net.ProtocolException;
import java.nio.ByteBuffer;

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

	static final int FRAME_HEADER_LENGTH = 4 + 4 + 1; // frame length (4), magic number (4), msg ID (1)

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
	 * NettyMessageEncoder}
	 */
	private static ByteBuf allocateBuffer(ByteBufAllocator allocator, byte id) {
		return allocateBuffer(allocator, id, -1);
	}

	/**
	 * Allocates a new (header and contents) buffer and adds some header information for the frame
	 * decoder.
	 *
	 * <p>If the <tt>contentLength</tt> is unknown, you must write the actual length after adding
	 * the contents as an integer to position <tt>0</tt>!
	 *
	 * @param allocator
	 * 		byte buffer allocator to use
	 * @param id
	 * 		{@link NettyMessage} subclass ID
	 * @param contentLength
	 * 		content length (or <tt>-1</tt> if unknown)
	 *
	 * @return a newly allocated direct buffer with header data written for {@link
	 * NettyMessageEncoder}
	 */
	private static ByteBuf allocateBuffer(ByteBufAllocator allocator, byte id, int contentLength) {
		return allocateBuffer(allocator, id, 0, contentLength, true);
	}

	/**
	 * Allocates a new buffer and adds some header information for the frame decoder.
	 *
	 * <p>If the <tt>contentLength</tt> is unknown, you must write the actual length after adding
	 * the contents as an integer to position <tt>0</tt>!
	 *
	 * @param allocator
	 * 		byte buffer allocator to use
	 * @param id
	 * 		{@link NettyMessage} subclass ID
	 * @param messageHeaderLength
	 * 		additional header length that should be part of the allocated buffer and is written
	 * 		outside of this method
	 * @param contentLength
	 * 		content length (or <tt>-1</tt> if unknown)
	 * @param allocateForContent
	 * 		whether to make room for the actual content in the buffer (<tt>true</tt>) or whether to
	 * 		only return a buffer with the header information (<tt>false</tt>)
	 *
	 * @return a newly allocated direct buffer with header data written for {@link
	 * NettyMessageEncoder}
	 */
	private static ByteBuf allocateBuffer(
			ByteBufAllocator allocator,
			byte id,
			int messageHeaderLength,
			int contentLength,
			boolean allocateForContent) {
		checkArgument(contentLength <= Integer.MAX_VALUE - FRAME_HEADER_LENGTH);

		final ByteBuf buffer;
		if (!allocateForContent) {
			buffer = allocator.directBuffer(FRAME_HEADER_LENGTH + messageHeaderLength);
		} else if (contentLength != -1) {
			buffer = allocator.directBuffer(FRAME_HEADER_LENGTH + messageHeaderLength + contentLength);
		} else {
			// content length unknown -> start with the default initial size (rather than FRAME_HEADER_LENGTH only):
			buffer = allocator.directBuffer();
		}
		buffer.writeInt(FRAME_HEADER_LENGTH + messageHeaderLength + contentLength); // may be updated later, e.g. if contentLength == -1
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
	}

	/**
	 * Message decoder based on netty's {@link LengthFieldBasedFrameDecoder} but avoiding the
	 * additional memory copy inside {@link #extractFrame(ChannelHandlerContext, ByteBuf, int, int)}
	 * since we completely decode the {@link ByteBuf} inside {@link #decode(ChannelHandlerContext,
	 * ByteBuf)} and will not re-use it afterwards.
	 *
	 * <p>The frame-length encoder will be based on this transmission scheme created by {@link NettyMessage#allocateBuffer(ByteBufAllocator, byte, int)}:
	 * <pre>
	 * +------------------+------------------+--------++----------------+
	 * | FRAME LENGTH (4) | MAGIC NUMBER (4) | ID (1) || CUSTOM MESSAGE |
	 * +------------------+------------------+--------++----------------+
	 * </pre>
	 */
	static class NettyMessageDecoder extends LengthFieldBasedFrameDecoder {
		/**
		 * Creates a new message decoded with the required frame properties.
		 */
		NettyMessageDecoder() {
			super(Integer.MAX_VALUE, 0, 4, -4, 4);
		}

		@Override
		protected Object decode(ChannelHandlerContext ctx, ByteBuf in) throws Exception {
			ByteBuf msg = (ByteBuf) super.decode(ctx, in);
			if (msg == null) {
				return null;
			}

			try {
				int magicNumber = msg.readInt();

				if (magicNumber != MAGIC_NUMBER) {
					throw new IllegalStateException(
						"Network stream corrupted: received incorrect magic number.");
				}

				byte msgId = msg.readByte();

				final NettyMessage decodedMsg;
				switch (msgId) {
					case PartitionRequest.ID:
						decodedMsg = PartitionRequest.readFrom(msg);
						break;
					case TaskEventRequest.ID:
						decodedMsg = TaskEventRequest.readFrom(msg, getClass().getClassLoader());
						break;
					case CancelPartitionRequest.ID:
						decodedMsg = CancelPartitionRequest.readFrom(msg);
						break;
					case CloseRequest.ID:
						decodedMsg = CloseRequest.readFrom(msg);
						break;
					case AddCredit.ID:
						decodedMsg = AddCredit.readFrom(msg);
						break;
					case ResumeConsumption.ID:
						decodedMsg = ResumeConsumption.readFrom(msg);
						break;
					default:
						throw new ProtocolException(
							"Received unknown message from producer: " + msg);
				}

				return decodedMsg;
			} finally {
				// ByteToMessageDecoder cleanup (only the BufferResponse holds on to the decoded
				// msg but already retain()s the buffer once)
				msg.release();
			}
		}
	}

	// ------------------------------------------------------------------------
	// Server responses
	// ------------------------------------------------------------------------

	static class BufferResponse extends NettyMessage {

		static final byte ID = 0;

		// receiver ID (16), sequence number (4), backlog (4), dataType (1), isCompressed (1), buffer size (4)
		static final int MESSAGE_HEADER_LENGTH = 16 + 4 + 4 + 1 + 1 + 4;

		final Buffer buffer;

		final InputChannelID receiverId;

		final int sequenceNumber;

		final int backlog;

		final Buffer.DataType dataType;

		final boolean isCompressed;

		final int bufferSize;

		private BufferResponse(
				@Nullable Buffer buffer,
				Buffer.DataType dataType,
				boolean isCompressed,
				int sequenceNumber,
				InputChannelID receiverId,
				int backlog,
				int bufferSize) {
			this.buffer = buffer;
			this.dataType = dataType;
			this.isCompressed = isCompressed;
			this.sequenceNumber = sequenceNumber;
			this.receiverId = checkNotNull(receiverId);
			this.backlog = backlog;
			this.bufferSize = bufferSize;
		}

		BufferResponse(
				Buffer buffer,
				int sequenceNumber,
				InputChannelID receiverId,
				int backlog) {
			this.buffer = checkNotNull(buffer);
			checkArgument(buffer.getDataType().ordinal() <= Byte.MAX_VALUE, "Too many data types defined!");
			this.dataType = buffer.getDataType();
			this.isCompressed = buffer.isCompressed();
			this.sequenceNumber = sequenceNumber;
			this.receiverId = checkNotNull(receiverId);
			this.backlog = backlog;
			this.bufferSize = buffer.getSize();
		}

		boolean isBuffer() {
			return dataType.isBuffer();
		}

		@Nullable
		public Buffer getBuffer() {
			return buffer;
		}

		void releaseBuffer() {
			if (buffer != null) {
				buffer.recycleBuffer();
			}
		}

		// --------------------------------------------------------------------
		// Serialization
		// --------------------------------------------------------------------

		@Override
		ByteBuf write(ByteBufAllocator allocator) throws IOException {
			ByteBuf headerBuf = null;
			try {
				// in order to forward the buffer to netty, it needs an allocator set
				buffer.setAllocator(allocator);

				// only allocate header buffer - we will combine it with the data buffer below
				headerBuf = allocateBuffer(allocator, ID, MESSAGE_HEADER_LENGTH, bufferSize, false);

				receiverId.writeTo(headerBuf);
				headerBuf.writeInt(sequenceNumber);
				headerBuf.writeInt(backlog);
				headerBuf.writeByte(dataType.ordinal());
				headerBuf.writeBoolean(isCompressed);
				headerBuf.writeInt(buffer.readableBytes());

				CompositeByteBuf composityBuf = allocator.compositeDirectBuffer();
				composityBuf.addComponent(headerBuf);
				composityBuf.addComponent(buffer.asByteBuf());
				// update writer index since we have data written to the components:
				composityBuf.writerIndex(headerBuf.writerIndex() + buffer.asByteBuf().writerIndex());
				return composityBuf;
			}
			catch (Throwable t) {
				if (headerBuf != null) {
					headerBuf.release();
				}
				buffer.recycleBuffer();

				ExceptionUtils.rethrowIOException(t);
				return null; // silence the compiler
			}
		}

		/**
		 * Parses the message header part and composes a new BufferResponse with an empty data buffer. The
		 * data buffer will be filled in later.
		 *
		 * @param messageHeader the serialized message header.
		 * @param bufferAllocator the allocator for network buffer.
		 * @return a BufferResponse object with the header parsed and the data buffer to fill in later. The
		 *			data buffer will be null if the target channel has been released or the buffer size is 0.
		 */
		static BufferResponse readFrom(ByteBuf messageHeader, NetworkBufferAllocator bufferAllocator) {
			InputChannelID receiverId = InputChannelID.fromByteBuf(messageHeader);
			int sequenceNumber = messageHeader.readInt();
			int backlog = messageHeader.readInt();
			Buffer.DataType dataType = Buffer.DataType.values()[messageHeader.readByte()];
			boolean isCompressed = messageHeader.readBoolean();
			int size = messageHeader.readInt();

			Buffer dataBuffer = null;

			if (size != 0) {
				if (dataType.isBuffer()) {
					dataBuffer = bufferAllocator.allocatePooledNetworkBuffer(receiverId);
				} else {
					dataBuffer = bufferAllocator.allocateUnPooledNetworkBuffer(size, dataType);
				}
			}

			if (dataBuffer != null) {
				dataBuffer.setCompressed(isCompressed);
			}

			return new BufferResponse(
				dataBuffer,
				dataType,
				isCompressed,
				sequenceNumber,
				receiverId,
				backlog,
				size);
		}
	}

	static class ErrorResponse extends NettyMessage {

		static final byte ID = 1;

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
				result = allocateBuffer(allocator, ID, 20 + 16 + 4 + 16 + 4);

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
			return String.format("PartitionRequest(%s:%d:%d)", partitionId, queueIndex, credit);
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

				result = allocateBuffer(allocator, ID, 4 + serializedEvent.remaining() + 20 + 16 + 16);

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

	/**
	 * Incremental credit announcement from the client to the server.
	 */
	static class AddCredit extends NettyMessage {

		private static final byte ID = 6;

		final int credit;

		final InputChannelID receiverId;

		AddCredit(int credit, InputChannelID receiverId) {
			checkArgument(credit > 0, "The announced credit should be greater than 0");
			this.credit = credit;
			this.receiverId = receiverId;
		}

		@Override
		ByteBuf write(ByteBufAllocator allocator) throws IOException {
			ByteBuf result = null;

			try {
				result = allocateBuffer(allocator, ID, 4 + 16);
				result.writeInt(credit);
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

		static AddCredit readFrom(ByteBuf buffer) {
			int credit = buffer.readInt();
			InputChannelID receiverId = InputChannelID.fromByteBuf(buffer);

			return new AddCredit(credit, receiverId);
		}

		@Override
		public String toString() {
			return String.format("AddCredit(%s : %d)", receiverId, credit);
		}
	}

	/**
	 * Message to notify the producer to unblock from checkpoint.
	 */
	static class ResumeConsumption extends NettyMessage {

		private static final byte ID = 7;

		final InputChannelID receiverId;

		ResumeConsumption(InputChannelID receiverId) {
			this.receiverId = receiverId;
		}

		@Override
		ByteBuf write(ByteBufAllocator allocator) throws IOException {
			ByteBuf result = null;

			try {
				result = allocateBuffer(allocator, ID, 16);
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

		static ResumeConsumption readFrom(ByteBuf buffer) {
			return new ResumeConsumption(InputChannelID.fromByteBuf(buffer));
		}

		@Override
		public String toString() {
			return String.format("ResumeConsumption(%s)", receiverId);
		}
	}
}
