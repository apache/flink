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
	 * NettyMessageDecoder}
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
	 * NettyMessageDecoder}
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
	 * NettyMessageDecoder}
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
		private final boolean restoreOldNettyBehaviour;
		private final NoDataBufferMessageParser noDataBufferMessageParser;

		/**
		 * Creates a new message decoded with the required frame properties.
		 *
		 * @param restoreOldNettyBehaviour
		 * 		restore Netty 4.0.27 code in {@link LengthFieldBasedFrameDecoder#extractFrame} to
		 * 		copy instead of slicing the buffer
		 */
		NettyMessageDecoder(boolean restoreOldNettyBehaviour) {
			super(Integer.MAX_VALUE, 0, 4, -4, 4);
			this.restoreOldNettyBehaviour = restoreOldNettyBehaviour;
			this.noDataBufferMessageParser = new NoDataBufferMessageParser();
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

				if (msgId == BufferResponse.ID) {
					decodedMsg = BufferResponse.readFrom(msg);
				} else {
					decodedMsg = noDataBufferMessageParser.parseMessageHeader(msgId, msg).getParsedMessage();
				}

				return decodedMsg;
			} finally {
				// ByteToMessageDecoder cleanup (only the BufferResponse holds on to the decoded
				// msg but already retain()s the buffer once)
				msg.release();
			}
		}

		@Override
		protected ByteBuf extractFrame(ChannelHandlerContext ctx, ByteBuf buffer, int index, int length) {
			if (restoreOldNettyBehaviour) {
				/*
				 * For non-credit based code paths with Netty >= 4.0.28.Final:
				 * These versions contain an improvement by Netty, which slices a Netty buffer
				 * instead of doing a memory copy [1] in the
				 * LengthFieldBasedFrameDecoder. In some situations, this
				 * interacts badly with our Netty pipeline leading to OutOfMemory
				 * errors.
				 *
				 * [1] https://github.com/netty/netty/issues/3704
				 *
				 * TODO: remove along with the non-credit based fallback protocol
				 */
				ByteBuf frame = ctx.alloc().buffer(length);
				frame.writeBytes(buffer, index, length);
				return frame;
			} else {
				return super.extractFrame(ctx, buffer, index, length);
			}
		}
	}

	/**
	 * Holds a buffer object who has the view of netty {@link ByteBuf}.
	 */
	interface BufferHolder<T> {
		/**
		 * Gets the original buffer object.
		 *
		 * @return the original buffer object.
		 */
		T getBuffer();

		/**
		 * Gets the view that casts the buffer object as a netty ByteBuf.
		 *
		 * @return the view of the buffer object as a netty ByteBuf.
		 */
		ByteBuf asByteBuf();

		/**
		 * Notification of the buffer object is going to be written.
		 *
		 * @param allocator the ByteBuf allocator of current netty pipeline.
		 */
		void onWrite(ByteBufAllocator allocator);

		/**
		 * Releases the underlying buffer object.
		 */
		void release();
	}

	/**
	 * The buffer holder to hold a netty {@link ByteBuf}.
	 */
	static class NettyBufferHolder implements BufferHolder<ByteBuf> {
		private ByteBuf byteBuf;

		NettyBufferHolder(@Nullable ByteBuf byteBuf) {
			this.byteBuf = byteBuf;
		}

		@Override
		@Nullable
		public ByteBuf getBuffer() {
			return byteBuf;
		}

		@Override
		public ByteBuf asByteBuf() {
			return byteBuf;
		}

		@Override
		public void onWrite(ByteBufAllocator allocator) {
			// No operations.
		}

		@Override
		public void release() {
			byteBuf.release();
		}
	}

	/**
	 * The buffer holder to hold a flink {@link Buffer}.
	 */
	static class FlinkBufferHolder implements BufferHolder<Buffer> {
		private Buffer buffer;

		FlinkBufferHolder(@Nullable Buffer buffer) {
			this.buffer = buffer;
		}

		@Override
		@Nullable
		public Buffer getBuffer() {
			return buffer;
		}

		@Override
		public ByteBuf asByteBuf() {
			return buffer == null ? null : buffer.asByteBuf();
		}

		@Override
		public void onWrite(ByteBufAllocator allocator) {
			// in order to forward the buffer to netty, it needs an allocator set
			buffer.setAllocator(allocator);
		}

		@Override
		public void release() {
			buffer.recycleBuffer();
		}
	}

	// ------------------------------------------------------------------------
	// Server responses
	// ------------------------------------------------------------------------

	static class BufferResponse<T> extends NettyMessage {

		static final byte ID = 0;

		// receiver ID (16), sequence number (4), backlog (4), isBuffer (1), buffer size (4)
		static final int MESSAGE_HEADER_LENGTH = 16 + 4 + 4 + 1 + 4;

		final BufferHolder<T> bufferHolder;

		final InputChannelID receiverId;

		final int sequenceNumber;

		final int backlog;

		final boolean isBuffer;

		final int bufferSize;

		BufferResponse(
				BufferHolder<T> bufferHolder,
				boolean isBuffer,
				int sequenceNumber,
				InputChannelID receiverId,
				int backlog) {
			this.bufferHolder = checkNotNull(bufferHolder);
			checkNotNull(bufferHolder.getBuffer());

			this.isBuffer = isBuffer;
			this.sequenceNumber = sequenceNumber;
			this.receiverId = checkNotNull(receiverId);
			this.backlog = backlog;
			this.bufferSize = bufferHolder.asByteBuf().readableBytes();
		}

		private BufferResponse(
				BufferHolder<T> bufferHolder,
				boolean isBuffer,
				int sequenceNumber,
				InputChannelID receiverId,
				int backlog,
				int bufferSize) {
			this.bufferHolder = checkNotNull(bufferHolder);

			this.isBuffer = isBuffer;
			this.sequenceNumber = sequenceNumber;
			this.receiverId = checkNotNull(receiverId);
			this.backlog = backlog;
			this.bufferSize = bufferSize;
		}

		boolean isBuffer() {
			return isBuffer;
		}

		T getBuffer() {
			return bufferHolder.getBuffer();
		}

		ByteBuf asByteBuf() {
			return bufferHolder.asByteBuf();
		}

		public int getBufferSize() {
			return bufferSize;
		}

		void releaseBuffer() {
			bufferHolder.release();
		}

		// --------------------------------------------------------------------
		// Serialization
		// --------------------------------------------------------------------

		@Override
		ByteBuf write(ByteBufAllocator allocator) throws IOException {
			ByteBuf headerBuf = null;
			try {
				bufferHolder.onWrite(allocator);

				// only allocate header buffer - we will combine it with the data buffer below
				headerBuf = allocateBuffer(allocator, ID, MESSAGE_HEADER_LENGTH, bufferSize, false);

				receiverId.writeTo(headerBuf);
				headerBuf.writeInt(sequenceNumber);
				headerBuf.writeInt(backlog);
				headerBuf.writeBoolean(isBuffer);
				headerBuf.writeInt(bufferSize);

				CompositeByteBuf composityBuf = allocator.compositeDirectBuffer();
				composityBuf.addComponent(headerBuf);
				composityBuf.addComponent(bufferHolder.asByteBuf());
				// update writer index since we have data written to the components:
				composityBuf.writerIndex(headerBuf.writerIndex() + bufferHolder.asByteBuf().writerIndex());
				return composityBuf;
			}
			catch (Throwable t) {
				if (headerBuf != null) {
					headerBuf.release();
				}
				bufferHolder.release();

				ExceptionUtils.rethrowIOException(t);
				return null; // silence the compiler
			}
		}

		/**
		 * Parses the whole BufferResponse message and composes a new BufferResponse with both header parsed and
		 * data buffer filled in. This method is used in non-credit-based network stack.
		 *
		 * @param buffer the whole serialized BufferResponse message.
		 * @return a BufferResponse object with the header parsed and the data buffer filled in.
		 */
		static BufferResponse<ByteBuf> readFrom(ByteBuf buffer) {
			InputChannelID receiverId = InputChannelID.fromByteBuf(buffer);
			int sequenceNumber = buffer.readInt();
			int backlog = buffer.readInt();
			boolean isBuffer = buffer.readBoolean();
			int size = buffer.readInt();

			ByteBuf retainedSlice = buffer.readSlice(size).retain();
			return new BufferResponse<>(
					new NettyBufferHolder(retainedSlice),
					isBuffer,
					sequenceNumber,
					receiverId,
					backlog,
					size);
		}

		/**
		 * Parses the message header part and composes a new BufferResponse with an empty data buffer. The
		 * data buffer will be filled in later. This method is used in credit-based network stack.
		 *
		 * @param messageHeader the serialized message header.
		 * @param bufferAllocator the allocator for network buffer.
		 * @return a BufferResponse object with the header parsed and the data buffer to fill in later.
		 */
		static BufferResponse<Buffer> readFrom(ByteBuf messageHeader, NetworkBufferAllocator bufferAllocator) {
			InputChannelID receiverId = InputChannelID.fromByteBuf(messageHeader);
			int sequenceNumber = messageHeader.readInt();
			int backlog = messageHeader.readInt();
			boolean isBuffer = messageHeader.readBoolean();
			int size = messageHeader.readInt();

			Buffer dataBuffer = null;
			if (size != 0) {
				if (isBuffer) {
					dataBuffer = bufferAllocator.allocatePooledNetworkBuffer(receiverId, size);
				} else {
					dataBuffer = bufferAllocator.allocateUnPooledNetworkBuffer(size);
				}
			}

			return new BufferResponse<>(
					new FlinkBufferHolder(dataBuffer),
					isBuffer,
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

		static final byte ID = 2;

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
			return String.format("PartitionRequest(%s:%d:%d)", partitionId, queueIndex, credit);
		}
	}

	static class TaskEventRequest extends NettyMessage {

		static final byte ID = 3;

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

		static final byte ID = 4;

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

		static final byte ID = 5;

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

		static final byte ID = 6;

		final ResultPartitionID partitionId;

		final int credit;

		final InputChannelID receiverId;

		AddCredit(ResultPartitionID partitionId, int credit, InputChannelID receiverId) {
			checkArgument(credit > 0, "The announced credit should be greater than 0");

			this.partitionId = partitionId;
			this.credit = credit;
			this.receiverId = receiverId;
		}

		@Override
		ByteBuf write(ByteBufAllocator allocator) throws IOException {
			ByteBuf result = null;

			try {
				result = allocateBuffer(allocator, ID, 16 + 16 + 4 + 16);

				partitionId.getPartitionId().writeTo(result);
				partitionId.getProducerId().writeTo(result);
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
			ResultPartitionID partitionId =
				new ResultPartitionID(
					IntermediateResultPartitionID.fromByteBuf(buffer),
					ExecutionAttemptID.fromByteBuf(buffer));
			int credit = buffer.readInt();
			InputChannelID receiverId = InputChannelID.fromByteBuf(buffer);

			return new AddCredit(partitionId, credit, receiverId);
		}

		@Override
		public String toString() {
			return String.format("AddCredit(%s : %d)", receiverId, credit);
		}
	}
}
