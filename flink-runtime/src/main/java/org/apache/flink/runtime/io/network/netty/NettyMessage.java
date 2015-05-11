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
import org.apache.flink.api.java.typeutils.runtime.DataInputViewStream;
import org.apache.flink.api.java.typeutils.runtime.DataOutputViewStream;
import org.apache.flink.core.memory.DataInputView;
import org.apache.flink.core.memory.DataOutputView;
import org.apache.flink.runtime.event.task.TaskEvent;
import org.apache.flink.runtime.executiongraph.ExecutionAttemptID;
import org.apache.flink.runtime.io.network.api.serialization.EventSerializer;
import org.apache.flink.runtime.io.network.buffer.Buffer;
import org.apache.flink.runtime.io.network.partition.ResultPartitionID;
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
				try {
					ctx.write(((NettyMessage) msg).write(ctx.alloc()), promise);
				}
				catch (Throwable t) {
					throw new IOException("Error while serializing message: " + msg, t);
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
			else {
				throw new IllegalStateException("Received unknown message from producer: " + decodedMsg.getClass());
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

		BufferResponse(Buffer buffer, int sequenceNumber, InputChannelID receiverId) {
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

		Throwable error;

		InputChannelID receiverId;

		public ErrorResponse() {
		}

		ErrorResponse(Throwable error) {
			this.error = error;
		}

		ErrorResponse(Throwable error, InputChannelID receiverId) {
			this.error = error;
			this.receiverId = receiverId;
		}

		boolean isFatalError() {
			return receiverId == null;
		}

		@Override
		ByteBuf write(ByteBufAllocator allocator) throws IOException {
			ByteBuf result = null;

			ObjectOutputStream oos = null;

			try {
				result = allocateBuffer(allocator, ID);

				DataOutputView outputView = new ByteBufDataOutputView(result);

				oos = new ObjectOutputStream(new DataOutputViewStream(outputView));

				oos.writeObject(error);

				if (receiverId != null) {
					result.writeBoolean(true);
					receiverId.writeTo(result);
				} else {
					result.writeBoolean(false);
				}

				// Update frame length...
				result.setInt(0, result.readableBytes());
			}
			catch (Throwable t) {
				if (result != null) {
					result.release();
				}

				throw new IOException(t);
			} finally {
				if(oos != null) {
					oos.close();
				}
			}

			return result;
		}
		@Override
		void readFrom(ByteBuf buffer) throws Exception {
			DataInputView inputView = new ByteBufDataInputView(buffer);
			ObjectInputStream ois = null;

			try {
				ois = new ObjectInputStream(new DataInputViewStream(inputView));

				Object obj = ois.readObject();

				if (!(obj instanceof Throwable)) {
					throw new ClassCastException("Read object expected to be of type Throwable, " +
							"actual type is " + obj.getClass() + ".");
				} else {
					error = (Throwable) obj;

					if (buffer.readBoolean()) {
						receiverId = InputChannelID.fromByteBuf(buffer);
					}
				}
			} finally {
				if (ois != null) {
					ois.close();
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
		public void readFrom(ByteBuf buffer) {
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

	// ------------------------------------------------------------------------

	private static class ByteBufDataInputView implements DataInputView {

		private final ByteBufInputStream inputView;

		public ByteBufDataInputView(ByteBuf buffer) {
			this.inputView = new ByteBufInputStream(buffer);
		}

		@Override
		public void skipBytesToRead(int numBytes) throws IOException {
			throw new UnsupportedOperationException();
		}

		@Override
		public int read(byte[] b, int off, int len) throws IOException {
			return inputView.read(b, off, len);
		}

		@Override
		public int read(byte[] b) throws IOException {
			return inputView.read(b);
		}

		@Override
		public void readFully(byte[] b) throws IOException {
			inputView.readFully(b);
		}

		@Override
		public void readFully(byte[] b, int off, int len) throws IOException {
			inputView.readFully(b, off, len);
		}

		@Override
		public int skipBytes(int n) throws IOException {
			return inputView.skipBytes(n);
		}

		@Override
		public boolean readBoolean() throws IOException {
			return inputView.readBoolean();
		}

		@Override
		public byte readByte() throws IOException {
			return inputView.readByte();
		}

		@Override
		public int readUnsignedByte() throws IOException {
			return inputView.readUnsignedByte();
		}

		@Override
		public short readShort() throws IOException {
			return inputView.readShort();
		}

		@Override
		public int readUnsignedShort() throws IOException {
			return inputView.readUnsignedShort();
		}

		@Override
		public char readChar() throws IOException {
			return inputView.readChar();
		}

		@Override
		public int readInt() throws IOException {
			return inputView.readInt();
		}

		@Override
		public long readLong() throws IOException {
			return inputView.readLong();
		}

		@Override
		public float readFloat() throws IOException {
			return inputView.readFloat();
		}

		@Override
		public double readDouble() throws IOException {
			return inputView.readDouble();
		}

		@Override
		public String readLine() throws IOException {
			return inputView.readLine();
		}

		@Override
		public String readUTF() throws IOException {
			return inputView.readUTF();
		}
	}

	private static class ByteBufDataOutputView implements DataOutputView {

		private final ByteBufOutputStream outputView;

		public ByteBufDataOutputView(ByteBuf buffer) {
			this.outputView = new ByteBufOutputStream(buffer);
		}

		@Override
		public void skipBytesToWrite(int numBytes) throws IOException {
			throw new UnsupportedOperationException();
		}

		@Override
		public void write(DataInputView source, int numBytes) throws IOException {
			throw new UnsupportedOperationException();
		}

		@Override
		public void write(int b) throws IOException {
			outputView.write(b);
		}

		@Override
		public void write(byte[] b) throws IOException {
			outputView.write(b);
		}

		@Override
		public void write(byte[] b, int off, int len) throws IOException {
			outputView.write(b, off, len);
		}

		@Override
		public void writeBoolean(boolean v) throws IOException {
			outputView.writeBoolean(v);
		}

		@Override
		public void writeByte(int v) throws IOException {
			outputView.writeByte(v);
		}

		@Override
		public void writeShort(int v) throws IOException {
			outputView.writeShort(v);
		}

		@Override
		public void writeChar(int v) throws IOException {
			outputView.writeChar(v);
		}

		@Override
		public void writeInt(int v) throws IOException {
			outputView.writeInt(v);
		}

		@Override
		public void writeLong(long v) throws IOException {
			outputView.writeLong(v);
		}

		@Override
		public void writeFloat(float v) throws IOException {
			outputView.writeFloat(v);
		}

		@Override
		public void writeDouble(double v) throws IOException {
			outputView.writeDouble(v);
		}

		@Override
		public void writeBytes(String s) throws IOException {
			outputView.writeBytes(s);
		}

		@Override
		public void writeChars(String s) throws IOException {
			outputView.writeChars(s);
		}

		@Override
		public void writeUTF(String s) throws IOException {
			outputView.writeUTF(s);
		}
	}
}
