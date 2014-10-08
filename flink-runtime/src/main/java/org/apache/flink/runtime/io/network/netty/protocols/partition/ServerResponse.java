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

package org.apache.flink.runtime.io.network.netty.protocols.partition;

import io.netty.buffer.ByteBuf;
import io.netty.channel.ChannelHandlerContext;
import io.netty.handler.codec.MessageToMessageDecoder;
import org.apache.flink.runtime.io.network.BufferOrEvent;
import org.apache.flink.runtime.io.network.partition.consumer.InputChannelID;
import org.apache.flink.runtime.util.SerializationUtil;

import java.nio.ByteBuffer;
import java.util.List;

import static com.google.common.base.Preconditions.checkNotNull;
import static io.netty.channel.ChannelHandler.Sharable;
import static org.apache.flink.runtime.io.network.BufferOrEvent.BufferOrEventType;

@Sharable
interface ServerResponse<T> extends NettyMessage<T> {

	static class ServerResponseDecoder extends MessageToMessageDecoder<ByteBuf> {

		@Override
		protected void decode(ChannelHandlerContext ctx, ByteBuf msg, List<Object> out) throws Exception {
			int magicNumber = msg.readInt();

			if (magicNumber != NettyMessageEncoder.MAGIC_NUMBER) {
				// ERROR case
				return;
			}

			byte msgId = msg.readByte();

			if (msgId == BufferOrEventResponse.ID) {
				out.add(BufferOrEventResponse.readFrom(msg));
			}
		}
	}

	static class BufferOrEventResponse implements ServerResponse<BufferOrEventResponse> {

		private static final byte ID = 0;

		private final int sequenceNumber;

		private final InputChannelID receiverId;

		private final BufferOrEventType bufferOrEventType;

		private final ByteBuffer bufferOrEvent;

		private final ByteBuf nettyBufferOrEvent;

		public BufferOrEventResponse(int sequenceNumber, InputChannelID receiverId, BufferOrEvent bufferOrEvent) {
			this.sequenceNumber = sequenceNumber;
			this.receiverId = receiverId;

			if (bufferOrEvent.isBuffer()) {
				this.bufferOrEventType = BufferOrEventType.BUFFER;
				this.bufferOrEvent = bufferOrEvent.getBuffer().getNioBuffer();
			}
			else if (bufferOrEvent.isEvent()) {
				this.bufferOrEventType = BufferOrEventType.EVENT;
				this.bufferOrEvent = SerializationUtil.toSerializedEvent(bufferOrEvent.getEvent());
			}
			else {
				throw new IllegalStateException("BufferOrEvent instance without buffer and event.");
			}

			nettyBufferOrEvent = null;
		}

		// Private deserialization constructor
		private BufferOrEventResponse(int sequenceNumber, InputChannelID receiverId, BufferOrEventType bufferOrEventType, ByteBuf bufferOrEvent) {
			this.sequenceNumber = sequenceNumber;
			this.receiverId = receiverId;
			this.bufferOrEventType = bufferOrEventType;
			this.nettyBufferOrEvent = bufferOrEvent;
			this.bufferOrEvent = null;
		}

		int getSequenceNumber() {
			return sequenceNumber;
		}

		InputChannelID getReceiverId() {
			return receiverId;
		}

		boolean isBuffer() {
			return bufferOrEventType == BufferOrEventType.BUFFER;
		}

		boolean isEvent() {
			return bufferOrEventType == BufferOrEventType.EVENT;
		}

		int getSize() {
			return checkNotNull(nettyBufferOrEvent).readableBytes();
		}

		void copyNettyBufferTo(ByteBuffer buffer) {
			nettyBufferOrEvent.readBytes(buffer);
		}

		void release() {
			nettyBufferOrEvent.release();
		}

		// --------------------------------------------------------------------
		// Serialization
		// --------------------------------------------------------------------

		@Override
		public byte getId() {
			return 0;
		}

		@Override
		public int getLength() {
			return 4 + 16 + 4 + 4 + bufferOrEvent.remaining();
		}

		@Override
		public void writeTo(ByteBuf outboundBuffer) {
			outboundBuffer.writeInt(sequenceNumber);
			receiverId.writeTo(outboundBuffer);
			outboundBuffer.writeInt(bufferOrEventType.ordinal());
			outboundBuffer.writeInt(bufferOrEvent.remaining());
			outboundBuffer.writeBytes(bufferOrEvent);
		}


		public static BufferOrEventResponse readFrom(ByteBuf inboundBuffer) {
			int sequenceNumber = inboundBuffer.readInt();
			InputChannelID receiverId = new InputChannelID(inboundBuffer.readLong(), inboundBuffer.readLong());
			BufferOrEventType type = BufferOrEventType.values()[inboundBuffer.readInt()];
			int length = inboundBuffer.readInt();
			ByteBuf bufferOrEvent = inboundBuffer.readSlice(length);

			return new BufferOrEventResponse(sequenceNumber, receiverId, type, bufferOrEvent);
		}
	}
}
