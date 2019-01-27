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

import org.apache.flink.annotation.VisibleForTesting;
import org.apache.flink.shaded.netty4.io.netty.buffer.ByteBuf;
import org.apache.flink.shaded.netty4.io.netty.buffer.Unpooled;
import org.apache.flink.shaded.netty4.io.netty.channel.ChannelHandlerContext;
import org.apache.flink.shaded.netty4.io.netty.channel.ChannelInboundHandlerAdapter;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.net.ProtocolException;

import static org.apache.flink.runtime.io.network.netty.NettyMessage.FRAME_HEADER_LENGTH;
import static org.apache.flink.runtime.io.network.netty.NettyMessage.MAGIC_NUMBER;
import static org.apache.flink.util.Preconditions.checkState;

/**
 * Decodes message from the netty buffer fragment.
 * This handler deals with both composing fragmentary buffers and decoding messages.
 *
 * Among all the messages, BufferResponse has the following format
 * +-----------------------------------+-----------------+
 * | FRAME_HEADER ||  MESSAGE_HEADER   |     BUFFER      |
 * +-----------------------------------+-----------------+
 *
 * and the other messages have the following format
 * +-----------------------------------+
 * | FRAME_HEADER ||  MESSAGE_HEADER   |
 * +-----------------------------------+
 *
 * The length of the frame header is fixed. Therefore, This handler first receives and
 * parses the frame header to acquire the message type and message length. If it is not
 * a BufferResponse, the length of the message header equals to the message length, and
 * otherwise the length of the message header is pre-defined. After the whole message header
 * is received, the message is decoded. Then if it is a BufferResponse, the buffer will
 * be received thereafter.
 *
 * The format of the frame header is
 * +------------------+------------------+--------++----------------+
 * | FRAME LENGTH (4) | MAGIC NUMBER (4) | ID (1) || CUSTOM MESSAGE |
 * +------------------+------------------+--------++----------------+
 */
public class ZeroCopyNettyMessageDecoder extends ChannelInboundHandlerAdapter {
	private static final Logger LOG = LoggerFactory.getLogger(ZeroCopyNettyMessageDecoder.class);

	private static final int INITIAL_MESSAGE_HEADER_BUFFER_LENGTH = 128;

	/** The allocator for the network buffer. */
	private final NetworkBufferAllocator networkBufferAllocator;

	/** The buffer used to receive the frame header part. **/
	private final ByteBuf frameHeaderBuffer;

	/** The buffer used to receive the message header part. **/
	private final ByteBuf messageHeaderBuffer;

	/** The header size of current message */
	private int remainingMessageHeaderToCopy = -1;

	/** The message type of current receiving message. **/
	private byte msgId = -1;

	/** The current receiving data buffer. **/
	private NettyMessage currentNettyMessage;

	/** The remaining bytes to discard before continuing decoding the next message. */
	private int remainingBufferSize = -1;

	ZeroCopyNettyMessageDecoder(NetworkBufferAllocator networkBufferAllocator) {
		this.networkBufferAllocator = networkBufferAllocator;

		this.frameHeaderBuffer = Unpooled.directBuffer(FRAME_HEADER_LENGTH, FRAME_HEADER_LENGTH);
		this.messageHeaderBuffer = Unpooled.directBuffer(INITIAL_MESSAGE_HEADER_BUFFER_LENGTH);
	}

	@Override
	public void channelRead(ChannelHandlerContext ctx, Object msg) throws Exception {
		if (!(msg instanceof ByteBuf)) {
			ctx.fireChannelRead(msg);
			return;
		}

		ByteBuf data = (ByteBuf) msg;

		try {
			while (data.readableBytes() > 0) {
				// if header is not full, fill in the header first.
				if (frameHeaderBuffer.isWritable()) {
					copyToTargetBuffer(data, frameHeaderBuffer, frameHeaderBuffer.writableBytes());

					if (!frameHeaderBuffer.isWritable()) {
						decodeFrameHeader();
					} else {
						break;
					}
				}

				// After fill up the header, fill in the message header. Since some messages like CloseRequest
				// has a zero-length message header, the messageHeaderBuffer will be un-writable directly, they
				// need to be processed specially.
				if (remainingMessageHeaderToCopy > 0
						|| (remainingMessageHeaderToCopy == 0 && messageHeaderBuffer.writerIndex() == 0)) {
					int messageHeaderReceivedSize = copyToTargetBuffer(data, messageHeaderBuffer, remainingMessageHeaderToCopy);
					remainingMessageHeaderToCopy -= messageHeaderReceivedSize;

					if (remainingMessageHeaderToCopy == 0) {
						currentNettyMessage = decodeNettyMessage();

						if (msgId != NettyMessage.BufferResponse.ID) {
							ctx.fireChannelRead(currentNettyMessage);
							clearState();
							continue;
						}
					} else {
						break;
					}
				}

				// If the message is a BufferResponse, read the data buffer.
				boolean dataBufferAllReceived = readOrDiscardBufferResponse(data);
				if (dataBufferAllReceived) {
					ctx.fireChannelRead(currentNettyMessage);
					clearState();
				}
			}

			checkState(!data.isReadable(), "Not all data of the received buffer consumed.");
		} finally {
			data.release();
		}
	}

	private void decodeFrameHeader() {
		int messageLength = frameHeaderBuffer.readInt();
		checkState(messageLength >= 0, "The length field of current message must be non-negative");

		int magicNumber = frameHeaderBuffer.readInt();
		checkState(magicNumber == MAGIC_NUMBER, "Network stream corrupted: received incorrect magic number.");

		msgId = frameHeaderBuffer.readByte();

		if (msgId != NettyMessage.BufferResponse.ID) {
			remainingMessageHeaderToCopy = messageLength - FRAME_HEADER_LENGTH;
		} else {
			remainingMessageHeaderToCopy = NettyMessage.BufferResponse.MESSAGE_HEADER_LENGTH;
		}

		if (messageHeaderBuffer.capacity() < remainingMessageHeaderToCopy) {
			messageHeaderBuffer.capacity(remainingMessageHeaderToCopy);
		}
	}

	private NettyMessage decodeNettyMessage() throws Exception {
		final NettyMessage decodedMsg;
		switch (msgId) {
			case NettyMessage.BufferResponse.ID:
				checkState(networkBufferAllocator != null,
					"buffer allocator is required to decode BufferResponse");
				decodedMsg = NettyMessage.BufferResponse.readFrom(messageHeaderBuffer, networkBufferAllocator);
				break;
			case NettyMessage.PartitionRequest.ID:
				decodedMsg = NettyMessage.PartitionRequest.readFrom(messageHeaderBuffer);
				break;
			case NettyMessage.TaskEventRequest.ID:
				decodedMsg = NettyMessage.TaskEventRequest.readFrom(messageHeaderBuffer, getClass().getClassLoader());
				break;
			case NettyMessage.ErrorResponse.ID:
				decodedMsg = NettyMessage.ErrorResponse.readFrom(messageHeaderBuffer);
				break;
			case NettyMessage.CancelPartitionRequest.ID:
				decodedMsg = NettyMessage.CancelPartitionRequest.readFrom(messageHeaderBuffer);
				break;
			case NettyMessage.CloseRequest.ID:
				decodedMsg = NettyMessage.CloseRequest.readFrom(messageHeaderBuffer);
				break;
			case NettyMessage.AddCredit.ID:
				decodedMsg = NettyMessage.AddCredit.readFrom(messageHeaderBuffer);
				break;
			default:
				throw new ProtocolException("Received unknown message from producer: " + messageHeaderBuffer);
		}

		return decodedMsg;
	}

	private boolean readOrDiscardBufferResponse(ByteBuf data) {
		checkState(currentNettyMessage != null && currentNettyMessage instanceof NettyMessage.BufferResponse);

		NettyMessage.BufferResponse bufferResponse = (NettyMessage.BufferResponse) currentNettyMessage;

		// If current buffer is empty, then no more data to receive
		if (bufferResponse.dataBufferSize == 0) {
			return true;
		}

		ByteBuf dataBuffer = bufferResponse.getBuffer();

		if (remainingBufferSize < 0) {
			remainingBufferSize = bufferResponse.dataBufferSize;
		}

		if (dataBuffer != null) {
			remainingBufferSize -= copyToTargetBuffer(data, dataBuffer, remainingBufferSize);
		} else {
			int actualBytesToDiscard = Math.min(data.readableBytes(), remainingBufferSize);
			data.readerIndex(data.readerIndex() + actualBytesToDiscard);
			remainingBufferSize -= actualBytesToDiscard;
		}

		return remainingBufferSize == 0;
	}

	/**
	 * Clears all the intermediate state of current handler for reading the next message.
	 */
	private void clearState() {
		frameHeaderBuffer.clear();
		messageHeaderBuffer.clear();
		remainingMessageHeaderToCopy = -1;
		msgId = -1;
		currentNettyMessage = null;
		remainingBufferSize = -1;
	}

	/**
	 * Copies bytes from the src to dest, but do not exceed the capacity of the dest buffer.
	 *
	 * @param src The ByteBuf to copy bytes from.
	 * @param dest The ByteBuf to copy bytes to.
	 * @param maxCopySize Maximum size of bytes to copy.
	 * @return The length of actually copied bytes.
	 */
	private int copyToTargetBuffer(ByteBuf src, ByteBuf dest, int maxCopySize) {
		int copyLength = Math.min(src.readableBytes(), maxCopySize);
		checkState(dest.writableBytes() >= copyLength,
			"There is not enough space to copy " + copyLength + " bytes, writable = " + dest.writableBytes());

		dest.writeBytes(src, copyLength);

		return copyLength;
	}

	/**
	 * Releases all pending resources when the handler exits.
	 *
	 * @param ctx the channel handler context.
	 */
	@Override
	public void channelInactive(ChannelHandlerContext ctx) throws Exception {
		LOG.info("Channel get inactive, currentNettyMessage = {}", currentNettyMessage);
		super.channelInactive(ctx);

		if (currentNettyMessage != null
				&& currentNettyMessage instanceof NettyMessage.BufferResponse
				&& ((NettyMessage.BufferResponse) currentNettyMessage).getBuffer() != null) {
			((NettyMessage.BufferResponse) currentNettyMessage).getBuffer().release();
		}

		currentNettyMessage = null;

		frameHeaderBuffer.release();
		messageHeaderBuffer.release();
	}

	@VisibleForTesting
	public NettyMessage getCurrentNettyMessage() {
		return currentNettyMessage;
	}
}
