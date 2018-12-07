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

import org.apache.flink.runtime.io.network.buffer.Buffer;
import org.apache.flink.shaded.netty4.io.netty.buffer.ByteBuf;
import org.apache.flink.shaded.netty4.io.netty.channel.ChannelHandlerContext;
import org.apache.flink.shaded.netty4.io.netty.channel.ChannelInboundHandlerAdapter;

import static org.apache.flink.runtime.io.network.netty.NettyMessage.FRAME_HEADER_LENGTH;
import static org.apache.flink.runtime.io.network.netty.NettyMessage.MAGIC_NUMBER;
import static org.apache.flink.util.Preconditions.checkState;

/**
 * Decodes messages from the fragmentary netty buffers. This decoder assumes the
 * messages have the following format:
 * +-----------------------------------+--------------------------------+
 * | FRAME HEADER ||  MESSAGE HEADER   |     DATA BUFFER (Optional)     |
 * +-----------------------------------+--------------------------------+
 * and it decodes each part in order.
 *
 * This decoder tries best to eliminate copying. For the frame header and message header,
 * it only cumulates data when they span multiple input buffers. For the buffer part, it
 * copies directly to the input channels to avoid future copying.
 *
 * The format of the frame header is
 * +------------------+------------------+--------++----------------+
 * | FRAME LENGTH (4) | MAGIC NUMBER (4) | ID (1) || CUSTOM MESSAGE |
 * +------------------+------------------+--------++----------------+
 */
public class ZeroCopyNettyMessageDecoder extends ChannelInboundHandlerAdapter {

	private static final int INITIAL_MESSAGE_HEADER_BUFFER_LENGTH = 128;

	/** The parser to parse the message header. */
	private final NettyMessageParser messageParser;

	/** The buffer used to cumulate the frame header part. */
	private ByteBuf frameHeaderBuffer;

	/** The buffer used to receive the message header part. */
	private ByteBuf messageHeaderBuffer;

	/** Which part of the current message is being decoded. */
	private DecodeStep decodeStep;

	/** How many bytes have been decoded in current step. */
	private int decodedBytesOfCurrentStep;

	/** The intermediate state when decoding the current message. */
	private final MessageDecodeIntermediateState intermediateState;

	ZeroCopyNettyMessageDecoder(NettyMessageParser messageParser) {
		this.messageParser = messageParser;
		this.intermediateState = new MessageDecodeIntermediateState();
	}

	@Override
	public void channelActive(ChannelHandlerContext ctx) throws Exception {
		super.channelActive(ctx);

		frameHeaderBuffer = ctx.alloc().directBuffer(NettyMessage.FRAME_HEADER_LENGTH);
		messageHeaderBuffer = ctx.alloc().directBuffer(INITIAL_MESSAGE_HEADER_BUFFER_LENGTH);

		decodeStep = DecodeStep.DECODING_FRAME;
	}

	@Override
	public void channelInactive(ChannelHandlerContext ctx) throws Exception {
		super.channelInactive(ctx);

		if (intermediateState.messageHeaderParseResult != null) {
			Buffer buffer = intermediateState.messageHeaderParseResult.getTargetDataBuffer();

			if (buffer != null) {
				buffer.recycleBuffer();
			}
		}

		clearState();

		frameHeaderBuffer.release();
		messageHeaderBuffer.release();
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
				if (decodeStep == DecodeStep.DECODING_FRAME) {
					ByteBuf toDecode = cumulateBufferIfNeeded(frameHeaderBuffer, data, FRAME_HEADER_LENGTH);

					if (toDecode != null) {
						decodeFrameHeader(toDecode);

						decodedBytesOfCurrentStep = 0;
						decodeStep = DecodeStep.DECODING_MESSAGE_HEADER;
					}
				}

				if (decodeStep == DecodeStep.DECODING_MESSAGE_HEADER) {
					ByteBuf toDecoder = cumulateBufferIfNeeded(messageHeaderBuffer, data, intermediateState.messageHeaderLength);

					if (toDecoder != null) {
						intermediateState.messageHeaderParseResult = messageParser.parseMessageHeader(intermediateState.msgId, toDecoder);

						if (intermediateState.messageHeaderParseResult.getDataBufferAction() == NettyMessageParser.DataBufferAction.NO_DATA_BUFFER) {
							ctx.fireChannelRead(intermediateState.messageHeaderParseResult.getParsedMessage());
							clearState();
						} else {
							decodedBytesOfCurrentStep = 0;
							decodeStep = DecodeStep.DECODING_BUFFER;
						}
					}
				}

				if (decodeStep == DecodeStep.DECODING_BUFFER) {
					readOrDiscardBufferResponse(data);

					if (decodedBytesOfCurrentStep == intermediateState.messageHeaderParseResult.getDataBufferSize()) {
						ctx.fireChannelRead(intermediateState.messageHeaderParseResult.getParsedMessage());
						clearState();
					}
				}
			}

			checkState(!data.isReadable(), "Not all data of the received buffer consumed.");
		} finally {
			data.release();
		}
	}

	private void decodeFrameHeader(ByteBuf frameHeaderBuffer) {
		int messageLength = frameHeaderBuffer.readInt();
		checkState(messageLength >= 0, "The length field of current message must be non-negative");

		int magicNumber = frameHeaderBuffer.readInt();
		checkState(magicNumber == MAGIC_NUMBER, "Network stream corrupted: received incorrect magic number.");

		intermediateState.msgId = frameHeaderBuffer.readByte();
		intermediateState.messageHeaderLength = messageParser.getMessageHeaderLength(
				messageLength - FRAME_HEADER_LENGTH,
				intermediateState.msgId);
	}

	private void readOrDiscardBufferResponse(ByteBuf data) {
		int dataBufferSize = intermediateState.messageHeaderParseResult.getDataBufferSize();

		// If current buffer is empty, then there is no more data to receive.
		if (dataBufferSize == 0) {
			return;
		}

		NettyMessageParser.DataBufferAction dataBufferAction = intermediateState.messageHeaderParseResult.getDataBufferAction();
		int remainingBufferSize = dataBufferSize - decodedBytesOfCurrentStep;

		switch (dataBufferAction) {
			case RECEIVE:
				Buffer dataBuffer = intermediateState.messageHeaderParseResult.getTargetDataBuffer();
				decodedBytesOfCurrentStep += copyToTargetBuffer(dataBuffer.asByteBuf(), data, remainingBufferSize);
				break;
			case DISCARD:
				int actualBytesToDiscard = Math.min(data.readableBytes(), remainingBufferSize);
				data.readerIndex(data.readerIndex() + actualBytesToDiscard);
				decodedBytesOfCurrentStep += actualBytesToDiscard;
				break;
		}
	}

	private ByteBuf cumulateBufferIfNeeded(ByteBuf cumulatedBuffer, ByteBuf src, int size) {
		int cumulatedSize = cumulatedBuffer.readableBytes();

		if (cumulatedSize == 0) {
			if (src.readableBytes() >= size) {
				return src;
			} else {
				// The capacity will stop increasing after reaching the maximum value.
				if (cumulatedBuffer.capacity() < size) {
					cumulatedBuffer.capacity(size);
				}
			}
		}

		copyToTargetBuffer(cumulatedBuffer, src, size - cumulatedBuffer.readableBytes());

		if (cumulatedBuffer.readableBytes() == size) {
			return cumulatedBuffer;
		}

		return null;
	}

	/**
	 * Clears all the intermediate state for reading the next message.
	 */
	private void clearState() {
		frameHeaderBuffer.clear();
		messageHeaderBuffer.clear();

		intermediateState.resetState();

		decodedBytesOfCurrentStep = 0;
		decodeStep = DecodeStep.DECODING_FRAME;
	}

	/**
	 * Copies bytes from the src to dest, but do not exceed the capacity of the dest buffer.
	 *
	 * @param dest The ByteBuf to copy bytes to.
	 * @param src The ByteBuf to copy bytes from.
	 * @param maxCopySize Maximum size of bytes to copy.
	 * @return The length of actually copied bytes.
	 */
	private int copyToTargetBuffer(ByteBuf dest, ByteBuf src, int maxCopySize) {
		int copyLength = Math.min(src.readableBytes(), maxCopySize);
		checkState(dest.writableBytes() >= copyLength,
			"There is not enough space to copy " + copyLength + " bytes, writable = " + dest.writableBytes());

		dest.writeBytes(src, copyLength);

		return copyLength;
	}

	/**
	 * Indicates which part of the current message is being decoded.
	 */
	private enum DecodeStep {
		/** The frame header is under decoding. */
		DECODING_FRAME,

		/** The message header is under decoding. */
		DECODING_MESSAGE_HEADER,

		/** The data buffer part is under decoding. */
		DECODING_BUFFER;
	}

	/**
	 * The intermediate state produced when decoding the current message.
	 */
	private static class MessageDecodeIntermediateState {
		/** The message id of current message. */
		byte msgId = -1;

		/** The length of message header part of current message */
		int messageHeaderLength = -1;

		/** The parse result of the message header part */
		NettyMessageParser.MessageHeaderParseResult messageHeaderParseResult;

		void resetState() {
			msgId = -1;
			messageHeaderLength = -1;
			messageHeaderParseResult = null;
		}
	}
}
