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

import org.apache.flink.runtime.io.network.NetworkClientHandler;
import org.apache.flink.shaded.netty4.io.netty.buffer.ByteBuf;
import org.apache.flink.shaded.netty4.io.netty.channel.ChannelHandlerContext;
import org.apache.flink.shaded.netty4.io.netty.channel.ChannelInboundHandlerAdapter;
import org.apache.flink.util.IOUtils;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static org.apache.flink.runtime.io.network.netty.NettyMessage.FRAME_HEADER_LENGTH;
import static org.apache.flink.runtime.io.network.netty.NettyMessage.MAGIC_NUMBER;
import static org.apache.flink.util.Preconditions.checkNotNull;
import static org.apache.flink.util.Preconditions.checkState;

/**
 * Decodes messages from the received netty buffers. This decoder assumes the
 * messages have the following format:
 * +-----------------------------------+--------------------------------+
 * | FRAME_HEADER ||  MESSAGE_HEADER   |     DATA BUFFER (Optional)     |
 * +-----------------------------------+--------------------------------+
 *
 * <p>This decoder decodes the frame header and delegates the following work to the
 * corresponding message decoders according to the message type. During this process
 * The frame header and message header are only accumulated if they span  received
 * multiple netty buffers, and the data buffer is copied directly to the buffer
 * of corresponding input channel to avoid more copying.
 *
 * <p>The format of the frame header is
 * +------------------+------------------+--------+
 * | FRAME LENGTH (4) | MAGIC NUMBER (4) | ID (1) |
 * +------------------+------------------+--------+
 */
public class NettyMessageClientDecoderDelegate extends ChannelInboundHandlerAdapter {
	private final Logger LOG = LoggerFactory.getLogger(NettyMessageClientDecoderDelegate.class);

	/** The decoder for BufferResponse. */
    private final NettyMessageDecoder bufferResponseDecoder;

    /** The decoder for messages other than BufferResponse. */
	private final NettyMessageDecoder nonBufferResponseDecoder;

	/** The accumulation buffer for the frame header. */
	private ByteBuf frameHeaderBuffer;

	/** The decoder for the current message. It is null if we are decoding the frame header. */
	private NettyMessageDecoder currentDecoder;

    NettyMessageClientDecoderDelegate(NetworkClientHandler networkClientHandler) {
		this.bufferResponseDecoder = new BufferResponseDecoder(
			new NetworkBufferAllocator(
				checkNotNull(networkClientHandler)));
        this.nonBufferResponseDecoder = new NonBufferResponseDecoder();
    }

    @Override
    public void channelActive(ChannelHandlerContext ctx) throws Exception {
        bufferResponseDecoder.onChannelActive(ctx);
        nonBufferResponseDecoder.onChannelActive(ctx);

		frameHeaderBuffer = ctx.alloc().directBuffer(FRAME_HEADER_LENGTH);

		super.channelActive(ctx);
    }

	/**
	 * Releases resources when the channel is closed. When exceptions are thrown during
	 * processing received netty buffers, {@link CreditBasedPartitionRequestClientHandler}
	 * is expected to catch the exception and close the channel and trigger this notification.
	 *
	 * @param ctx The context of the channel close notification.
	 */
	@Override
    public void channelInactive(ChannelHandlerContext ctx) throws Exception {
		IOUtils.cleanup(LOG, bufferResponseDecoder, nonBufferResponseDecoder);
		frameHeaderBuffer.release();

		super.channelInactive(ctx);
    }

    @Override
    public void channelRead(ChannelHandlerContext ctx, Object msg) throws Exception {
    	if (!(msg instanceof ByteBuf)) {
			ctx.fireChannelRead(msg);
			return;
		}

        ByteBuf data = (ByteBuf) msg;
        try {
            while (data.isReadable()) {
            	if (currentDecoder != null) {
					NettyMessageDecoder.DecodingResult result = currentDecoder.onChannelRead(data);
					if (!result.isFinished()) {
						break;
					}
					ctx.fireChannelRead(result.getMessage());

					currentDecoder = null;
					frameHeaderBuffer.clear();
				}

				decodeFrameHeader(data);
            }
            checkState(!data.isReadable(), "Not all data of the received buffer consumed.");
        } finally {
            data.release();
        }
    }

    private void decodeFrameHeader(ByteBuf data) {
		ByteBuf fullFrameHeaderBuf = ByteBufUtils.accumulate(
			frameHeaderBuffer,
			data,
			FRAME_HEADER_LENGTH,
			frameHeaderBuffer.readableBytes());

		if (fullFrameHeaderBuf != null) {
			int messageAndFrameLength = fullFrameHeaderBuf.readInt();
			checkState(messageAndFrameLength >= 0, "The length field of current message must be non-negative");

			int magicNumber = fullFrameHeaderBuf.readInt();
			checkState(magicNumber == MAGIC_NUMBER, "Network stream corrupted: received incorrect magic number.");

			int msgId = fullFrameHeaderBuf.readByte();
			if (msgId == NettyMessage.BufferResponse.ID) {
				currentDecoder = bufferResponseDecoder;
			} else {
				currentDecoder = nonBufferResponseDecoder;
			}

			currentDecoder.onNewMessageReceived(msgId, messageAndFrameLength - FRAME_HEADER_LENGTH);
		}
	}
}
