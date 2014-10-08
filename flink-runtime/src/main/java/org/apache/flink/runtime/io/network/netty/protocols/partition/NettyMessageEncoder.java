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
import io.netty.channel.ChannelOutboundHandlerAdapter;
import io.netty.channel.ChannelPromise;

import static io.netty.channel.ChannelHandler.Sharable;

@Sharable
class NettyMessageEncoder extends ChannelOutboundHandlerAdapter {

	static final int MAGIC_NUMBER = 0xBADC0FFE;

	@Override
	public void write(ChannelHandlerContext ctx, Object msg, ChannelPromise promise) throws Exception {
		if (msg instanceof NettyMessage) {
			ClientRequest request = (ClientRequest) msg;
			// frame length (4), magic number (4), message id (1), message (var length)
			int frameLength = 4 + 4 + 1 + request.getLength();

			final ByteBuf outboundBuffer = ctx.alloc().directBuffer(frameLength);

			outboundBuffer.writeInt(frameLength);
			outboundBuffer.writeInt(MAGIC_NUMBER);
			outboundBuffer.writeByte(request.getId());
			request.writeTo(outboundBuffer);

			ctx.write(outboundBuffer, promise);
		}
	}
}
