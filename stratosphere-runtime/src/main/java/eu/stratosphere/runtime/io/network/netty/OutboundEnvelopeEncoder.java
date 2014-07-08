/***********************************************************************************************************************
 * Copyright (C) 2010-2014 by the Stratosphere project (http://stratosphere.eu)
 *
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software distributed under the License is distributed on
 * an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the License for the
 * specific language governing permissions and limitations under the License.
 **********************************************************************************************************************/

package eu.stratosphere.runtime.io.network.netty;

import eu.stratosphere.runtime.io.Buffer;
import eu.stratosphere.runtime.io.network.Envelope;
import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;
import io.netty.channel.ChannelHandler;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.ChannelOutboundHandlerAdapter;
import io.netty.channel.ChannelPromise;

@ChannelHandler.Sharable
public class OutboundEnvelopeEncoder extends ChannelOutboundHandlerAdapter {

	public static final int HEADER_SIZE = 48;

	public static final int MAGIC_NUMBER = 0xBADC0FFE;

	@Override
	public void write(ChannelHandlerContext ctx, Object msg, ChannelPromise promise) throws Exception {
		Envelope env = (Envelope) msg;

		ByteBuf buf = ctx.alloc().directBuffer();

		encode(env, buf);

		if (buf.isReadable()) {
			ctx.write(buf, promise);
		}
		else {
			buf.release();
			ctx.write(Unpooled.EMPTY_BUFFER, promise);
		}
	}

	private void encode(Envelope env, ByteBuf out) {
		// --------------------------------------------------------------------
		// (1) header (48 bytes)
		// --------------------------------------------------------------------
		out.writeInt(MAGIC_NUMBER); // 4 bytes

		if (out.getInt(out.writerIndex() - 4) != MAGIC_NUMBER) {
			throw new RuntimeException();
		}

		out.writeInt(env.getSequenceNumber()); // 4 bytes
		env.getJobID().writeTo(out); // 16 bytes
		env.getSource().writeTo(out); // 16 bytes
		out.writeInt(env.getEventsSerialized() != null ? env.getEventsSerialized().remaining() : 0); // 4 bytes
		out.writeInt(env.getBuffer() != null ? env.getBuffer().size() : 0); // 4 bytes
		// --------------------------------------------------------------------
		// (2) events (var length)
		// --------------------------------------------------------------------
		if (env.getEventsSerialized() != null) {
			out.writeBytes(env.getEventsSerialized());
		}

		// --------------------------------------------------------------------
		// (3) buffer (var length)
		// --------------------------------------------------------------------
		if (env.getBuffer() != null) {
			Buffer envBuffer = env.getBuffer();
			out.writeBytes(envBuffer.getMemorySegment().wrap(0, envBuffer.size()));

			// Recycle the buffer from OUR buffer pool after everything has been
			// copied to Nettys buffer space.
			envBuffer.recycleBuffer();
		}
	}
}
