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
import io.netty.buffer.Unpooled;
import io.netty.channel.Channel;
import io.netty.channel.ChannelHandler;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.ChannelInboundHandlerAdapter;
import org.junit.Test;

import java.util.concurrent.atomic.AtomicReference;

import static org.apache.flink.runtime.io.network.netty.NettyTestUtil.NettyServerAndClient;
import static org.apache.flink.runtime.io.network.netty.NettyTestUtil.awaitClose;
import static org.apache.flink.runtime.io.network.netty.NettyTestUtil.connect;
import static org.apache.flink.runtime.io.network.netty.NettyTestUtil.createConfig;
import static org.apache.flink.runtime.io.network.netty.NettyTestUtil.initServerAndClient;
import static org.apache.flink.runtime.io.network.netty.NettyTestUtil.shutdown;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

public class NettyServerLowAndHighWatermarkTest {

	private final static int PageSize = 1024;

	/**
	 * Verifies that the high and low watermark are set in relation to the page size.
	 *
	 * <p> The high and low water marks control the data flow to the wire. If the Netty write buffer
	 * has size greater or equal to the high water mark, the channel state becomes not-writable.
	 * Only when the size falls below the low water mark again, the state changes to writable again.
	 *
	 * <p> The Channel writability state needs to be checked by the handler when writing to the
	 * channel and is not enforced in the sense that you cannot write a channel, which is in
	 * not-writable state.
	 */
	@Test
	public void testLowAndHighWatermarks() throws Throwable {
		final AtomicReference<Throwable> error = new AtomicReference<Throwable>();
		final NettyProtocol protocol = new NettyProtocol() {
			@Override
			public ChannelHandler[] getServerChannelHandlers() {
				// The channel handler implements the test
				return new ChannelHandler[] {new TestLowAndHighWatermarkHandler(error)};
			}

			@Override
			public ChannelHandler[] getClientChannelHandlers() {
				return new ChannelHandler[0];
			}
		};

		final NettyConfig conf = createConfig(PageSize);

		final NettyServerAndClient serverAndClient = initServerAndClient(protocol, conf);

		try {
			// We can't just check the config of this channel as it is the client's channel. We need
			// to check the server channel, because it is doing the data transfers.
			final Channel ch = connect(serverAndClient);

			// Wait for the channel to be closed
			awaitClose(ch);

			final Throwable t = error.get();
			if (t != null) {
				throw t;
			}
		}
		finally {
			shutdown(serverAndClient);
		}
	}

	/**
	 * This handler implements the test.
	 *
	 * <p> Verifies that the high and low watermark are set in relation to the page size.
	 */
	private static class TestLowAndHighWatermarkHandler extends ChannelInboundHandlerAdapter {

		private final AtomicReference<Throwable> error;

		private boolean hasFlushed;

		public TestLowAndHighWatermarkHandler(AtomicReference<Throwable> error) {
			this.error = error;
		}

		@Override
		public void channelActive(ChannelHandlerContext ctx) throws Exception {
			final Channel ch = ctx.channel();

			// Start with a writable channel
			assertTrue(ch.isWritable());

			// First buffer should not change writability
			ch.write(buffer());
			assertTrue(ch.isWritable());

			// ...second buffer should though
			ch.write(buffer());
			assertFalse(ch.isWritable());

			// Flush everything and close the channel after the writability changed event is fired.
			hasFlushed = true;
			ch.flush();
		}

		@Override
		public void channelWritabilityChanged(ChannelHandlerContext ctx) throws Exception {
			if (hasFlushed) {
				// After flushing the writability should change back to writable
				assertTrue(ctx.channel().isWritable());

				// Close the channel. This will terminate the main test Thread.
				ctx.close();
			}

			super.channelWritabilityChanged(ctx);
		}

		@Override
		public void exceptionCaught(ChannelHandlerContext ctx, Throwable cause) throws Exception {
			if (error.get() == null) {
				error.set(cause);
			}

			ctx.close();

			super.exceptionCaught(ctx, cause);
		}
	}

	// ---------------------------------------------------------------------------------------------

	private static ByteBuf buffer() {
		return buffer(PageSize);
	}

	/**
	 * Creates a new buffer of the given size.
	 */
	private static ByteBuf buffer(int size) {
		// Set the writer index to the size to ensure that all bytes are written to the wire
		// although the buffer is "empty".
		return Unpooled.buffer(size).writerIndex(size);
	}
}
