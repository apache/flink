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

import org.apache.flink.shaded.netty4.io.netty.buffer.ByteBuf;
import org.apache.flink.shaded.netty4.io.netty.buffer.Unpooled;
import org.apache.flink.shaded.netty4.io.netty.channel.Channel;
import org.apache.flink.shaded.netty4.io.netty.channel.ChannelHandler;
import org.apache.flink.shaded.netty4.io.netty.channel.ChannelHandlerContext;
import org.apache.flink.shaded.netty4.io.netty.channel.ChannelInboundHandlerAdapter;

import org.junit.Test;

import java.util.concurrent.atomic.AtomicReference;

import static org.apache.flink.runtime.io.network.netty.NettyTestUtil.NettyServerAndClient;
import static org.apache.flink.runtime.io.network.netty.NettyTestUtil.awaitClose;
import static org.apache.flink.runtime.io.network.netty.NettyTestUtil.connect;
import static org.apache.flink.runtime.io.network.netty.NettyTestUtil.createConfig;
import static org.apache.flink.runtime.io.network.netty.NettyTestUtil.initServerAndClient;
import static org.apache.flink.runtime.io.network.netty.NettyTestUtil.shutdown;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

/**
 * Verifies that high and low watermarks for {@link NettyServer} may be set to any (valid) values
 * given by the user.
 */
public class NettyServerLowAndHighWatermarkTest {

	/**
	 * Verify low and high watermarks being set correctly for larger memory segment sizes which
	 * trigger <a href="https://issues.apache.org/jira/browse/FLINK-7258">FLINK-7258</a>.
	 */
	@Test
	public void testLargeLowAndHighWatermarks() throws Throwable {
		testLowAndHighWatermarks(65536);
	}

	/**
	 * Verify low and high watermarks being set correctly for smaller memory segment sizes than
	 * Netty's defaults.
	 */
	@Test
	public void testSmallLowAndHighWatermarks() throws Throwable {
		testLowAndHighWatermarks(1024);
	}

	/**
	 * Verifies that the high and low watermark are set in relation to the page size.
	 *
	 * <p>The high and low water marks control the data flow to the wire. If the Netty write buffer
	 * has size greater or equal to the high water mark, the channel state becomes not-writable.
	 * Only when the size falls below the low water mark again, the state changes to writable again.
	 *
	 * <p>The Channel writability state needs to be checked by the handler when writing to the
	 * channel and is not enforced in the sense that you cannot write a channel, which is in
	 * not-writable state.
	 *
	 * @param pageSize memory segment size to test with (influences high and low watermarks)
	 */
	private void testLowAndHighWatermarks(int pageSize) throws Throwable {
		final int expectedLowWatermark = pageSize + 1;
		final int expectedHighWatermark = 2 * pageSize;

		final AtomicReference<Throwable> error = new AtomicReference<Throwable>();
		final NettyProtocol protocol = new NettyProtocol(null, null, true) {
			@Override
			public ChannelHandler[] getServerChannelHandlers() {
				// The channel handler implements the test
				return new ChannelHandler[] {new TestLowAndHighWatermarkHandler(
					pageSize, expectedLowWatermark, expectedHighWatermark, error)};
			}

			@Override
			public ChannelHandler[] getClientChannelHandlers() {
				return new ChannelHandler[0];
			}
		};

		final NettyConfig conf = createConfig(pageSize);

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
	 * <p>Verifies that the high and low watermark are set in relation to the page size.
	 */
	private static class TestLowAndHighWatermarkHandler extends ChannelInboundHandlerAdapter {

		private final int pageSize;

		private final int expectedLowWatermark;

		private final int expectedHighWatermark;

		private final AtomicReference<Throwable> error;

		private boolean hasFlushed;

		public TestLowAndHighWatermarkHandler(
				int pageSize, int expectedLowWatermark, int expectedHighWatermark,
				AtomicReference<Throwable> error) {
			this.pageSize = pageSize;
			this.expectedLowWatermark = expectedLowWatermark;
			this.expectedHighWatermark = expectedHighWatermark;
			this.error = error;
		}

		@Override
		public void channelActive(ChannelHandlerContext ctx) throws Exception {
			final Channel ch = ctx.channel();

			assertEquals("Low watermark", expectedLowWatermark, ch.config().getWriteBufferLowWaterMark());
			assertEquals("High watermark", expectedHighWatermark, ch.config().getWriteBufferHighWaterMark());

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

		private ByteBuf buffer() {
			return NettyServerLowAndHighWatermarkTest.buffer(pageSize);
		}
	}

	// ---------------------------------------------------------------------------------------------

	/**
	 * Creates a new buffer of the given size.
	 */
	private static ByteBuf buffer(int size) {
		// Set the writer index to the size to ensure that all bytes are written to the wire
		// although the buffer is "empty".
		return Unpooled.buffer(size).writerIndex(size);
	}
}
