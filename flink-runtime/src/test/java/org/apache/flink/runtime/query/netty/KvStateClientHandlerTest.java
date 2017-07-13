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

package org.apache.flink.runtime.query.netty;

import org.apache.flink.runtime.query.netty.message.KvStateRequestSerializer;

import io.netty.buffer.ByteBuf;
import io.netty.channel.embedded.EmbeddedChannel;
import org.junit.Test;

import java.nio.channels.ClosedChannelException;

import static org.junit.Assert.assertEquals;
import static org.mockito.Matchers.any;
import static org.mockito.Matchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;

/**
 * Tests for {@link KvStateClientHandler}.
 */
public class KvStateClientHandlerTest {

	/**
	 * Tests that on reads the expected callback methods are called and read
	 * buffers are recycled.
	 */
	@Test
	public void testReadCallbacksAndBufferRecycling() throws Exception {
		KvStateClientHandlerCallback callback = mock(KvStateClientHandlerCallback.class);

		EmbeddedChannel channel = new EmbeddedChannel(new KvStateClientHandler(callback));

		//
		// Request success
		//
		ByteBuf buf = KvStateRequestSerializer.serializeKvStateRequestResult(
				channel.alloc(),
				1222112277,
				new byte[0]);
		buf.skipBytes(4); // skip frame length

		// Verify callback
		channel.writeInbound(buf);
		verify(callback, times(1)).onRequestResult(eq(1222112277L), any(byte[].class));
		assertEquals("Buffer not recycled", 0, buf.refCnt());

		//
		// Request failure
		//
		buf = KvStateRequestSerializer.serializeKvStateRequestFailure(
				channel.alloc(),
				1222112278,
				new RuntimeException("Expected test Exception"));
		buf.skipBytes(4); // skip frame length

		// Verify callback
		channel.writeInbound(buf);
		verify(callback, times(1)).onRequestFailure(eq(1222112278L), any(RuntimeException.class));
		assertEquals("Buffer not recycled", 0, buf.refCnt());

		//
		// Server failure
		//
		buf = KvStateRequestSerializer.serializeServerFailure(
				channel.alloc(),
				new RuntimeException("Expected test Exception"));
		buf.skipBytes(4); // skip frame length

		// Verify callback
		channel.writeInbound(buf);
		verify(callback, times(1)).onFailure(any(RuntimeException.class));

		//
		// Unexpected messages
		//
		buf = channel.alloc().buffer(4).writeInt(1223823);

		// Verify callback
		channel.writeInbound(buf);
		verify(callback, times(2)).onFailure(any(IllegalStateException.class));
		assertEquals("Buffer not recycled", 0, buf.refCnt());

		//
		// Exception caught
		//
		channel.pipeline().fireExceptionCaught(new RuntimeException("Expected test Exception"));
		verify(callback, times(3)).onFailure(any(RuntimeException.class));

		//
		// Channel inactive
		//
		channel.pipeline().fireChannelInactive();
		verify(callback, times(4)).onFailure(any(ClosedChannelException.class));
	}

}
