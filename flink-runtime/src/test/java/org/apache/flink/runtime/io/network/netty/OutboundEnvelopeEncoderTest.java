/**
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
import io.netty.channel.embedded.EmbeddedChannel;

import org.junit.Assert;

import org.apache.flink.core.memory.MemorySegment;
import org.apache.flink.runtime.io.network.Buffer;
import org.apache.flink.runtime.io.network.Envelope;
import org.apache.flink.runtime.io.network.channels.ChannelID;
import org.apache.flink.runtime.io.network.netty.OutboundEnvelopeEncoder;
import org.apache.flink.runtime.jobgraph.JobID;
import org.junit.Test;

import java.nio.ByteBuffer;
import java.util.Random;

import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

public class OutboundEnvelopeEncoderTest {

	private final long RANDOM_SEED = 520346508276087l;

	private final Random random = new Random(RANDOM_SEED);

	private static final int NUM_RANDOM_ENVELOPES = 512;

	private static final int MAX_EVENTS_SIZE = 1024;

	private static final int MAX_BUFFER_SIZE = 32768;

	@Test
	public void testEncodedSizeAndBufferRecycling() {
		final ByteBuffer events = ByteBuffer.allocate(MAX_EVENTS_SIZE);
		final MemorySegment segment = new MemorySegment(new byte[MAX_BUFFER_SIZE]);

		final Buffer buffer = mock(Buffer.class);
		when(buffer.getMemorySegment()).thenReturn(segment);

		final EmbeddedChannel channel = new EmbeddedChannel(new OutboundEnvelopeEncoder());

		int numBuffers = 0;
		for (int i = 0; i < NUM_RANDOM_ENVELOPES; i++) {
			Envelope env = new Envelope(i, new JobID(), new ChannelID());
			int expectedEncodedMsgSize = OutboundEnvelopeEncoder.HEADER_SIZE;

			if (random.nextBoolean()) {
				int eventsSize = random.nextInt(MAX_EVENTS_SIZE + 1);
				expectedEncodedMsgSize += eventsSize;

				events.clear();
				events.limit(eventsSize);

				env.setEventsSerialized(events);
			}

			if (random.nextBoolean()) {
				numBuffers++;

				int bufferSize = random.nextInt(MAX_BUFFER_SIZE + 1);
				when(buffer.size()).thenReturn(bufferSize);
				env.setBuffer(buffer);

				expectedEncodedMsgSize += bufferSize;
			}

			Assert.assertTrue(channel.writeOutbound(env));

			// --------------------------------------------------------------------
			// verify encoded ByteBuf size
			// --------------------------------------------------------------------
			ByteBuf encodedMsg = (ByteBuf) channel.readOutbound();
			Assert.assertEquals(expectedEncodedMsgSize, encodedMsg.readableBytes());

			encodedMsg.release();
		}

		// --------------------------------------------------------------------
		// verify buffers are recycled
		// --------------------------------------------------------------------
		verify(buffer, times(numBuffers)).recycleBuffer();
	}
}
