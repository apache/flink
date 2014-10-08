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

import io.netty.channel.embedded.EmbeddedChannel;
import org.apache.flink.runtime.event.task.TaskEvent;
import org.apache.flink.runtime.io.network.buffer.Buffer;
import org.apache.flink.runtime.io.network.BufferOrEvent;
import org.apache.flink.runtime.io.network.partition.IntermediateResultPartitionQueueIterator;
import org.apache.flink.runtime.io.network.partition.consumer.InputChannelID;
import org.junit.Test;

import java.io.IOException;
import java.util.Random;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.fail;
import static org.mockito.Mockito.mock;

public class ServerOutboundPartitionQueueTest {

	@Test
	public void testSendPartition() {
		try {

			ServerOutboundPartitionQueue queue = new ServerOutboundPartitionQueue();
			EmbeddedChannel ch = new EmbeddedChannel(queue);

			IntermediateResultPartitionQueueIterator it = new MockPartitionIterator();

			queue.enqueue(it, new InputChannelID());

			assertEquals(1024, ch.outboundMessages().size());
		}
		catch (Throwable t) {
			t.printStackTrace();
			fail(t.getMessage());
		}
	}

	static class MockPartitionIterator implements IntermediateResultPartitionQueueIterator {

		private Random random = new Random();

		private int numBufferOrEventsToReturn = 1024;

		@Override
		public boolean hasNext() {
			return numBufferOrEventsToReturn > 0;
		}

		@Override
		public BufferOrEvent getNextBufferOrEvent() throws IOException {
			if (!hasNext()) {
				return null;
			}

			if (random.nextDouble() <= 0.2) {
				return null;
			}

			numBufferOrEventsToReturn--;

			if (random.nextDouble() <= 0.9) {
				return new BufferOrEvent(mock(Buffer.class));
			}
			else {
				return new BufferOrEvent(mock(TaskEvent.class));
			}
		}
	}
}
