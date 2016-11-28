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

import io.netty.channel.embedded.EmbeddedChannel;
import org.apache.flink.runtime.execution.CancelTaskException;
import org.apache.flink.runtime.io.network.buffer.BufferProvider;
import org.apache.flink.runtime.io.network.partition.BufferAvailabilityListener;
import org.apache.flink.runtime.io.network.partition.ResultPartitionID;
import org.apache.flink.runtime.io.network.partition.ResultPartitionProvider;
import org.apache.flink.runtime.io.network.partition.ResultSubpartitionView;
import org.apache.flink.runtime.io.network.partition.consumer.InputChannelID;
import org.junit.Test;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import static org.mockito.Matchers.any;
import static org.mockito.Matchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

public class PartitionRequestQueueTest {

	@Test
	public void testProducerFailedException() throws Exception {
		PartitionRequestQueue queue = new PartitionRequestQueue();

		ResultPartitionProvider partitionProvider = mock(ResultPartitionProvider.class);
		ResultPartitionID rpid = new ResultPartitionID();
		BufferProvider bufferProvider = mock(BufferProvider.class);

		ResultSubpartitionView view = mock(ResultSubpartitionView.class);
		when(view.isReleased()).thenReturn(true);
		when(view.getFailureCause()).thenReturn(new RuntimeException("Expected test exception"));

		when(partitionProvider.createSubpartitionView(
			eq(rpid),
			eq(0),
			eq(bufferProvider),
			any(BufferAvailabilityListener.class))).thenReturn(view);

		EmbeddedChannel ch = new EmbeddedChannel(queue);

		SequenceNumberingViewReader seqView = new SequenceNumberingViewReader(new InputChannelID(), queue);
		seqView.requestSubpartitionView(partitionProvider, rpid, 0, bufferProvider);

		// Enqueue the erroneous view
		queue.notifyReaderNonEmpty(seqView);
		ch.runPendingTasks();

		// Read the enqueued msg
		Object msg = ch.readOutbound();

		assertEquals(msg.getClass(), NettyMessage.ErrorResponse.class);

		NettyMessage.ErrorResponse err = (NettyMessage.ErrorResponse) msg;
		assertTrue(err.cause instanceof CancelTaskException);
	}
}
