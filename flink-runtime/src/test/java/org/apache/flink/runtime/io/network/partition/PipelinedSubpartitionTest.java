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

package org.apache.flink.runtime.io.network.partition;

import org.apache.flink.core.memory.MemorySegment;
import org.apache.flink.runtime.event.AbstractEvent;
import org.apache.flink.runtime.io.network.buffer.Buffer;
import org.apache.flink.runtime.io.network.buffer.BufferProvider;
import org.apache.flink.runtime.io.network.partition.consumer.BufferOrEvent;
import org.apache.flink.runtime.io.network.util.TestConsumerCallback;
import org.apache.flink.runtime.io.network.util.TestPooledBufferProvider;
import org.apache.flink.runtime.io.network.util.TestProducerSource;
import org.apache.flink.runtime.io.network.util.TestSubpartitionConsumer;
import org.apache.flink.runtime.io.network.util.TestSubpartitionProducer;
import org.junit.AfterClass;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

import java.util.Arrays;
import java.util.Collection;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;

import static org.apache.flink.runtime.io.network.util.TestBufferFactory.createBuffer;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.fail;
import static org.mockito.Matchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;

@RunWith(Parameterized.class)
public class PipelinedSubpartitionTest extends SubpartitionTestBase {

	@Parameterized.Parameters
	public static Collection<Object[]> data() {
		// The capacity limit tests are mostly
		// relevant for the slow/fast consumer tests

		return Arrays.asList(new Object[][]{
			{0}, // unbounded
			{1}, // single buffer capacity limit
			{4} // 4 buffers capacity limit
		});
	}

	/** Executor service for concurrent produce/consume tests */
	private final static ExecutorService executorService = Executors.newCachedThreadPool();

	private final int capacityLimit;

	public PipelinedSubpartitionTest(int capacityLimit) {
		this.capacityLimit = capacityLimit;
	}

	@AfterClass
	public static void shutdownExecutorService() throws Exception {
		executorService.shutdownNow();
	}

	@Override
	PipelinedSubpartition createSubpartition() {
		final ResultPartition parent = mock(ResultPartition.class);

		return new PipelinedSubpartition(0, parent, capacityLimit);
	}

	@Test
	public void testIllegalReadViewRequest() throws Exception {
		final PipelinedSubpartition subpartition = createSubpartition();

		// Successful request
		assertNotNull(subpartition.createReadView(null, new BufferAvailabilityListener() {
			@Override
			public void notifyBuffersAvailable(long numBuffers) {
			}
		}));

		try {
			subpartition.createReadView(null, new BufferAvailabilityListener() {
				@Override
				public void notifyBuffersAvailable(long numBuffers) {
				}
			});

			fail("Did not throw expected exception after duplicate notifyNonEmpty view request.");
		} catch (IllegalStateException expected) {
		}
	}

	@Test
	public void testBasicPipelinedProduceConsumeLogic() throws Exception {
		final PipelinedSubpartition subpartition = createSubpartition();

		BufferAvailabilityListener listener = mock(BufferAvailabilityListener.class);

		ResultSubpartitionView view = subpartition.createReadView(null, listener);

		// Empty => should return null
		assertNull(view.getNextBuffer());
		verify(listener, times(1)).notifyBuffersAvailable(eq(0L));

		// Add data to the queue...
		subpartition.add(createBuffer(), false);

		// ...should have resulted in a notification
		verify(listener, times(1)).notifyBuffersAvailable(eq(1L));

		// ...and one available result
		assertNotNull(view.getNextBuffer());
		assertNull(view.getNextBuffer());

		// Add data to the queue...
		subpartition.add(createBuffer(), false);
		verify(listener, times(2)).notifyBuffersAvailable(eq(1L));
	}

	@Test
	public void testConcurrentFastProduceAndFastConsume() throws Exception {
		testProduceConsume(false, false);
	}

	@Test
	public void testConcurrentFastProduceAndSlowConsume() throws Exception {
		testProduceConsume(false, true);
	}

	@Test
	public void testConcurrentSlowProduceAndFastConsume() throws Exception {
		testProduceConsume(true, false);
	}

	@Test
	public void testConcurrentSlowProduceAndSlowConsume() throws Exception {
		testProduceConsume(true, true);
	}

	private void testProduceConsume(boolean isSlowProducer, boolean isSlowConsumer) throws Exception {
		// Config
		final int producerBufferPoolSize = 8;
		final int producerNumberOfBuffersToProduce = 128;

		// Producer behaviour
		final TestProducerSource producerSource = new TestProducerSource() {

			private BufferProvider bufferProvider = new TestPooledBufferProvider(producerBufferPoolSize);

			private int numberOfBuffers;

			@Override
			public BufferOrEvent getNextBufferOrEvent() throws Exception {
				if (numberOfBuffers == producerNumberOfBuffersToProduce) {
					return null;
				}

				final Buffer buffer = bufferProvider.requestBufferBlocking();

				final MemorySegment segment = buffer.getMemorySegment();

				int next = numberOfBuffers * (segment.size() / 4);

				for (int i = 0; i < segment.size(); i += 4) {
					segment.putInt(i, next);

					next++;
				}

				numberOfBuffers++;

				return new BufferOrEvent(buffer, 0);
			}
		};

		// Consumer behaviour
		final TestConsumerCallback consumerCallback = new TestConsumerCallback() {

			private int numberOfBuffers;

			@Override
			public void onBuffer(Buffer buffer) {
				final MemorySegment segment = buffer.getMemorySegment();

				int expected = numberOfBuffers * (segment.size() / 4);

				for (int i = 0; i < segment.size(); i += 4) {
					assertEquals(expected, segment.getInt(i));

					expected++;
				}

				numberOfBuffers++;

				buffer.recycle();
			}

			@Override
			public void onEvent(AbstractEvent event) {
				// Nothing to do in this test
			}
		};

		final PipelinedSubpartition subpartition = createSubpartition();

		TestSubpartitionConsumer consumer = new TestSubpartitionConsumer(isSlowConsumer, consumerCallback);
		final PipelinedSubpartitionView view = subpartition.createReadView(null, consumer);
		consumer.setSubpartitionView(view);

		Future<Boolean> producerResult = executorService.submit(
			new TestSubpartitionProducer(subpartition, isSlowProducer, producerSource));

		Future<Boolean> consumerResult = executorService.submit(consumer);

		// Wait for producer and consumer to finish
		producerResult.get();
		consumerResult.get();
	}
}
