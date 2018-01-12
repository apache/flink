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
import org.apache.flink.core.memory.MemorySegmentFactory;
import org.apache.flink.runtime.event.AbstractEvent;
import org.apache.flink.runtime.io.network.buffer.Buffer;
import org.apache.flink.runtime.io.network.buffer.BufferProvider;
import org.apache.flink.runtime.io.network.buffer.FreeingBufferRecycler;
import org.apache.flink.runtime.io.network.partition.consumer.BufferOrEvent;
import org.apache.flink.runtime.io.network.util.TestConsumerCallback;
import org.apache.flink.runtime.io.network.util.TestPooledBufferProvider;
import org.apache.flink.runtime.io.network.util.TestProducerSource;
import org.apache.flink.runtime.io.network.util.TestSubpartitionConsumer;
import org.apache.flink.runtime.io.network.util.TestSubpartitionProducer;

import org.junit.AfterClass;
import org.junit.Assert;
import org.junit.Test;

import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;

import static org.apache.flink.runtime.io.network.util.TestBufferFactory.BUFFER_SIZE;
import static org.apache.flink.runtime.io.network.util.TestBufferFactory.createBuffer;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;
import static org.mockito.Matchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

public class PipelinedSubpartitionTest extends SubpartitionTestBase {

	/** Executor service for concurrent produce/consume tests */
	private final static ExecutorService executorService = Executors.newCachedThreadPool();

	@AfterClass
	public static void shutdownExecutorService() throws Exception {
		executorService.shutdownNow();
	}

	@Override
	PipelinedSubpartition createSubpartition() {
		final ResultPartition parent = mock(ResultPartition.class);

		return new PipelinedSubpartition(0, parent);
	}

	@Test
	public void testIllegalReadViewRequest() throws Exception {
		final PipelinedSubpartition subpartition = createSubpartition();

		// Successful request
		assertNotNull(subpartition.createReadView(new BufferAvailabilityListener() {
			@Override
			public void notifyBuffersAvailable(long numBuffers) {
			}
		}));

		try {
			subpartition.createReadView(new BufferAvailabilityListener() {
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

		ResultSubpartitionView view = subpartition.createReadView(listener);

		// Empty => should return null
		assertNull(view.getNextBuffer());
		verify(listener, times(1)).notifyBuffersAvailable(eq(0L));

		// Add data to the queue...
		subpartition.add(createBuffer());
		assertEquals(1, subpartition.getTotalNumberOfBuffers());
		assertEquals(BUFFER_SIZE, subpartition.getTotalNumberOfBytes());

		// ...should have resulted in a notification
		verify(listener, times(1)).notifyBuffersAvailable(eq(1L));

		// ...and one available result
		assertNotNull(view.getNextBuffer());
		assertNull(view.getNextBuffer());

		// Add data to the queue...
		subpartition.add(createBuffer());
		assertEquals(2, subpartition.getTotalNumberOfBuffers());
		assertEquals(2 * BUFFER_SIZE, subpartition.getTotalNumberOfBytes());
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

	/**
	 * Verifies that the isReleased() check of the view checks the parent
	 * subpartition.
	 */
	@Test
	public void testIsReleasedChecksParent() throws Exception {
		PipelinedSubpartition subpartition = mock(PipelinedSubpartition.class);

		PipelinedSubpartitionView reader = new PipelinedSubpartitionView(
				subpartition, mock(BufferAvailabilityListener.class));

		assertFalse(reader.isReleased());
		verify(subpartition, times(1)).isReleased();

		when(subpartition.isReleased()).thenReturn(true);
		assertTrue(reader.isReleased());
		verify(subpartition, times(2)).isReleased();
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
		final PipelinedSubpartitionView view = subpartition.createReadView(consumer);
		consumer.setSubpartitionView(view);

		Future<Boolean> producerResult = executorService.submit(
			new TestSubpartitionProducer(subpartition, isSlowProducer, producerSource));

		Future<Boolean> consumerResult = executorService.submit(consumer);

		// Wait for producer and consumer to finish
		producerResult.get();
		consumerResult.get();
	}


	/**
	 * Tests cleanup of {@link PipelinedSubpartition#release()} with no read view attached.
	 */
	@Test
	public void testCleanupReleasedPartitionNoView() throws Exception {
		testCleanupReleasedPartition(false);
	}

	/**
	 * Tests cleanup of {@link PipelinedSubpartition#release()} with a read view attached.
	 */
	@Test
	public void testCleanupReleasedPartitionWithView() throws Exception {
		testCleanupReleasedPartition(true);
	}

	/**
	 * Tests cleanup of {@link PipelinedSubpartition#release()}.
	 *
	 * @param createView
	 * 		whether the partition should have a view attached to it (<tt>true</tt>) or not (<tt>false</tt>)
	 */
	private void testCleanupReleasedPartition(boolean createView) throws Exception {
		PipelinedSubpartition partition = createSubpartition();

		Buffer buffer1 = new Buffer(MemorySegmentFactory.allocateUnpooledSegment(4096),
			FreeingBufferRecycler.INSTANCE);
		Buffer buffer2 = new Buffer(MemorySegmentFactory.allocateUnpooledSegment(4096),
			FreeingBufferRecycler.INSTANCE);
		boolean buffer1Recycled;
		boolean buffer2Recycled;
		try {
			partition.add(buffer1);
			partition.add(buffer2);
			// create the read view first
			ResultSubpartitionView view = null;
			if (createView) {
				view = partition.createReadView(numBuffers -> {});
			}

			partition.release();

			assertTrue(partition.isReleased());
			if (createView) {
				assertTrue(view.isReleased());
			}
			assertTrue(buffer1.isRecycled());
		} finally {
			buffer1Recycled = buffer1.isRecycled();
			if (!buffer1Recycled) {
				buffer1.recycle();
			}
			buffer2Recycled = buffer2.isRecycled();
			if (!buffer2Recycled) {
				buffer2.recycle();
			}
		}
		if (!buffer1Recycled) {
			Assert.fail("buffer 1 not recycled");
		}
		if (!buffer2Recycled) {
			Assert.fail("buffer 2 not recycled");
		}
		assertEquals(2, partition.getTotalNumberOfBuffers());
		assertEquals(2 * 4096, partition.getTotalNumberOfBytes());
	}
}
