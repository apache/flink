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
import org.apache.flink.runtime.io.network.buffer.BufferBuilder;
import org.apache.flink.runtime.io.network.buffer.BufferConsumer;
import org.apache.flink.runtime.io.network.buffer.BufferProvider;
import org.apache.flink.runtime.io.network.partition.ResultSubpartition.BufferAndBacklog;
import org.apache.flink.runtime.io.network.util.TestConsumerCallback;
import org.apache.flink.runtime.io.network.util.TestPooledBufferProvider;
import org.apache.flink.runtime.io.network.util.TestProducerSource;
import org.apache.flink.runtime.io.network.util.TestSubpartitionConsumer;
import org.apache.flink.runtime.io.network.util.TestSubpartitionProducer;

import org.junit.AfterClass;
import org.junit.Assert;
import org.junit.Test;

import java.nio.ByteBuffer;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;

import static org.apache.flink.runtime.io.network.buffer.BufferBuilderTestUtils.createBufferBuilder;
import static org.apache.flink.runtime.io.network.buffer.BufferBuilderTestUtils.createEventBufferConsumer;
import static org.apache.flink.runtime.io.network.buffer.BufferBuilderTestUtils.createFilledBufferBuilder;
import static org.apache.flink.runtime.io.network.buffer.BufferBuilderTestUtils.createFilledBufferConsumer;
import static org.apache.flink.runtime.io.network.util.TestBufferFactory.BUFFER_SIZE;
import static org.apache.flink.util.FutureUtil.waitForAll;
import static org.apache.flink.util.Preconditions.checkState;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;
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

	@Test(expected = IllegalStateException.class)
	public void testAddTwoNonFinishedBuffer() throws Exception {
		final ResultSubpartition subpartition = createSubpartition();
		AwaitableBufferAvailablityListener availablityListener = new AwaitableBufferAvailablityListener();
		ResultSubpartitionView readView = subpartition.createReadView(availablityListener);
		availablityListener.resetNotificationCounters();

		try {
			subpartition.add(createBufferBuilder().createBufferConsumer());
			subpartition.add(createBufferBuilder().createBufferConsumer());
			assertNull(readView.getNextBuffer());
		} finally {
			subpartition.release();
		}
	}

	@Test
	public void testAddEmptyNonFinishedBuffer() throws Exception {
		final ResultSubpartition subpartition = createSubpartition();
		AwaitableBufferAvailablityListener availablityListener = new AwaitableBufferAvailablityListener();
		ResultSubpartitionView readView = subpartition.createReadView(availablityListener);
		availablityListener.resetNotificationCounters();

		try {
			assertEquals(0, availablityListener.getNumNotifications());

			BufferBuilder bufferBuilder = createBufferBuilder();
			subpartition.add(bufferBuilder.createBufferConsumer());

			assertEquals(0, availablityListener.getNumNotifications());
			assertNull(readView.getNextBuffer());

			bufferBuilder.finish();
			bufferBuilder = createBufferBuilder();
			subpartition.add(bufferBuilder.createBufferConsumer());

			assertEquals(1, availablityListener.getNumNotifications()); // notification from finishing previous buffer.
			assertNull(readView.getNextBuffer());
			assertEquals(1, subpartition.getBuffersInBacklog());
		} finally {
			readView.releaseAllResources();
			subpartition.release();
		}
	}

	@Test
	public void testAddNonEmptyNotFinishedBuffer() throws Exception {
		final ResultSubpartition subpartition = createSubpartition();
		AwaitableBufferAvailablityListener availablityListener = new AwaitableBufferAvailablityListener();
		ResultSubpartitionView readView = subpartition.createReadView(availablityListener);
		availablityListener.resetNotificationCounters();

		try {
			assertEquals(0, availablityListener.getNumNotifications());

			BufferBuilder bufferBuilder = createBufferBuilder();
			bufferBuilder.appendAndCommit(ByteBuffer.allocate(1024));
			subpartition.add(bufferBuilder.createBufferConsumer());

			// note that since the buffer builder is not finished, there is still a retained instance!
			assertNextBuffer(readView, 1024, false, 1, false, false);
			assertEquals(1, subpartition.getBuffersInBacklog());
		} finally {
			readView.releaseAllResources();
			subpartition.release();
		}
	}

	/**
	 * Normally moreAvailable flag from InputChannel should ignore non finished BufferConsumers, otherwise we would
	 * busy loop on the unfinished BufferConsumers.
	 */
	@Test
	public void testUnfinishedBufferBehindFinished() throws Exception {
		final ResultSubpartition subpartition = createSubpartition();
		AwaitableBufferAvailablityListener availablityListener = new AwaitableBufferAvailablityListener();
		ResultSubpartitionView readView = subpartition.createReadView(availablityListener);

		try {
			subpartition.add(createFilledBufferConsumer(1025)); // finished
			subpartition.add(createFilledBufferBuilder(1024).createBufferConsumer()); // not finished

			assertNextBuffer(readView, 1025, false, 1, false, true);
		} finally {
			subpartition.release();
		}
	}

	/**
	 * After flush call unfinished BufferConsumers should be reported as available, otherwise we might not flush some
	 * of the data.
	 */
	@Test
	public void testFlushWithUnfinishedBufferBehindFinished() throws Exception {
		final ResultSubpartition subpartition = createSubpartition();
		AwaitableBufferAvailablityListener availablityListener = new AwaitableBufferAvailablityListener();
		ResultSubpartitionView readView = subpartition.createReadView(availablityListener);

		try {
			subpartition.add(createFilledBufferConsumer(1025)); // finished
			subpartition.add(createFilledBufferBuilder(1024).createBufferConsumer()); // not finished
			subpartition.flush();

			assertNextBuffer(readView, 1025, true, 1, false, true);
			assertNextBuffer(readView, 1024, false, 1, false, false);
		} finally {
			subpartition.release();
		}
	}

	@Test
	public void testMultipleEmptyBuffers() throws Exception {
		final ResultSubpartition subpartition = createSubpartition();
		AwaitableBufferAvailablityListener availablityListener = new AwaitableBufferAvailablityListener();
		ResultSubpartitionView readView = subpartition.createReadView(availablityListener);
		availablityListener.resetNotificationCounters();

		try {
			assertEquals(0, availablityListener.getNumNotifications());

			subpartition.add(createFilledBufferConsumer(0));

			assertEquals(1, availablityListener.getNumNotifications());
			subpartition.add(createFilledBufferConsumer(0));
			assertEquals(2, availablityListener.getNumNotifications());

			subpartition.add(createFilledBufferConsumer(0));
			assertEquals(2, availablityListener.getNumNotifications());
			assertEquals(3, subpartition.getBuffersInBacklog());

			subpartition.add(createFilledBufferConsumer(1024));
			assertEquals(2, availablityListener.getNumNotifications());

			assertNextBuffer(readView, 1024, false, 0, false, true);
		} finally {
			readView.releaseAllResources();
			subpartition.release();
		}
	}

	@Test
	public void testIllegalReadViewRequest() throws Exception {
		final PipelinedSubpartition subpartition = createSubpartition();

		// Successful request
		assertNotNull(subpartition.createReadView(new NoOpBufferAvailablityListener()));

		try {
			subpartition.createReadView(new NoOpBufferAvailablityListener());

			fail("Did not throw expected exception after duplicate notifyNonEmpty view request.");
		} catch (IllegalStateException expected) {
		}
	}

	@Test
	public void testEmptyFlush() throws Exception {
		final PipelinedSubpartition subpartition = createSubpartition();

		AwaitableBufferAvailablityListener listener = new AwaitableBufferAvailablityListener();
		subpartition.createReadView(listener);
		subpartition.flush();
		assertEquals(0, listener.getNumNotifications());
	}

	@Test
	public void testBasicPipelinedProduceConsumeLogic() throws Exception {
		final PipelinedSubpartition subpartition = createSubpartition();

		BufferAvailabilityListener listener = mock(BufferAvailabilityListener.class);

		ResultSubpartitionView view = subpartition.createReadView(listener);

		// Empty => should return null
		assertFalse(view.nextBufferIsEvent());
		assertNull(view.getNextBuffer());
		assertFalse(view.nextBufferIsEvent()); // also after getNextBuffer()
		verify(listener, times(0)).notifyDataAvailable();

		// Add data to the queue...
		subpartition.add(createFilledBufferConsumer(BUFFER_SIZE));
		assertFalse(view.nextBufferIsEvent());

		assertEquals(1, subpartition.getTotalNumberOfBuffers());
		assertEquals(1, subpartition.getBuffersInBacklog());
		assertEquals(0, subpartition.getTotalNumberOfBytes()); // only updated when getting the buffer

		// ...should have resulted in a notification
		verify(listener, times(1)).notifyDataAvailable();

		// ...and one available result
		assertFalse(view.nextBufferIsEvent());
		BufferAndBacklog read = view.getNextBuffer();
		assertNotNull(read);
		assertTrue(read.buffer().isBuffer());
		assertEquals(BUFFER_SIZE, subpartition.getTotalNumberOfBytes()); // only updated when getting the buffer
		assertEquals(0, subpartition.getBuffersInBacklog());
		assertEquals(subpartition.getBuffersInBacklog(), read.buffersInBacklog());
		assertFalse(read.nextBufferIsEvent());
		assertFalse(view.nextBufferIsEvent());
		assertNull(view.getNextBuffer());
		assertEquals(0, subpartition.getBuffersInBacklog());

		// Add data to the queue...
		subpartition.add(createFilledBufferConsumer(BUFFER_SIZE));
		assertFalse(view.nextBufferIsEvent());

		assertEquals(2, subpartition.getTotalNumberOfBuffers());
		assertEquals(1, subpartition.getBuffersInBacklog());
		assertEquals(BUFFER_SIZE, subpartition.getTotalNumberOfBytes()); // only updated when getting the buffer
		verify(listener, times(2)).notifyDataAvailable();

		assertFalse(view.nextBufferIsEvent());
		read = view.getNextBuffer();
		assertNotNull(read);
		assertTrue(read.buffer().isBuffer());
		assertEquals(2 * BUFFER_SIZE, subpartition.getTotalNumberOfBytes()); // only updated when getting the buffer
		assertEquals(0, subpartition.getBuffersInBacklog());
		assertEquals(subpartition.getBuffersInBacklog(), read.buffersInBacklog());
		assertFalse(read.nextBufferIsEvent());
		assertFalse(view.nextBufferIsEvent());
		assertNull(view.getNextBuffer());
		assertEquals(0, subpartition.getBuffersInBacklog());

		// some tests with events

		// fill with: buffer, event , and buffer
		subpartition.add(createFilledBufferConsumer(BUFFER_SIZE));
		assertFalse(view.nextBufferIsEvent());
		subpartition.add(createEventBufferConsumer(BUFFER_SIZE));
		assertFalse(view.nextBufferIsEvent());
		subpartition.add(createFilledBufferConsumer(BUFFER_SIZE));
		assertFalse(view.nextBufferIsEvent());

		assertEquals(5, subpartition.getTotalNumberOfBuffers());
		assertEquals(2, subpartition.getBuffersInBacklog()); // two buffers (events don't count)
		assertEquals(2 * BUFFER_SIZE, subpartition.getTotalNumberOfBytes()); // only updated when getting the buffer
		verify(listener, times(4)).notifyDataAvailable();

		assertFalse(view.nextBufferIsEvent()); // the first buffer
		read = view.getNextBuffer();
		assertNotNull(read);
		assertTrue(read.buffer().isBuffer());
		assertEquals(3 * BUFFER_SIZE, subpartition.getTotalNumberOfBytes()); // only updated when getting the buffer
		assertEquals(1, subpartition.getBuffersInBacklog());
		assertEquals(subpartition.getBuffersInBacklog(), read.buffersInBacklog());
		assertTrue(read.nextBufferIsEvent());

		assertTrue(view.nextBufferIsEvent()); // the event
		read = view.getNextBuffer();
		assertNotNull(read);
		assertFalse(read.buffer().isBuffer());
		assertEquals(4 * BUFFER_SIZE, subpartition.getTotalNumberOfBytes()); // only updated when getting the buffer
		assertEquals(1, subpartition.getBuffersInBacklog());
		assertEquals(subpartition.getBuffersInBacklog(), read.buffersInBacklog());
		assertFalse(read.nextBufferIsEvent());

		assertFalse(view.nextBufferIsEvent()); // the remaining buffer
		read = view.getNextBuffer();
		assertNotNull(read);
		assertTrue(read.buffer().isBuffer());
		assertEquals(5 * BUFFER_SIZE, subpartition.getTotalNumberOfBytes()); // only updated when getting the buffer
		assertEquals(0, subpartition.getBuffersInBacklog());
		assertEquals(subpartition.getBuffersInBacklog(), read.buffersInBacklog());
		assertFalse(read.nextBufferIsEvent());

		assertEquals(5, subpartition.getTotalNumberOfBuffers());
		assertEquals(5 * BUFFER_SIZE, subpartition.getTotalNumberOfBytes());
		verify(listener, times(4)).notifyDataAvailable();
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
			public BufferConsumerAndChannel getNextBufferConsumer() throws Exception {
				if (numberOfBuffers == producerNumberOfBuffersToProduce) {
					return null;
				}

				final BufferBuilder bufferBuilder = bufferProvider.requestBufferBuilderBlocking();
				int segmentSize = bufferBuilder.getMaxCapacity();

				MemorySegment segment = MemorySegmentFactory.allocateUnpooledSegment(segmentSize);

				int next = numberOfBuffers * (segmentSize / Integer.BYTES);

				for (int i = 0; i < segmentSize; i += 4) {
					segment.putInt(i, next);
					next++;
				}

				checkState(bufferBuilder.appendAndCommit(ByteBuffer.wrap(segment.getArray())) == segmentSize);
				bufferBuilder.finish();

				numberOfBuffers++;

				return new BufferConsumerAndChannel(bufferBuilder.createBufferConsumer(), 0);
			}
		};

		// Consumer behaviour
		final TestConsumerCallback consumerCallback = new TestConsumerCallback() {

			private int numberOfBuffers;

			@Override
			public void onBuffer(Buffer buffer) {
				final MemorySegment segment = buffer.getMemorySegment();
				assertEquals(segment.size(), buffer.getSize());

				int expected = numberOfBuffers * (segment.size() / 4);

				for (int i = 0; i < segment.size(); i += 4) {
					assertEquals(expected, segment.getInt(i));

					expected++;
				}

				numberOfBuffers++;

				buffer.recycleBuffer();
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

		waitForAll(60_000L, producerResult, consumerResult);
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

		BufferConsumer buffer1 = createFilledBufferConsumer(4096);
		BufferConsumer buffer2 = createFilledBufferConsumer(4096);
		boolean buffer1Recycled;
		boolean buffer2Recycled;
		try {
			partition.add(buffer1);
			partition.add(buffer2);
			// create the read view first
			ResultSubpartitionView view = null;
			if (createView) {
				view = partition.createReadView(new NoOpBufferAvailablityListener());
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
				buffer1.close();
			}
			buffer2Recycled = buffer2.isRecycled();
			if (!buffer2Recycled) {
				buffer2.close();
			}
		}
		if (!buffer1Recycled) {
			Assert.fail("buffer 1 not recycled");
		}
		if (!buffer2Recycled) {
			Assert.fail("buffer 2 not recycled");
		}
		assertEquals(2, partition.getTotalNumberOfBuffers());
		assertEquals(0, partition.getTotalNumberOfBytes()); // buffer data is never consumed
	}
}
