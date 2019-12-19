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

import org.apache.flink.core.fs.CloseableRegistry;
import org.apache.flink.runtime.event.AbstractEvent;
import org.apache.flink.runtime.io.disk.NoOpFileChannelManager;
import org.apache.flink.runtime.io.network.api.serialization.EventSerializer;
import org.apache.flink.runtime.io.network.buffer.BufferBuilder;

import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

import javax.annotation.Nullable;

import java.io.IOException;

import static org.apache.flink.runtime.io.network.buffer.BufferBuilderTestUtils.createBufferBuilder;
import static org.apache.flink.runtime.io.network.buffer.BufferBuilderTestUtils.createEventBufferConsumer;
import static org.apache.flink.runtime.io.network.buffer.BufferBuilderTestUtils.createFilledFinishedBufferConsumer;
import static org.apache.flink.runtime.io.network.buffer.BufferBuilderTestUtils.createFilledUnfinishedBufferConsumer;
import static org.apache.flink.runtime.io.network.util.TestBufferFactory.BUFFER_SIZE;
import static org.apache.flink.util.Preconditions.checkArgument;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.greaterThan;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.core.IsInstanceOf.instanceOf;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;

/**
 * Additional tests for {@link PipelinedSubpartition} which require an availability listener and a
 * read view.
 *
 * @see PipelinedSubpartitionTest
 */
@RunWith(Parameterized.class)
public class PipelinedSubpartitionWithReadViewTest {

	private PipelinedSubpartition subpartition;
	private AwaitableBufferAvailablityListener availablityListener;
	private PipelinedSubpartitionView readView;

	@Parameterized.Parameter
	public boolean compressionEnabled;

	@Parameterized.Parameters(name = "compressionEnabled = {0}")
	public static Boolean[] parameters() {
		return new Boolean[] {false, true};
	}

	@Before
	public void setup() throws IOException {
		final ResultPartition parent = PartitionTestUtils.createPartition(
			ResultPartitionType.PIPELINED,
			NoOpFileChannelManager.INSTANCE,
			compressionEnabled,
			BUFFER_SIZE);
		subpartition = new PipelinedSubpartition(0, parent);
		availablityListener = new AwaitableBufferAvailablityListener();
		readView = subpartition.createReadView(availablityListener);
	}

	@After
	public void tearDown() {
		readView.releaseAllResources();
		subpartition.release();
	}

	@Test(expected = IllegalStateException.class)
	public void testAddTwoNonFinishedBuffer() {
		subpartition.add(createBufferBuilder().createBufferConsumer());
		subpartition.add(createBufferBuilder().createBufferConsumer());
		assertNull(readView.getNextBuffer());
	}

	@Test
	public void testAddEmptyNonFinishedBuffer() {
		assertEquals(0, availablityListener.getNumNotifications());

		BufferBuilder bufferBuilder = createBufferBuilder();
		subpartition.add(bufferBuilder.createBufferConsumer());

		assertEquals(0, availablityListener.getNumNotifications());
		assertNull(readView.getNextBuffer());

		bufferBuilder.finish();
		bufferBuilder = createBufferBuilder();
		subpartition.add(bufferBuilder.createBufferConsumer());

		assertEquals(1, subpartition.getBuffersInBacklog());
		assertEquals(1, availablityListener.getNumNotifications()); // notification from finishing previous buffer.
		assertNull(readView.getNextBuffer());
		assertEquals(0, subpartition.getBuffersInBacklog());
	}

	@Test
	public void testAddNonEmptyNotFinishedBuffer() throws Exception {
		assertEquals(0, availablityListener.getNumNotifications());

		subpartition.add(createFilledUnfinishedBufferConsumer(1024));

		// note that since the buffer builder is not finished, there is still a retained instance!
		assertEquals(0, subpartition.getBuffersInBacklog());
		assertNextBuffer(readView, 1024, false, 0, false, false);
	}

	/**
	 * Normally moreAvailable flag from InputChannel should ignore non finished BufferConsumers, otherwise we would
	 * busy loop on the unfinished BufferConsumers.
	 */
	@Test
	public void testUnfinishedBufferBehindFinished() throws Exception {
		subpartition.add(createFilledFinishedBufferConsumer(1025)); // finished
		subpartition.add(createFilledUnfinishedBufferConsumer(1024)); // not finished

		assertEquals(1, subpartition.getBuffersInBacklog());
		assertThat(availablityListener.getNumNotifications(), greaterThan(0L));
		assertNextBuffer(readView, 1025, false, 0, false, true);
		// not notified, but we could still access the unfinished buffer
		assertNextBuffer(readView, 1024, false, 0, false, false);
		assertNoNextBuffer(readView);
	}

	/**
	 * After flush call unfinished BufferConsumers should be reported as available, otherwise we might not flush some
	 * of the data.
	 */
	@Test
	public void testFlushWithUnfinishedBufferBehindFinished() throws Exception {
		subpartition.add(createFilledFinishedBufferConsumer(1025)); // finished
		subpartition.add(createFilledUnfinishedBufferConsumer(1024)); // not finished
		long oldNumNotifications = availablityListener.getNumNotifications();

		assertEquals(1, subpartition.getBuffersInBacklog());

		subpartition.flush();
		// buffer queue is > 1, should already be notified, no further notification necessary
		assertThat(oldNumNotifications, greaterThan(0L));
		assertEquals(oldNumNotifications, availablityListener.getNumNotifications());

		assertEquals(2, subpartition.getBuffersInBacklog());
		assertNextBuffer(readView, 1025, true, 1, false, true);
		assertNextBuffer(readView, 1024, false, 0, false, false);
		assertNoNextBuffer(readView);
	}

	/**
	 * A flush call with a buffer size of 1 should always notify consumers (unless already flushed).
	 */
	@Test
	public void testFlushWithUnfinishedBufferBehindFinished2() throws Exception {
		// no buffers -> no notification or any other effects
		subpartition.flush();
		assertEquals(0, availablityListener.getNumNotifications());

		subpartition.add(createFilledFinishedBufferConsumer(1025)); // finished
		subpartition.add(createFilledUnfinishedBufferConsumer(1024)); // not finished

		assertEquals(1, subpartition.getBuffersInBacklog());
		assertNextBuffer(readView, 1025, false, 0, false, true);

		long oldNumNotifications = availablityListener.getNumNotifications();
		subpartition.flush();
		// buffer queue is 1 again -> need to flush
		assertEquals(oldNumNotifications + 1, availablityListener.getNumNotifications());
		subpartition.flush();
		// calling again should not flush again
		assertEquals(oldNumNotifications + 1, availablityListener.getNumNotifications());

		assertEquals(1, subpartition.getBuffersInBacklog());
		assertNextBuffer(readView, 1024, false, 0, false, false);
		assertNoNextBuffer(readView);
	}

	@Test
	public void testMultipleEmptyBuffers() throws Exception {
		assertEquals(0, availablityListener.getNumNotifications());

		subpartition.add(createFilledFinishedBufferConsumer(0));
		assertEquals(0, availablityListener.getNumNotifications());

		subpartition.add(createFilledFinishedBufferConsumer(0));
		assertEquals(1, availablityListener.getNumNotifications());

		subpartition.add(createFilledFinishedBufferConsumer(0));
		assertEquals(1, availablityListener.getNumNotifications());
		assertEquals(2, subpartition.getBuffersInBacklog());

		subpartition.add(createFilledFinishedBufferConsumer(1024));
		assertEquals(1, availablityListener.getNumNotifications());

		assertNextBuffer(readView, 1024, false, 0, false, true);
	}

	@Test
	public void testEmptyFlush()  {
		subpartition.flush();
		assertEquals(0, availablityListener.getNumNotifications());
	}

	@Test
	public void testBasicPipelinedProduceConsumeLogic() throws Exception {
		// Empty => should return null
		assertFalse(readView.nextBufferIsEvent());
		assertNoNextBuffer(readView);
		assertFalse(readView.nextBufferIsEvent()); // also after getNextBuffer()
		assertEquals(0, availablityListener.getNumNotifications());

		// Add data to the queue...
		subpartition.add(createFilledFinishedBufferConsumer(BUFFER_SIZE));
		assertFalse(readView.nextBufferIsEvent());

		assertEquals(1, subpartition.getTotalNumberOfBuffers());
		assertEquals(0, subpartition.getBuffersInBacklog());
		assertEquals(0, subpartition.getTotalNumberOfBytes()); // only updated when getting the buffer

		assertEquals(0, availablityListener.getNumNotifications());

		// ...and one available result
		assertNextBuffer(readView, BUFFER_SIZE, false, 0, false, true);
		assertEquals(BUFFER_SIZE, subpartition.getTotalNumberOfBytes()); // only updated when getting the buffer
		assertEquals(0, subpartition.getBuffersInBacklog());
		assertNoNextBuffer(readView);
		assertEquals(0, subpartition.getBuffersInBacklog());

		// Add data to the queue...
		subpartition.add(createFilledFinishedBufferConsumer(BUFFER_SIZE));
		assertFalse(readView.nextBufferIsEvent());

		assertEquals(2, subpartition.getTotalNumberOfBuffers());
		assertEquals(0, subpartition.getBuffersInBacklog());
		assertEquals(BUFFER_SIZE, subpartition.getTotalNumberOfBytes()); // only updated when getting the buffer
		assertEquals(0, availablityListener.getNumNotifications());

		assertNextBuffer(readView, BUFFER_SIZE, false, 0, false, true);
		assertEquals(2 * BUFFER_SIZE, subpartition.getTotalNumberOfBytes()); // only updated when getting the buffer
		assertEquals(0, subpartition.getBuffersInBacklog());
		assertNoNextBuffer(readView);
		assertEquals(0, subpartition.getBuffersInBacklog());

		// some tests with events

		// fill with: buffer, event, and buffer
		subpartition.add(createFilledFinishedBufferConsumer(BUFFER_SIZE));
		assertFalse(readView.nextBufferIsEvent());
		subpartition.add(createEventBufferConsumer(BUFFER_SIZE));
		assertFalse(readView.nextBufferIsEvent());
		subpartition.add(createFilledFinishedBufferConsumer(BUFFER_SIZE));
		assertFalse(readView.nextBufferIsEvent());

		assertEquals(5, subpartition.getTotalNumberOfBuffers());
		assertEquals(1, subpartition.getBuffersInBacklog()); // two buffers (events don't count)
		assertEquals(2 * BUFFER_SIZE, subpartition.getTotalNumberOfBytes()); // only updated when getting the buffer
		assertEquals(1, availablityListener.getNumNotifications());

		// the first buffer
		assertNextBuffer(readView, BUFFER_SIZE, true, 0, true, true);
		assertEquals(3 * BUFFER_SIZE, subpartition.getTotalNumberOfBytes()); // only updated when getting the buffer
		assertEquals(0, subpartition.getBuffersInBacklog());

		// the event
		assertNextEvent(readView, BUFFER_SIZE, null, false, 0, false, true);
		assertEquals(4 * BUFFER_SIZE, subpartition.getTotalNumberOfBytes()); // only updated when getting the buffer
		assertEquals(0, subpartition.getBuffersInBacklog());

		// the remaining buffer
		assertNextBuffer(readView, BUFFER_SIZE, false, 0, false, true);
		assertEquals(5 * BUFFER_SIZE, subpartition.getTotalNumberOfBytes()); // only updated when getting the buffer
		assertEquals(0, subpartition.getBuffersInBacklog());

		// nothing more
		assertNoNextBuffer(readView);
		assertEquals(0, subpartition.getBuffersInBacklog());

		assertEquals(5, subpartition.getTotalNumberOfBuffers());
		assertEquals(5 * BUFFER_SIZE, subpartition.getTotalNumberOfBytes());
		assertEquals(1, availablityListener.getNumNotifications());
	}

	@Test
	public void testBacklogConsistentWithNumberOfConsumableBuffers() throws Exception {
		testBacklogConsistentWithNumberOfConsumableBuffers(false, false);
	}

	@Test
	public void testBacklogConsistentWithConsumableBuffersForFlushedPartition() throws Exception {
		testBacklogConsistentWithNumberOfConsumableBuffers(true, false);
	}

	@Test
	public void testBacklogConsistentWithConsumableBuffersForFinishedPartition() throws Exception {
		testBacklogConsistentWithNumberOfConsumableBuffers(false, true);
	}

	private void testBacklogConsistentWithNumberOfConsumableBuffers(boolean isFlushRequested, boolean isFinished) throws Exception {
		final int numberOfAddedBuffers = 5;

		for (int i = 1; i <= numberOfAddedBuffers; i++) {
			if (i < numberOfAddedBuffers || isFinished) {
				subpartition.add(createFilledFinishedBufferConsumer(1024));
			} else {
				subpartition.add(createFilledUnfinishedBufferConsumer(1024));
			}
		}

		if (isFlushRequested) {
			subpartition.flush();
		}

		if (isFinished) {
			subpartition.finish();
		}

		final int backlog = subpartition.getBuffersInBacklog();

		int numberOfConsumableBuffers = 0;
		try (final CloseableRegistry closeableRegistry = new CloseableRegistry()) {
			while (readView.isAvailable()) {
				ResultSubpartition.BufferAndBacklog bufferAndBacklog = readView.getNextBuffer();
				assertNotNull(bufferAndBacklog);

				if (bufferAndBacklog.buffer().isBuffer()) {
					++numberOfConsumableBuffers;
				}

				closeableRegistry.registerCloseable(bufferAndBacklog.buffer() :: recycleBuffer);
			}

			assertThat(backlog, is(numberOfConsumableBuffers));
		}
	}

	// ------------------------------------------------------------------------

	static void assertNextBuffer(
			ResultSubpartitionView readView,
			int expectedReadableBufferSize,
			boolean expectedIsMoreAvailable,
			int expectedBuffersInBacklog,
			boolean expectedNextBufferIsEvent,
			boolean expectedRecycledAfterRecycle) throws IOException, InterruptedException {
		assertNextBufferOrEvent(
				readView,
				expectedReadableBufferSize,
				true,
				null,
				expectedIsMoreAvailable,
				expectedBuffersInBacklog,
				expectedNextBufferIsEvent,
				expectedRecycledAfterRecycle);
	}

	static void assertNextEvent(
			ResultSubpartitionView readView,
			int expectedReadableBufferSize,
			Class<? extends AbstractEvent> expectedEventClass,
			boolean expectedIsMoreAvailable,
			int expectedBuffersInBacklog,
			boolean expectedNextBufferIsEvent,
			boolean expectedRecycledAfterRecycle) throws IOException, InterruptedException {
		assertNextBufferOrEvent(
				readView,
				expectedReadableBufferSize,
				false,
				expectedEventClass,
				expectedIsMoreAvailable,
				expectedBuffersInBacklog,
				expectedNextBufferIsEvent,
				expectedRecycledAfterRecycle);
	}

	private static void assertNextBufferOrEvent(
			ResultSubpartitionView readView,
			int expectedReadableBufferSize,
			boolean expectedIsBuffer,
			@Nullable Class<? extends AbstractEvent> expectedEventClass,
			boolean expectedIsMoreAvailable,
			int expectedBuffersInBacklog,
			boolean expectedNextBufferIsEvent,
			boolean expectedRecycledAfterRecycle) throws IOException, InterruptedException {
		checkArgument(expectedEventClass == null || !expectedIsBuffer);

		ResultSubpartition.BufferAndBacklog bufferAndBacklog = readView.getNextBuffer();
		assertNotNull(bufferAndBacklog);
		try {
			assertEquals("buffer size", expectedReadableBufferSize,
					bufferAndBacklog.buffer().readableBytes());
			assertEquals("buffer or event", expectedIsBuffer,
					bufferAndBacklog.buffer().isBuffer());
			if (expectedEventClass != null) {
				Assert.assertThat(EventSerializer
								.fromBuffer(bufferAndBacklog.buffer(), ClassLoader.getSystemClassLoader()),
						instanceOf(expectedEventClass));
			}
			assertEquals("more available", expectedIsMoreAvailable,
					bufferAndBacklog.isMoreAvailable());
			assertEquals("more available", expectedIsMoreAvailable, readView.isAvailable());
			assertEquals("backlog", expectedBuffersInBacklog, bufferAndBacklog.buffersInBacklog());
			assertEquals("next is event", expectedNextBufferIsEvent,
					bufferAndBacklog.nextBufferIsEvent());
			assertEquals("next is event", expectedNextBufferIsEvent,
					readView.nextBufferIsEvent());

			assertFalse("not recycled", bufferAndBacklog.buffer().isRecycled());
		} finally {
			bufferAndBacklog.buffer().recycleBuffer();
		}
		assertEquals("recycled", expectedRecycledAfterRecycle, bufferAndBacklog.buffer().isRecycled());
	}

	static void assertNoNextBuffer(ResultSubpartitionView readView) throws IOException, InterruptedException {
		assertNull(readView.getNextBuffer());
	}
}
