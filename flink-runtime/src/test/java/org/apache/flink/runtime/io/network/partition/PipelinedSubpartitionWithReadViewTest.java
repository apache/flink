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

import org.apache.flink.runtime.io.network.buffer.BufferBuilder;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.io.IOException;
import java.nio.ByteBuffer;

import static org.apache.flink.runtime.io.network.buffer.BufferBuilderTestUtils.createBufferBuilder;
import static org.apache.flink.runtime.io.network.buffer.BufferBuilderTestUtils.createEventBufferConsumer;
import static org.apache.flink.runtime.io.network.buffer.BufferBuilderTestUtils.createFilledBufferBuilder;
import static org.apache.flink.runtime.io.network.buffer.BufferBuilderTestUtils.createFilledBufferConsumer;
import static org.apache.flink.runtime.io.network.partition.SubpartitionTestBase.assertNextBuffer;
import static org.apache.flink.runtime.io.network.partition.SubpartitionTestBase.assertNextEvent;
import static org.apache.flink.runtime.io.network.partition.SubpartitionTestBase.assertNoNextBuffer;
import static org.apache.flink.runtime.io.network.util.TestBufferFactory.BUFFER_SIZE;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.greaterThan;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNull;
import static org.mockito.Mockito.mock;

/**
 * Additional tests for {@link PipelinedSubpartition} which require an availability listener and a
 * read view.
 *
 * @see PipelinedSubpartitionTest
 */
public class PipelinedSubpartitionWithReadViewTest {

	private PipelinedSubpartition subpartition;
	private AwaitableBufferAvailablityListener availablityListener;
	private PipelinedSubpartitionView readView;

	@Before
	public void setup() throws IOException {
		final ResultPartition parent = mock(ResultPartition.class);
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

		assertEquals(1, availablityListener.getNumNotifications()); // notification from finishing previous buffer.
		assertNull(readView.getNextBuffer());
		assertEquals(1, subpartition.getBuffersInBacklog());
	}

	@Test
	public void testAddNonEmptyNotFinishedBuffer() throws Exception {
		assertEquals(0, availablityListener.getNumNotifications());

		BufferBuilder bufferBuilder = createBufferBuilder();
		bufferBuilder.appendAndCommit(ByteBuffer.allocate(1024));
		subpartition.add(bufferBuilder.createBufferConsumer());

		// note that since the buffer builder is not finished, there is still a retained instance!
		assertNextBuffer(readView, 1024, false, 1, false, false);
		assertEquals(1, subpartition.getBuffersInBacklog());
	}

	/**
	 * Normally moreAvailable flag from InputChannel should ignore non finished BufferConsumers, otherwise we would
	 * busy loop on the unfinished BufferConsumers.
	 */
	@Test
	public void testUnfinishedBufferBehindFinished() throws Exception {
		subpartition.add(createFilledBufferConsumer(1025)); // finished
		subpartition.add(createFilledBufferBuilder(1024).createBufferConsumer()); // not finished

		assertThat(availablityListener.getNumNotifications(), greaterThan(0L));
		assertNextBuffer(readView, 1025, false, 1, false, true);
		// not notified, but we could still access the unfinished buffer
		assertNextBuffer(readView, 1024, false, 1, false, false);
		assertNoNextBuffer(readView);
	}

	/**
	 * After flush call unfinished BufferConsumers should be reported as available, otherwise we might not flush some
	 * of the data.
	 */
	@Test
	public void testFlushWithUnfinishedBufferBehindFinished() throws Exception {
		subpartition.add(createFilledBufferConsumer(1025)); // finished
		subpartition.add(createFilledBufferBuilder(1024).createBufferConsumer()); // not finished
		long oldNumNotifications = availablityListener.getNumNotifications();
		subpartition.flush();
		// buffer queue is > 1, should already be notified, no further notification necessary
		assertThat(oldNumNotifications, greaterThan(0L));
		assertEquals(oldNumNotifications, availablityListener.getNumNotifications());

		assertNextBuffer(readView, 1025, true, 1, false, true);
		assertNextBuffer(readView, 1024, false, 1, false, false);
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

		subpartition.add(createFilledBufferConsumer(1025)); // finished
		subpartition.add(createFilledBufferBuilder(1024).createBufferConsumer()); // not finished

		assertNextBuffer(readView, 1025, false, 1, false, true);

		long oldNumNotifications = availablityListener.getNumNotifications();
		subpartition.flush();
		// buffer queue is 1 again -> need to flush
		assertEquals(oldNumNotifications + 1, availablityListener.getNumNotifications());
		subpartition.flush();
		// calling again should not flush again
		assertEquals(oldNumNotifications + 1, availablityListener.getNumNotifications());

		assertNextBuffer(readView, 1024, false, 1, false, false);
		assertNoNextBuffer(readView);
	}

	@Test
	public void testMultipleEmptyBuffers() throws Exception {
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
		subpartition.add(createFilledBufferConsumer(BUFFER_SIZE));
		assertFalse(readView.nextBufferIsEvent());

		assertEquals(1, subpartition.getTotalNumberOfBuffers());
		assertEquals(1, subpartition.getBuffersInBacklog());
		assertEquals(0, subpartition.getTotalNumberOfBytes()); // only updated when getting the buffer

		// ...should have resulted in a notification
		assertEquals(1, availablityListener.getNumNotifications());

		// ...and one available result
		assertNextBuffer(readView, BUFFER_SIZE, false, subpartition.getBuffersInBacklog() - 1, false, true);
		assertEquals(BUFFER_SIZE, subpartition.getTotalNumberOfBytes()); // only updated when getting the buffer
		assertEquals(0, subpartition.getBuffersInBacklog());
		assertNoNextBuffer(readView);
		assertEquals(0, subpartition.getBuffersInBacklog());

		// Add data to the queue...
		subpartition.add(createFilledBufferConsumer(BUFFER_SIZE));
		assertFalse(readView.nextBufferIsEvent());

		assertEquals(2, subpartition.getTotalNumberOfBuffers());
		assertEquals(1, subpartition.getBuffersInBacklog());
		assertEquals(BUFFER_SIZE, subpartition.getTotalNumberOfBytes()); // only updated when getting the buffer
		assertEquals(2, availablityListener.getNumNotifications());

		assertNextBuffer(readView, BUFFER_SIZE, false, subpartition.getBuffersInBacklog() - 1, false, true);
		assertEquals(2 * BUFFER_SIZE, subpartition.getTotalNumberOfBytes()); // only updated when getting the buffer
		assertEquals(0, subpartition.getBuffersInBacklog());
		assertNoNextBuffer(readView);
		assertEquals(0, subpartition.getBuffersInBacklog());

		// some tests with events

		// fill with: buffer, event, and buffer
		subpartition.add(createFilledBufferConsumer(BUFFER_SIZE));
		assertFalse(readView.nextBufferIsEvent());
		subpartition.add(createEventBufferConsumer(BUFFER_SIZE));
		assertFalse(readView.nextBufferIsEvent());
		subpartition.add(createFilledBufferConsumer(BUFFER_SIZE));
		assertFalse(readView.nextBufferIsEvent());

		assertEquals(5, subpartition.getTotalNumberOfBuffers());
		assertEquals(2, subpartition.getBuffersInBacklog()); // two buffers (events don't count)
		assertEquals(2 * BUFFER_SIZE, subpartition.getTotalNumberOfBytes()); // only updated when getting the buffer
		assertEquals(4, availablityListener.getNumNotifications());

		// the first buffer
		assertNextBuffer(readView, BUFFER_SIZE, true, subpartition.getBuffersInBacklog() - 1, true, true);
		assertEquals(3 * BUFFER_SIZE, subpartition.getTotalNumberOfBytes()); // only updated when getting the buffer
		assertEquals(1, subpartition.getBuffersInBacklog());

		// the event
		assertNextEvent(readView, BUFFER_SIZE, null, true, subpartition.getBuffersInBacklog(), false, true);
		assertEquals(4 * BUFFER_SIZE, subpartition.getTotalNumberOfBytes()); // only updated when getting the buffer
		assertEquals(1, subpartition.getBuffersInBacklog());

		// the remaining buffer
		assertNextBuffer(readView, BUFFER_SIZE, false, subpartition.getBuffersInBacklog() - 1, false, true);
		assertEquals(5 * BUFFER_SIZE, subpartition.getTotalNumberOfBytes()); // only updated when getting the buffer
		assertEquals(0, subpartition.getBuffersInBacklog());

		// nothing more
		assertNoNextBuffer(readView);
		assertEquals(0, subpartition.getBuffersInBacklog());

		assertEquals(5, subpartition.getTotalNumberOfBuffers());
		assertEquals(5 * BUFFER_SIZE, subpartition.getTotalNumberOfBytes());
		assertEquals(4, availablityListener.getNumNotifications());
	}
}
