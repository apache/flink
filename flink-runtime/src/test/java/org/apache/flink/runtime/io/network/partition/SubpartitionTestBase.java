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

import org.apache.flink.runtime.event.AbstractEvent;
import org.apache.flink.runtime.io.network.api.serialization.EventSerializer;
import org.apache.flink.runtime.io.network.buffer.BufferBuilderTestUtils;
import org.apache.flink.runtime.io.network.buffer.BufferConsumer;
import org.apache.flink.util.TestLogger;

import org.junit.Test;

import javax.annotation.Nullable;

import java.io.IOException;

import static org.apache.flink.runtime.io.network.buffer.BufferBuilderTestUtils.createFilledBufferConsumer;
import static org.apache.flink.util.Preconditions.checkArgument;
import static org.hamcrest.core.IsInstanceOf.instanceOf;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertThat;
import static org.junit.Assert.assertTrue;
import static org.mockito.Mockito.mock;

/**
 * Basic subpartition behaviour tests.
 */
public abstract class SubpartitionTestBase extends TestLogger {

	/**
	 * Return the subpartition to be tested.
	 */
	abstract ResultSubpartition createSubpartition();

	// ------------------------------------------------------------------------

	@Test
	public void testAddAfterFinish() throws Exception {
		final ResultSubpartition subpartition = createSubpartition();

		try {
			subpartition.finish();
			assertEquals(1, subpartition.getTotalNumberOfBuffers());
			assertEquals(0, subpartition.getTotalNumberOfBytes()); // only updated after consuming the buffers

			assertEquals(1, subpartition.getTotalNumberOfBuffers());
			assertEquals(0, subpartition.getBuffersInBacklog());
			assertEquals(0, subpartition.getTotalNumberOfBytes()); // only updated after consuming the buffers

			BufferConsumer bufferConsumer = createFilledBufferConsumer(4096, 4096);

			assertFalse(subpartition.add(bufferConsumer));
			assertEquals(1, subpartition.getTotalNumberOfBuffers());
			assertEquals(0, subpartition.getBuffersInBacklog());
			assertEquals(0, subpartition.getTotalNumberOfBytes()); // only updated after consuming the buffers
		} finally {
			if (subpartition != null) {
				subpartition.release();
			}
		}
	}

	@Test
	public void testAddAfterRelease() throws Exception {
		final ResultSubpartition subpartition = createSubpartition();

		try {
			subpartition.release();
			assertEquals(0, subpartition.getTotalNumberOfBuffers());
			assertEquals(0, subpartition.getTotalNumberOfBytes());

			assertEquals(0, subpartition.getTotalNumberOfBuffers());
			assertEquals(0, subpartition.getBuffersInBacklog());
			assertEquals(0, subpartition.getTotalNumberOfBytes());

			BufferConsumer bufferConsumer = createFilledBufferConsumer(4096, 4096);

			assertFalse(subpartition.add(bufferConsumer));
			assertEquals(0, subpartition.getTotalNumberOfBuffers());
			assertEquals(0, subpartition.getBuffersInBacklog());
			assertEquals(0, subpartition.getTotalNumberOfBytes());
		} finally {
			if (subpartition != null) {
				subpartition.release();
			}
		}
	}

	@Test
	public void testReleaseParent() throws Exception {
		final ResultSubpartition partition = createSubpartition();
		verifyViewReleasedAfterParentRelease(partition);
	}

	@Test
	public void testReleaseParentAfterSpilled() throws Exception {
		final ResultSubpartition partition = createSubpartition();
		partition.releaseMemory();

		verifyViewReleasedAfterParentRelease(partition);
	}

	private void verifyViewReleasedAfterParentRelease(ResultSubpartition partition) throws Exception {
		// Add a bufferConsumer
		BufferConsumer bufferConsumer = createFilledBufferConsumer(BufferBuilderTestUtils.BUFFER_SIZE);
		partition.add(bufferConsumer);
		partition.finish();

		// Create the view
		BufferAvailabilityListener listener = mock(BufferAvailabilityListener.class);
		ResultSubpartitionView view = partition.createReadView(listener);

		// The added bufferConsumer and end-of-partition event
		assertNotNull(view.getNextBuffer());
		assertNotNull(view.getNextBuffer());

		// Release the parent
		assertFalse(view.isReleased());
		partition.release();

		// Verify that parent release is reflected at partition view
		assertTrue(view.isReleased());
	}

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
				assertThat(EventSerializer
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
