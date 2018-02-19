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

import org.apache.flink.runtime.io.network.buffer.BufferBuilderTestUtils;
import org.apache.flink.runtime.io.network.buffer.BufferConsumer;
import org.apache.flink.util.TestLogger;

import org.junit.Test;

import java.io.IOException;

import static org.apache.flink.runtime.io.network.buffer.BufferBuilderTestUtils.createFilledBufferConsumer;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
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

			assertEquals(1, subpartition.getTotalNumberOfBuffers());
			assertEquals(0, subpartition.getBuffersInBacklog());

			BufferConsumer bufferConsumer = createFilledBufferConsumer(4096, 4096);

			assertFalse(subpartition.add(bufferConsumer));
			assertEquals(1, subpartition.getTotalNumberOfBuffers());
			assertEquals(0, subpartition.getBuffersInBacklog());
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

	protected void assertNextBuffer(
			ResultSubpartitionView readView,
			int expectedReadableBufferSize,
			boolean expectedIsMoreAvailable,
			int expectedBuffersInBacklog) throws IOException, InterruptedException {
		ResultSubpartition.BufferAndBacklog bufferAndBacklog = readView.getNextBuffer();
		assertEquals(expectedReadableBufferSize, bufferAndBacklog.buffer().readableBytes());
		assertEquals(expectedIsMoreAvailable, bufferAndBacklog.isMoreAvailable());
		assertEquals(expectedBuffersInBacklog, bufferAndBacklog.buffersInBacklog());
	}
}
