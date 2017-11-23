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

import org.apache.flink.runtime.io.network.buffer.Buffer;
import org.apache.flink.runtime.io.network.util.TestBufferFactory;
import org.apache.flink.util.TestLogger;

import org.junit.Test;

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
			assertEquals(4, subpartition.getTotalNumberOfBytes());

			assertFalse(subpartition.add(mock(Buffer.class)));
			assertEquals(1, subpartition.getTotalNumberOfBuffers());
			assertEquals(4, subpartition.getTotalNumberOfBytes());
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

			assertFalse(subpartition.add(mock(Buffer.class)));
			assertEquals(0, subpartition.getTotalNumberOfBuffers());
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
		// Add a buffer
		Buffer buffer = TestBufferFactory.createBuffer();
		partition.add(buffer);
		partition.finish();

		// Create the view
		BufferAvailabilityListener listener = mock(BufferAvailabilityListener.class);
		ResultSubpartitionView view = partition.createReadView(listener);

		// The added buffer and end-of-partition event
		assertNotNull(view.getNextBuffer());
		assertNotNull(view.getNextBuffer());

		// Release the parent
		assertFalse(view.isReleased());
		partition.release();

		// Verify that parent release is reflected at partition view
		assertTrue(view.isReleased());
	}
}
