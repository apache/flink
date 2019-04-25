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

import org.apache.flink.runtime.io.network.buffer.BufferConsumer;
import org.apache.flink.util.TestLogger;

import org.junit.Test;

import static org.apache.flink.runtime.io.network.buffer.BufferBuilderTestUtils.createFilledBufferConsumer;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

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
			assertEquals(0, subpartition.getBuffersInBacklog());

			BufferConsumer bufferConsumer = createFilledBufferConsumer(4096, 4096);

			assertFalse(subpartition.add(bufferConsumer));
			assertTrue(bufferConsumer.isRecycled());

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

			BufferConsumer bufferConsumer = createFilledBufferConsumer(4096, 4096);

			assertFalse(subpartition.add(bufferConsumer));
			assertTrue(bufferConsumer.isRecycled());

		} finally {
			if (subpartition != null) {
				subpartition.release();
			}
		}
	}
}
