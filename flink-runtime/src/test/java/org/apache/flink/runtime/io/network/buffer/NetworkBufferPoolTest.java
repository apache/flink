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

package org.apache.flink.runtime.io.network.buffer;

import org.apache.flink.core.memory.MemoryType;
import org.junit.Test;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

public class NetworkBufferPoolTest {

	@Test
	public void testCreatePoolAfterDestroy() {
		try {
			final int bufferSize = 128;
			final int numBuffers = 10;

			NetworkBufferPool globalPool = new NetworkBufferPool(numBuffers, bufferSize, MemoryType.HEAP);
			assertEquals(bufferSize, globalPool.getMemorySegmentSize());
			assertEquals(numBuffers, globalPool.getTotalNumberOfMemorySegments());
			assertEquals(numBuffers, globalPool.getNumberOfAvailableMemorySegments());
			assertEquals(0, globalPool.getNumberOfRegisteredBufferPools());

			globalPool.destroy();

			assertTrue(globalPool.isDestroyed());

			try {
				globalPool.createBufferPool(2, true);
				fail("Should throw an IllegalStateException");
			}
			catch (IllegalStateException e) {
				// yippie!
			}

			try {
				globalPool.createBufferPool(2, false);
				fail("Should throw an IllegalStateException");
			}
			catch (IllegalStateException e) {
				// yippie!
			}
		}
		catch (Exception e) {
			e.printStackTrace();
			fail(e.getMessage());
		}

	}
	@Test
	public void testDestroyAll() {
		try {
			NetworkBufferPool globalPool = new NetworkBufferPool(10, 128, MemoryType.HEAP);

			BufferPool fixedPool = globalPool.createBufferPool(2, true);
			BufferPool nonFixedPool = globalPool.createBufferPool(5, false);

			assertEquals(2, fixedPool.getNumberOfRequiredMemorySegments());
			assertEquals(5, nonFixedPool.getNumberOfRequiredMemorySegments());

			Buffer[] buffers = {
					fixedPool.requestBuffer(),
					fixedPool.requestBuffer(),

					nonFixedPool.requestBuffer(),
					nonFixedPool.requestBuffer(),
					nonFixedPool.requestBuffer(),
					nonFixedPool.requestBuffer(),
					nonFixedPool.requestBuffer(),
					nonFixedPool.requestBuffer(),
					nonFixedPool.requestBuffer(),
					nonFixedPool.requestBuffer()
			};

			for (Buffer b : buffers) {
				assertNotNull(b);
				assertNotNull(b.getMemorySegment());
			}

			assertNull(fixedPool.requestBuffer());
			assertNull(nonFixedPool.requestBuffer());

			// destroy all allocated ones
			globalPool.destroyAllBufferPools();

			// check the destroyed status
			assertFalse(globalPool.isDestroyed());
			assertTrue(fixedPool.isDestroyed());
			assertTrue(nonFixedPool.isDestroyed());

			assertEquals(0, globalPool.getNumberOfRegisteredBufferPools());

			// buffers are not yet recycled
			assertEquals(0, globalPool.getNumberOfAvailableMemorySegments());

			// the recycled buffers should go to the global pool
			for (Buffer b : buffers) {
				b.recycle();
			}
			assertEquals(globalPool.getTotalNumberOfMemorySegments(), globalPool.getNumberOfAvailableMemorySegments());

			// can request no more buffers
			try {
				fixedPool.requestBuffer();
				fail("Should fail with an IllegalStateException");
			}
			catch (IllegalStateException e) {
				// that's the way we like it, aha, aha
			}

			try {
				nonFixedPool.requestBuffer();
				fail("Should fail with an IllegalStateException");
			}
			catch (IllegalStateException e) {
				// stayin' alive
			}

			// can create a new pool now
			assertNotNull(globalPool.createBufferPool(10, false));
		}
		catch (Exception e) {
			e.printStackTrace();
			fail(e.getMessage());
		}
	}
}
