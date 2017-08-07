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
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.mockito.Matchers;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.powermock.api.mockito.PowerMockito.spy;

public class FixedBufferPoolTest {

	private final static int numBuffers = 1024;

	private final static int memorySegmentSize = 128;

	private NetworkBufferPool networkBufferPool;

	private BufferPool fixedBufferPool;

	@Before
	public void setupFixedBufferPool() {
		networkBufferPool = new NetworkBufferPool(numBuffers, memorySegmentSize, MemoryType.HEAP);
		fixedBufferPool = new FixedBufferPool(networkBufferPool, 1);

		assertEquals(0, fixedBufferPool.getNumberOfAvailableMemorySegments());
	}

	@After
	public void destroyAndVerifyAllBuffersReturned() throws IOException {
		if (!fixedBufferPool.isDestroyed()) {
			fixedBufferPool.lazyDestroy();
		}

		String msg = "Did not return all buffers to memory segment pool after test.";
		assertEquals(msg, numBuffers, networkBufferPool.getNumberOfAvailableMemorySegments());
	}


	@Test
	public void testRequestMoreThanAvailable() throws IOException {
		fixedBufferPool.lazyDestroy();

		fixedBufferPool = new FixedBufferPool(networkBufferPool, numBuffers);

		List<Buffer> requests = new ArrayList<Buffer>(numBuffers);

		for (int i = 1; i <= numBuffers; i++) {
			Buffer buffer = fixedBufferPool.requestBuffer();

			assertEquals(i, getNumRequestedFromMemorySegmentPool());
			assertNotNull(buffer);

			requests.add(buffer);
		}

		{
			// One more...
			Buffer buffer = fixedBufferPool.requestBuffer();
			assertEquals(numBuffers, getNumRequestedFromMemorySegmentPool());
			assertNull(buffer);
		}

		for (Buffer buffer : requests) {
			buffer.recycle();
		}
	}

	@Test
	public void testRequestAfterDestroy() throws IOException {
		fixedBufferPool.lazyDestroy();

		try {
			fixedBufferPool.requestBuffer();
			fail("Call should have failed with an IllegalStateException");
		} catch (IllegalStateException e) {
			// we expect exactly that
		}
	}

	@Test
	public void testRecycleAfterDestroy() throws IOException {
		fixedBufferPool.lazyDestroy();

		fixedBufferPool = new FixedBufferPool(networkBufferPool, numBuffers);

		List<Buffer> requests = new ArrayList<Buffer>(numBuffers);

		for (int i = 0; i < numBuffers; i++) {
			requests.add(fixedBufferPool.requestBuffer());
		}

		fixedBufferPool.lazyDestroy();

		// All buffers have been requested, but can not be returned yet.
		assertEquals(numBuffers, getNumRequestedFromMemorySegmentPool());

		// Recycle should return buffers to memory segment pool
		for (Buffer buffer : requests) {
			buffer.recycle();
		}

		assertEquals(0, getNumRequestedFromMemorySegmentPool());
	}

	@Test(expected = IllegalArgumentException.class)
	public void testSetLessThanRequiredNumBuffers() throws IOException {
		fixedBufferPool.lazyDestroy();

		fixedBufferPool = new FixedBufferPool(networkBufferPool, numBuffers);

		fixedBufferPool.setNumBuffers(numBuffers - 1);
	}

	@Test(expected = IllegalArgumentException.class)
	public void testSetMoreThanRequiredNumBuffers() throws IOException {
		fixedBufferPool.lazyDestroy();

		fixedBufferPool = new FixedBufferPool(networkBufferPool, numBuffers);

		fixedBufferPool.setNumBuffers(numBuffers + 1);
	}

	@Test
	public void testNotifyBufferPoolListener() throws Exception {
		fixedBufferPool.lazyDestroy();

		fixedBufferPool = new FixedBufferPool(networkBufferPool, numBuffers);

		final List<Buffer> requests = new ArrayList<Buffer>(numBuffers);
		final List<Buffer> notifies = new ArrayList<Buffer>(numBuffers);

		BufferPoolListener listener = spy(new BufferPoolListener() {
			int notifyTimes = numBuffers;

			@Override
			public boolean notifyBufferAvailable(Buffer buffer) {
				notifies.add(buffer);
				return --notifyTimes > 0;
			}
		});

		for (int i = 0; i < numBuffers; i++) {
			requests.add(fixedBufferPool.requestBuffer());
		}

		assertNull(fixedBufferPool.requestBuffer());

		assertTrue(fixedBufferPool.addBufferPoolListener(listener));

		for (Buffer buffer : requests) {
			buffer.recycle();
		}

		verify(listener, times(numBuffers)).notifyBufferAvailable(Matchers.any(Buffer.class));

		for (Buffer buffer : notifies) {
			buffer.recycle();
		}
	}

	// ------------------------------------------------------------------------
	// Helpers
	// ------------------------------------------------------------------------

	private int getNumRequestedFromMemorySegmentPool() {
		return networkBufferPool.getTotalNumberOfMemorySegments() - networkBufferPool.getNumberOfAvailableMemorySegments();
	}
}
