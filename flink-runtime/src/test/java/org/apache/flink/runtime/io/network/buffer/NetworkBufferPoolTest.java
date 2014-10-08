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

import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.mockito.Mockito;

import java.util.ArrayList;
import java.util.List;
import java.util.Random;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.mockito.Mockito.mock;

public class NetworkBufferPoolTest {

	private final static int numBuffers = 1024;

	private final static int memorySegmentSize = 128;

	private NetworkBufferPool networkBufferPool;

	@Before
	public void setupNetworkBufferPool() {
		networkBufferPool = new NetworkBufferPool(numBuffers, memorySegmentSize);
	}

	@After
	public void verifyAllBuffersReturned() {
		String msg = "Did not return all buffers to network buffer pool after test.";
		assertEquals(msg, numBuffers, networkBufferPool.getNumAvailableMemorySegments());
	}

	@Test
	public void testPendingMemorySegmentRequests() {
		// --------------------------------------------------------------------
		// Request all buffers and afterwards reduce number of buffers
		// --------------------------------------------------------------------
		List<BufferFuture> availableRequests = new ArrayList<BufferFuture>(numBuffers);
		LocalBufferPool available = createLocalBufferPool();

		available.setNumBuffers(numBuffers);

		for (int i = 0; i < numBuffers; i++) {
			availableRequests.add(available.requestBuffer());
		}

		available.setNumBuffers(numBuffers / 2);

		// --------------------------------------------------------------------
		// Request remaining half of buffers (unavailable at this point)
		// --------------------------------------------------------------------
		List<BufferFuture> unavailableRequests = new ArrayList<BufferFuture>(numBuffers);
		LocalBufferPool unavailable = createLocalBufferPool();

		unavailable.setNumBuffers(numBuffers / 2);

		for (int i = 0; i < numBuffers / 2; i++) {
			BufferFuture future = unavailable.requestBuffer();
			assertNull(future.getBuffer());
			unavailableRequests.add(future);
		}

		// --------------------------------------------------------------------
		// Recycle excess buffers from first pool
		// --------------------------------------------------------------------
		while (availableRequests.size() > available.getNumBuffers()) {
			availableRequests.remove(0).getBuffer().recycle();
		}

		// --------------------------------------------------------------------
		// Verify that all recycled buffers have been delivered to the pending
		// requests of the other pool.
		// --------------------------------------------------------------------
		for (BufferFuture request : unavailableRequests) {
			assertNotNull(request.getBuffer());
		}

		// --------------------------------------------------------------------

		recycleAllBuffers(availableRequests);
		recycleAllBuffers(unavailableRequests);

		assertEquals(numBuffers / 2, available.getNumAvailableBuffers());
		assertEquals(numBuffers / 2, unavailable.getNumAvailableBuffers());

		available.destroy();
		unavailable.destroy();
	}

	@Test
	public void testObsoletePendingMemorySegmentRequests() {
		// --------------------------------------------------------------------
		// Request all buffers and afterwards reduce number of buffers
		// --------------------------------------------------------------------
		List<BufferFuture> availableRequests = new ArrayList<BufferFuture>(numBuffers);
		LocalBufferPool available = createLocalBufferPool();
		available.setNumBuffers(numBuffers);

		for (int i = 0; i < numBuffers; i++) {
			availableRequests.add(available.requestBuffer());
		}

		available.setNumBuffers(numBuffers / 2);

		// --------------------------------------------------------------------
		// Request remaining half of buffers (unavailable at this point)
		// --------------------------------------------------------------------
		List<BufferFuture> unavailableRequests = new ArrayList<BufferFuture>(numBuffers);
		LocalBufferPool unavailable = createLocalBufferPool();
		unavailable.setNumBuffers(numBuffers / 2);

		for (int i = 0; i < numBuffers / 2; i++) {
			BufferFuture future = unavailable.requestBuffer();
			assertNull(future.getBuffer());
			unavailableRequests.add(future);
		}

		// --------------------------------------------------------------------
		// Recycle single excess buffer from first pool
		// --------------------------------------------------------------------
		availableRequests.remove(0).getBuffer().recycle();

		// --------------------------------------------------------------------
		// Now cycle this recycled buffer through all pending buffers requests
		// (making the pending memory segment request obsolete).
		// --------------------------------------------------------------------
		recycleAllBuffers(unavailableRequests);
		recycleAllBuffers(availableRequests);

		assertEquals(numBuffers / 2, available.getNumAvailableBuffers());
		assertEquals(1, unavailable.getNumAvailableBuffers());

		available.destroy();
		unavailable.destroy();
	}

	// ------------------------------------------------------------------------
	// BufferPoolFactory
	// ------------------------------------------------------------------------

	@Test(expected = IllegalStateException.class)
	public void testRequireMoreThanPossible() {
		BufferPoolOwner owner = Mockito.mock(BufferPoolOwner.class);
		networkBufferPool.createBufferPool(networkBufferPool.getNumMemorySegments() * 2, false);
	}

	@Test
	public void testFixedPool() {
		BufferPoolOwner owner = Mockito.mock(BufferPoolOwner.class);
		BufferPool lbp = networkBufferPool.createBufferPool(1, true);

		assertEquals(1, lbp.getNumBuffers());
	}

	@Test
	public void testSingleManagedPoolGetsAll() {
		BufferPoolOwner owner = Mockito.mock(BufferPoolOwner.class);
		BufferPool lbp = networkBufferPool.createBufferPool(1, false);

		assertEquals(networkBufferPool.getNumMemorySegments(), lbp.getNumBuffers());
	}

	@Test
	public void testSingleManagedPoolGetsAllExceptFixedOnes() {
		BufferPoolOwner owner = Mockito.mock(BufferPoolOwner.class);

		BufferPool fixed = networkBufferPool.createBufferPool(24, true);

		BufferPool lbp = networkBufferPool.createBufferPool(1, false);

		assertEquals(24, fixed.getNumBuffers());
		assertEquals(networkBufferPool.getNumMemorySegments() - fixed.getNumBuffers(), lbp.getNumBuffers());
	}

	@Test
	public void testUniformDistribution() {
		BufferPoolOwner owner = Mockito.mock(BufferPoolOwner.class);

		BufferPool first = networkBufferPool.createBufferPool(0, false);
		BufferPool second = networkBufferPool.createBufferPool(0, false);

		assertEquals(networkBufferPool.getNumMemorySegments() / 2, first.getNumBuffers());
		assertEquals(networkBufferPool.getNumMemorySegments() / 2, second.getNumBuffers());
	}

	@Test
	public void testAllDistributed() {
		BufferPoolOwner owner = Mockito.mock(BufferPoolOwner.class);

		Random random = new Random();

		try {
			List<BufferPool> pools = new ArrayList<BufferPool>();

			int numPools = numBuffers / 32;
			for (int i = 0; i < numPools; i++) {
				pools.add(networkBufferPool.createBufferPool(random.nextInt(7 + 1), random.nextBoolean()));
			}

			int numDistributedBuffers = 0;
			for (BufferPool pool : pools) {
				numDistributedBuffers += pool.getNumBuffers();
			}

			assertEquals(numBuffers, numDistributedBuffers);
		} catch (Throwable t) {
			t.printStackTrace();
			Assert.fail(t.getMessage());
		}
	}

	@Test
	public void testCreateDestroy() {
		BufferPoolOwner owner = Mockito.mock(BufferPoolOwner.class);

		BufferPool first = networkBufferPool.createBufferPool(0, false);

		assertEquals(networkBufferPool.getNumMemorySegments(), first.getNumBuffers());

		BufferPool second = networkBufferPool.createBufferPool(0, false);

		assertEquals(networkBufferPool.getNumMemorySegments() / 2, first.getNumBuffers());

		assertEquals(networkBufferPool.getNumMemorySegments() / 2, second.getNumBuffers());

		first.destroy();

		assertEquals(networkBufferPool.getNumMemorySegments(), second.getNumBuffers());
	}

	// ------------------------------------------------------------------------

	private void recycleAllBuffers(List<BufferFuture> requests) {
		for (BufferFuture request : requests) {
			if (request.isSuccess()) {
				request.getBuffer().recycle();
			}
		}
	}

	private LocalBufferPool createLocalBufferPool() {
		return new LocalBufferPool(networkBufferPool, 1);
	}

}
