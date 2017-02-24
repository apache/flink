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

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Random;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotEquals;
import static org.junit.Assert.fail;

public class BufferPoolFactoryTest {

	private final static int numBuffers = 1024;

	private final static int memorySegmentSize = 128;

	private NetworkBufferPool networkBufferPool;

	@Before
	public void setupNetworkBufferPool() {
		networkBufferPool = new NetworkBufferPool(numBuffers, memorySegmentSize, MemoryType.HEAP);
	}

	@After
	public void verifyAllBuffersReturned() {
		String msg = "Did not return all buffers to network buffer pool after test.";
		assertEquals(msg, numBuffers, networkBufferPool.getNumberOfAvailableMemorySegments());
		// in case buffers have actually been requested, we must release them again
		networkBufferPool.destroy();
	}

	@Test(expected = IOException.class)
	public void testRequireMoreThanPossible() throws IOException {
		networkBufferPool.createBufferPool(networkBufferPool.getTotalNumberOfMemorySegments() * 2, Integer.MAX_VALUE);
	}

	@Test
	public void testBoundedPools() throws IOException {
		BufferPool lbp = networkBufferPool.createBufferPool(1, 1);
		assertEquals(1, lbp.getNumBuffers());

		lbp = networkBufferPool.createBufferPool(1, 2);
		assertEquals(2, lbp.getNumBuffers());
	}

	@Test
	public void testSingleManagedPoolGetsAll() throws IOException {
		BufferPool lbp = networkBufferPool.createBufferPool(1, Integer.MAX_VALUE);

		assertEquals(networkBufferPool.getTotalNumberOfMemorySegments(), lbp.getNumBuffers());
	}

	@Test
	public void testSingleManagedPoolGetsAllExceptFixedOnes() throws IOException {
		BufferPool fixed = networkBufferPool.createBufferPool(24, 24);

		BufferPool lbp = networkBufferPool.createBufferPool(1, Integer.MAX_VALUE);

		assertEquals(24, fixed.getNumBuffers());
		assertEquals(networkBufferPool.getTotalNumberOfMemorySegments() - fixed.getNumBuffers(), lbp.getNumBuffers());
	}

	@Test
	public void testUniformDistribution() throws IOException {
		BufferPool first = networkBufferPool.createBufferPool(0, Integer.MAX_VALUE);
		assertEquals(networkBufferPool.getTotalNumberOfMemorySegments(), first.getNumBuffers());

		BufferPool second = networkBufferPool.createBufferPool(0, Integer.MAX_VALUE);
		assertEquals(networkBufferPool.getTotalNumberOfMemorySegments() / 2, first.getNumBuffers());
		assertEquals(networkBufferPool.getTotalNumberOfMemorySegments() / 2, second.getNumBuffers());
	}

	/**
	 * Tests that buffers, once given to an initial buffer pool, get re-distributed to a second one
	 * in case both buffer pools request half of the available buffer count.
	 */
	@Test
	public void testUniformDistributionAllBuffers() throws IOException {
		BufferPool first = networkBufferPool
			.createBufferPool(networkBufferPool.getTotalNumberOfMemorySegments() / 2, Integer.MAX_VALUE);
		assertEquals(networkBufferPool.getTotalNumberOfMemorySegments(), first.getNumBuffers());

		BufferPool second = networkBufferPool
			.createBufferPool(networkBufferPool.getTotalNumberOfMemorySegments() / 2, Integer.MAX_VALUE);
		assertEquals(networkBufferPool.getTotalNumberOfMemorySegments() / 2, first.getNumBuffers());
		assertEquals(networkBufferPool.getTotalNumberOfMemorySegments() / 2,
			second.getNumBuffers());
	}

	@Test
	public void testUniformDistributionBounded1() throws IOException {
		BufferPool first = networkBufferPool.createBufferPool(0, networkBufferPool.getTotalNumberOfMemorySegments());
		assertEquals(networkBufferPool.getTotalNumberOfMemorySegments(), first.getNumBuffers());

		BufferPool second = networkBufferPool.createBufferPool(0, networkBufferPool.getTotalNumberOfMemorySegments());
		assertEquals(networkBufferPool.getTotalNumberOfMemorySegments() / 2, first.getNumBuffers());
		assertEquals(networkBufferPool.getTotalNumberOfMemorySegments() / 2, second.getNumBuffers());
	}

	@Test
	public void testUniformDistributionBounded2() throws IOException {
		BufferPool first = networkBufferPool.createBufferPool(0, 10);
		assertEquals(10, first.getNumBuffers());

		BufferPool second = networkBufferPool.createBufferPool(0, 10);
		assertEquals(10, first.getNumBuffers());
		assertEquals(10, second.getNumBuffers());
	}

	@Test
	public void testUniformDistributionBounded3() throws IOException {
		NetworkBufferPool globalPool = new NetworkBufferPool(3, 128, MemoryType.HEAP);
		BufferPool first = globalPool.createBufferPool(0, 10);
		assertEquals(3, first.getNumBuffers());

		BufferPool second = globalPool.createBufferPool(0, 10);
		// the order of which buffer pool received 2 or 1 buffer is undefined
		assertEquals(3, first.getNumBuffers() + second.getNumBuffers());
		assertNotEquals(3, first.getNumBuffers());
		assertNotEquals(3, second.getNumBuffers());

		BufferPool third = globalPool.createBufferPool(0, 10);
		assertEquals(1, first.getNumBuffers());
		assertEquals(1, second.getNumBuffers());
		assertEquals(1, third.getNumBuffers());

		// similar to #verifyAllBuffersReturned()
		String msg = "Did not return all buffers to network buffer pool after test.";
		assertEquals(msg, 3, globalPool.getNumberOfAvailableMemorySegments());
		// in case buffers have actually been requested, we must release them again
		globalPool.destroy();
	}

	@Test
	public void testBufferRedistributionMixed1() throws IOException {
		// try multiple times for various orders during redistribution
		for (int i = 0; i < 1_000; ++i) {
			BufferPool first = networkBufferPool.createBufferPool(0, 10);
			assertEquals(10, first.getNumBuffers());

			BufferPool second = networkBufferPool.createBufferPool(0, 10);
			assertEquals(10, first.getNumBuffers());
			assertEquals(10, second.getNumBuffers());

			BufferPool third = networkBufferPool.createBufferPool(0, Integer.MAX_VALUE);
			// note: exact buffer distribution depends on the order during the redistribution
			for (BufferPool bp : new BufferPool[] {first, second, third}) {
				int size = networkBufferPool.getTotalNumberOfMemorySegments() *
					Math.min(networkBufferPool.getTotalNumberOfMemorySegments(),
						bp.getMaxNumberOfMemorySegments()) /
					(networkBufferPool.getTotalNumberOfMemorySegments() + 20);
				if (bp.getNumBuffers() != size && bp.getNumBuffers() != (size + 1)) {
					fail("wrong buffer pool size after redistribution: " + bp.getNumBuffers());
				}
			}

			BufferPool fourth = networkBufferPool.createBufferPool(0, Integer.MAX_VALUE);
			// note: exact buffer distribution depends on the order during the redistribution
			for (BufferPool bp : new BufferPool[] {first, second, third, fourth}) {
				int size = networkBufferPool.getTotalNumberOfMemorySegments() *
					Math.min(networkBufferPool.getTotalNumberOfMemorySegments(),
						bp.getMaxNumberOfMemorySegments()) /
					(2 * networkBufferPool.getTotalNumberOfMemorySegments() + 20);
				if (bp.getNumBuffers() != size && bp.getNumBuffers() != (size + 1)) {
					fail("wrong buffer pool size after redistribution: " + bp.getNumBuffers());
				}
			}

			verifyAllBuffersReturned();
			setupNetworkBufferPool();
		}
	}

	@Test
	public void testAllDistributed() throws IOException {
		// try multiple times for various orders during redistribution
		for (int i = 0; i < 1_000; ++i) {
			Random random = new Random();

			List<BufferPool> pools = new ArrayList<BufferPool>();

			int numPools = numBuffers / 32;
			long maxTotalUsed = 0;
			for (int j = 0; j < numPools; j++) {
				int numRequiredBuffers = random.nextInt(7 + 1);
				// make unbounded buffers more likely:
				int maxUsedBuffers = random.nextBoolean() ? Integer.MAX_VALUE :
					Math.max(1, random.nextInt(10) + numRequiredBuffers);
				pools.add(networkBufferPool.createBufferPool(numRequiredBuffers, maxUsedBuffers));
				maxTotalUsed = Math.min(numBuffers, maxTotalUsed + maxUsedBuffers);

				// after every iteration, all buffers (up to maxTotalUsed) must be distributed
				int numDistributedBuffers = 0;
				for (BufferPool pool : pools) {
					numDistributedBuffers += pool.getNumBuffers();
				}
				assertEquals(maxTotalUsed, numDistributedBuffers);
			}

			verifyAllBuffersReturned();
			setupNetworkBufferPool();
		}
	}

	@Test
	public void testCreateDestroy() throws IOException {
		BufferPool first = networkBufferPool.createBufferPool(0, Integer.MAX_VALUE);

		assertEquals(networkBufferPool.getTotalNumberOfMemorySegments(), first.getNumBuffers());

		BufferPool second = networkBufferPool.createBufferPool(0, Integer.MAX_VALUE);

		assertEquals(networkBufferPool.getTotalNumberOfMemorySegments() / 2, first.getNumBuffers());

		assertEquals(networkBufferPool.getTotalNumberOfMemorySegments() / 2, second.getNumBuffers());

		first.lazyDestroy();

		assertEquals(networkBufferPool.getTotalNumberOfMemorySegments(), second.getNumBuffers());
	}

}
