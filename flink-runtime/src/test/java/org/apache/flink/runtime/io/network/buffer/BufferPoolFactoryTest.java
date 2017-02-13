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
		BufferPool second = networkBufferPool.createBufferPool(0, Integer.MAX_VALUE);

		assertEquals(networkBufferPool.getTotalNumberOfMemorySegments() / 2, first.getNumBuffers());
		assertEquals(networkBufferPool.getTotalNumberOfMemorySegments() / 2, second.getNumBuffers());
	}

	@Test
	public void testAllDistributed() {
		Random random = new Random();

		int numDistributedBuffers = 0;
		try {
			List<BufferPool> pools = new ArrayList<BufferPool>();

			int numPools = numBuffers / 32;
			for (int i = 0; i < numPools; i++) {
				int numRequiredBuffers = random.nextInt(7 + 1);
				// make unbounded buffers more likely:
				int maxUsedBuffers = random.nextBoolean() ? Integer.MAX_VALUE :
					Math.max(1, random.nextInt(10) + numRequiredBuffers);
				pools.add(networkBufferPool.createBufferPool(numRequiredBuffers, maxUsedBuffers));
			}

			for (BufferPool pool : pools) {
				numDistributedBuffers += pool.getNumBuffers();
			}
		}
		catch (Throwable t) {
			t.printStackTrace();
			fail(t.getMessage());
		}

		assertEquals(numBuffers, numDistributedBuffers);
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
