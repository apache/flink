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
		networkBufferPool.createBufferPool(networkBufferPool.getTotalNumberOfMemorySegments() * 2, false);
	}

	@Test
	public void testFixedPool() throws IOException {
		BufferPool lbp = networkBufferPool.createBufferPool(1, true);

		assertEquals(1, lbp.getNumBuffers());
	}

	@Test
	public void testSingleManagedPoolGetsAll() throws IOException {
		BufferPool lbp = networkBufferPool.createBufferPool(1, false);

		assertEquals(networkBufferPool.getTotalNumberOfMemorySegments(), lbp.getNumBuffers());
	}

	@Test
	public void testSingleManagedPoolGetsAllExceptFixedOnes() throws IOException {
		BufferPool fixed = networkBufferPool.createBufferPool(24, true);

		BufferPool lbp = networkBufferPool.createBufferPool(1, false);

		assertEquals(24, fixed.getNumBuffers());
		assertEquals(networkBufferPool.getTotalNumberOfMemorySegments() - fixed.getNumBuffers(), lbp.getNumBuffers());
	}

	@Test
	public void testUniformDistribution() throws IOException {
		BufferPool first = networkBufferPool.createBufferPool(0, false);
		BufferPool second = networkBufferPool.createBufferPool(0, false);

		assertEquals(networkBufferPool.getTotalNumberOfMemorySegments() / 2, first.getNumBuffers());
		assertEquals(networkBufferPool.getTotalNumberOfMemorySegments() / 2, second.getNumBuffers());
	}

	@Test
	public void testAllDistributed() {
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
		}
		catch (Throwable t) {
			t.printStackTrace();
			fail(t.getMessage());
		}
	}

	@Test
	public void testCreateDestroy() throws IOException {
		BufferPool first = networkBufferPool.createBufferPool(0, false);

		assertEquals(networkBufferPool.getTotalNumberOfMemorySegments(), first.getNumBuffers());

		BufferPool second = networkBufferPool.createBufferPool(0, false);

		assertEquals(networkBufferPool.getTotalNumberOfMemorySegments() / 2, first.getNumBuffers());

		assertEquals(networkBufferPool.getTotalNumberOfMemorySegments() / 2, second.getNumBuffers());

		first.lazyDestroy();

		assertEquals(networkBufferPool.getTotalNumberOfMemorySegments(), second.getNumBuffers());
	}

}
