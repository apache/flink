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

import org.apache.flink.core.memory.MemorySegment;
import org.apache.flink.core.testutils.CheckedThread;
import org.apache.flink.core.testutils.OneShotLatch;
import org.apache.flink.runtime.io.network.partition.InternalResultPartition;
import org.apache.flink.runtime.io.network.partition.ResultPartitionType;
import org.apache.flink.util.TestLogger;

import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;
import org.mockito.invocation.InvocationOnMock;
import org.mockito.stubbing.Answer;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;

import static org.hamcrest.CoreMatchers.instanceOf;
import static org.hamcrest.CoreMatchers.nullValue;
import static org.hamcrest.Matchers.hasProperty;
import static org.hamcrest.core.IsCollectionContaining.hasItem;
import static org.hamcrest.core.IsNot.not;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertThat;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;
import static org.mockito.Matchers.anyInt;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

/**
 * Tests for {@link NetworkBufferPool}.
 */
public class NetworkBufferPoolTest extends TestLogger {

	@Rule
	public ExpectedException expectedException = ExpectedException.none();

	@Test
	public void testCreatePoolAfterDestroy() {
		try {
			final int bufferSize = 128;
			final int numBuffers = 10;

			NetworkBufferPool globalPool = new NetworkBufferPool(numBuffers, bufferSize);
			assertEquals(bufferSize, globalPool.getMemorySegmentSize());
			assertEquals(numBuffers, globalPool.getTotalNumberOfMemorySegments());
			assertEquals(numBuffers, globalPool.getNumberOfAvailableMemorySegments());
			assertEquals(0, globalPool.getNumberOfRegisteredBufferPools());

			globalPool.destroy();

			assertTrue(globalPool.isDestroyed());

			try {
				globalPool.createBufferPool(2, 2);
				fail("Should throw an IllegalStateException");
			}
			catch (IllegalStateException e) {
				// yippie!
			}

			try {
				globalPool.createBufferPool(2, 10);
				fail("Should throw an IllegalStateException");
			}
			catch (IllegalStateException e) {
				// yippie!
			}

			try {
				globalPool.createBufferPool(2, Integer.MAX_VALUE);
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
			NetworkBufferPool globalPool = new NetworkBufferPool(10, 128);

			BufferPool fixedPool = globalPool.createBufferPool(2, 2);
			BufferPool boundedPool = globalPool.createBufferPool(0, 1);
			BufferPool nonFixedPool = globalPool.createBufferPool(5, Integer.MAX_VALUE);

			assertEquals(2, fixedPool.getNumberOfRequiredMemorySegments());
			assertEquals(0, boundedPool.getNumberOfRequiredMemorySegments());
			assertEquals(5, nonFixedPool.getNumberOfRequiredMemorySegments());

			// actually, the buffer pool sizes may be different due to rounding and based on the internal order of
			// the buffer pools - the total number of retrievable buffers should be equal to the number of buffers
			// in the NetworkBufferPool though

			ArrayList<Buffer> buffers = new ArrayList<>(globalPool.getTotalNumberOfMemorySegments());
			collectBuffers:
			for (int i = 0; i < 10; ++i) {
				for (BufferPool bp : new BufferPool[] { fixedPool, boundedPool, nonFixedPool }) {
					Buffer buffer = bp.requestBuffer();
					if (buffer != null) {
						assertNotNull(buffer.getMemorySegment());
						buffers.add(buffer);
						continue collectBuffers;
					}
				}
			}

			assertEquals(globalPool.getTotalNumberOfMemorySegments(), buffers.size());

			assertNull(fixedPool.requestBuffer());
			assertNull(boundedPool.requestBuffer());
			assertNull(nonFixedPool.requestBuffer());

			// destroy all allocated ones
			globalPool.destroyAllBufferPools();

			// check the destroyed status
			assertFalse(globalPool.isDestroyed());
			assertTrue(fixedPool.isDestroyed());
			assertTrue(boundedPool.isDestroyed());
			assertTrue(nonFixedPool.isDestroyed());

			assertEquals(0, globalPool.getNumberOfRegisteredBufferPools());

			// buffers are not yet recycled
			assertEquals(0, globalPool.getNumberOfAvailableMemorySegments());

			// the recycled buffers should go to the global pool
			for (Buffer b : buffers) {
				b.recycleBuffer();
			}
			assertEquals(globalPool.getTotalNumberOfMemorySegments(), globalPool.getNumberOfAvailableMemorySegments());

			// can request no more buffers
			try {
				fixedPool.requestBuffer();
				fail("Should fail with an IllegalStateException");
			}
			catch (IllegalStateException e) {
				// yippie!
			}

			try {
				boundedPool.requestBuffer();
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
			assertNotNull(globalPool.createBufferPool(10, Integer.MAX_VALUE));
		}
		catch (Exception e) {
			e.printStackTrace();
			fail(e.getMessage());
		}
	}

	/**
	 * Tests {@link NetworkBufferPool#requestMemorySegments(int)} with the {@link NetworkBufferPool}
	 * currently containing the number of required free segments.
	 */
	@Test
	public void testRequestMemorySegmentsLessThanTotalBuffers() throws Exception {
		final int numBuffers = 10;

		NetworkBufferPool globalPool = new NetworkBufferPool(numBuffers, 128);

		List<MemorySegment> memorySegments = Collections.emptyList();
		try {
			memorySegments = globalPool.requestMemorySegments(numBuffers / 2);
			assertEquals(memorySegments.size(), numBuffers / 2);

			globalPool.recycleMemorySegments(memorySegments);
			memorySegments.clear();
			assertEquals(globalPool.getNumberOfAvailableMemorySegments(), numBuffers);
		} finally {
			globalPool.recycleMemorySegments(memorySegments); // just in case
			globalPool.destroy();
		}
	}

	/**
	 * Tests {@link NetworkBufferPool#requestMemorySegments(int)} with the number of required
	 * buffers exceeding the capacity of {@link NetworkBufferPool}.
	 */
	@Test
	public void testRequestMemorySegmentsMoreThanTotalBuffers() throws Exception {
		final int numBuffers = 10;

		NetworkBufferPool globalPool = new NetworkBufferPool(numBuffers, 128);

		try {
			globalPool.requestMemorySegments(numBuffers + 1);
			fail("Should throw an IOException");
		} catch (IOException e) {
			assertEquals(globalPool.getNumberOfAvailableMemorySegments(), numBuffers);
		} finally {
			globalPool.destroy();
		}
	}

	/**
	 * Tests {@link NetworkBufferPool#requestMemorySegments(int)} with the invalid argument to
	 * cause exception.
	 */
	@Test
	public void testRequestMemorySegmentsWithInvalidArgument() throws Exception {
		final int numBuffers = 10;

		NetworkBufferPool globalPool = new NetworkBufferPool(numBuffers, 128);

		try {
			// the number of requested buffers should be larger than zero
			globalPool.requestMemorySegments(0);
			fail("Should throw an IllegalArgumentException");
		} catch (IllegalArgumentException e) {
			assertEquals(globalPool.getNumberOfAvailableMemorySegments(), numBuffers);
		} finally {
			globalPool.destroy();
		}
	}

	/**
	 * Tests {@link NetworkBufferPool#requestMemorySegments(int)} with the {@link NetworkBufferPool}
	 * currently not containing the number of required free segments (currently occupied by a buffer pool).
	 */
	@Test
	public void testRequestMemorySegmentsWithBuffersTaken() throws IOException, InterruptedException {
		final int numBuffers = 10;

		NetworkBufferPool networkBufferPool = new NetworkBufferPool(numBuffers, 128);

		final List<Buffer> buffers = new ArrayList<>(numBuffers);
		List<MemorySegment> memorySegments = Collections.emptyList();
		Thread bufferRecycler = null;
		BufferPool lbp1 = null;
		try {
			lbp1 = networkBufferPool.createBufferPool(numBuffers / 2, numBuffers);

			// take all buffers (more than the minimum required)
			for (int i = 0; i < numBuffers; ++i) {
				Buffer buffer = lbp1.requestBuffer();
				buffers.add(buffer);
				assertNotNull(buffer);
			}

			// requestMemorySegments() below will wait for buffers
			// this will make sure that enough buffers are freed eventually for it to continue
			final OneShotLatch isRunning = new OneShotLatch();
			bufferRecycler = new Thread(() -> {
				try {
					isRunning.trigger();
					Thread.sleep(100);
				} catch (InterruptedException ignored) {
				}

				for (Buffer buffer : buffers) {
					buffer.recycleBuffer();
				}
			});
			bufferRecycler.start();

			// take more buffers than are freely available at the moment via requestMemorySegments()
			isRunning.await();
			memorySegments = networkBufferPool.requestMemorySegments(numBuffers / 2);
			assertThat(memorySegments, not(hasItem(nullValue())));
		} finally {
			if (bufferRecycler != null) {
				bufferRecycler.join();
			}
			if (lbp1 != null) {
				lbp1.lazyDestroy();
			}
			networkBufferPool.recycleMemorySegments(memorySegments);
			networkBufferPool.destroy();
		}
	}

	/**
	 * Tests {@link NetworkBufferPool#requestMemorySegments(int)} with an exception occurring during
	 * the call to {@link NetworkBufferPool#redistributeBuffers()}.
	 */
	@Test
	public void testRequestMemorySegmentsExceptionDuringBufferRedistribution() throws IOException {
		final int numBuffers = 3;

		NetworkBufferPool networkBufferPool = new NetworkBufferPool(numBuffers, 128);

		final List<Buffer> buffers = new ArrayList<>(numBuffers);
		List<MemorySegment> memorySegments = Collections.emptyList();
		BufferPool bufferPool = networkBufferPool.createBufferPool(1, numBuffers);
		// make releaseMemory calls always fail:
		bufferPool.setBufferPoolOwner(numBuffersToRecycle -> {
			throw new TestIOException();
		});

		try {
			// take all but one buffer
			for (int i = 0; i < numBuffers - 1; ++i) {
				Buffer buffer = bufferPool.requestBuffer();
				buffers.add(buffer);
				assertNotNull(buffer);
			}

			// this will ask the buffer pool to release its excess buffers which should fail
			memorySegments = networkBufferPool.requestMemorySegments(2);
			fail("Requesting memory segments should have thrown during buffer pool redistribution.");
		} catch (TestIOException e) {
			// test indirectly for NetworkBufferPool#numTotalRequiredBuffers being correct:
			// -> creating a new buffer pool should not fail with "insufficient number of network
			//    buffers" and instead only with the TestIOException from redistributing buffers in
			//    bufferPool
			expectedException.expect(TestIOException.class);
			networkBufferPool.createBufferPool(2, 2);
		} finally {
			for (Buffer buffer : buffers) {
				buffer.recycleBuffer();
			}
			bufferPool.lazyDestroy();
			networkBufferPool.recycleMemorySegments(memorySegments);
			networkBufferPool.destroy();
		}
	}

	@Test
	public void testCreateBufferPoolExceptionDuringBufferRedistribution() throws IOException {
		final int numBuffers = 3;
		final NetworkBufferPool networkBufferPool = new NetworkBufferPool(numBuffers, 128);

		final List<Buffer> buffers = new ArrayList<>(numBuffers);
		BufferPool bufferPool = networkBufferPool.createBufferPool(1, numBuffers);
		bufferPool.setBufferPoolOwner(
			numBuffersToRecycle -> {
				throw new TestIOException();
			});

		try {

			for (int i = 0; i < numBuffers; i++) {
				Buffer buffer = bufferPool.requestBuffer();
				buffers.add(buffer);
				assertNotNull(buffer);
			}

			try {
				networkBufferPool.createBufferPool(1, numBuffers);
				fail("Should have failed because the other buffer pool does not support memory release.");
			} catch (TestIOException expected) {
			}

			// destroy the faulty buffer pool
			for (Buffer buffer : buffers) {
				buffer.recycleBuffer();
			}
			buffers.clear();
			bufferPool.lazyDestroy();

			// now we should be able to create a new buffer pool
			bufferPool = networkBufferPool.createBufferPool(numBuffers, numBuffers);
		} finally {
			for (Buffer buffer : buffers) {
				buffer.recycleBuffer();
			}
			bufferPool.lazyDestroy();
			networkBufferPool.destroy();
		}
	}

	private static final class TestIOException extends IOException {
		private static final long serialVersionUID = -814705441998024472L;
	}

	/**
	 * Tests {@link NetworkBufferPool#requestMemorySegments(int)}, verifying it may be aborted in
	 * case of a concurrent {@link NetworkBufferPool#destroy()} call.
	 */
	@Test
	public void testRequestMemorySegmentsInterruptable() throws Exception {
		final int numBuffers = 10;

		NetworkBufferPool globalPool = new NetworkBufferPool(numBuffers, 128);
		MemorySegment segment = globalPool.requestMemorySegment();
		assertNotNull(segment);

		final OneShotLatch isRunning = new OneShotLatch();
		CheckedThread asyncRequest = new CheckedThread() {
			@Override
			public void go() throws Exception {
				isRunning.trigger();
				globalPool.requestMemorySegments(10);
			}
		};
		asyncRequest.start();

		// We want the destroy call inside the blocking part of the globalPool.requestMemorySegments()
		// call above. We cannot guarantee this though but make it highly probable:
		isRunning.await();
		Thread.sleep(10);
		globalPool.destroy();

		segment.free();

		expectedException.expect(IllegalStateException.class);
		expectedException.expectMessage("destroyed");
		try {
			asyncRequest.sync();
		} finally {
			globalPool.destroy();
		}
	}

	/**
	 * Tests {@link NetworkBufferPool#requestMemorySegments(int)}, verifying it may be aborted and
	 * remains in a defined state even if the waiting is interrupted.
	 */
	@Test
	public void testRequestMemorySegmentsInterruptable2() throws Exception {
		final int numBuffers = 10;

		NetworkBufferPool globalPool = new NetworkBufferPool(numBuffers, 128);
		MemorySegment segment = globalPool.requestMemorySegment();
		assertNotNull(segment);

		final OneShotLatch isRunning = new OneShotLatch();
		CheckedThread asyncRequest = new CheckedThread() {
			@Override
			public void go() throws Exception {
				isRunning.trigger();
				globalPool.requestMemorySegments(10);
			}
		};
		asyncRequest.start();

		// We want the destroy call inside the blocking part of the globalPool.requestMemorySegments()
		// call above. We cannot guarantee this though but make it highly probable:
		isRunning.await();
		Thread.sleep(10);
		asyncRequest.interrupt();

		globalPool.recycle(segment);

		try {
			asyncRequest.sync();
		} catch (IOException e) {
			assertThat(e, hasProperty("cause", instanceOf(InterruptedException.class)));

			// test indirectly for NetworkBufferPool#numTotalRequiredBuffers being correct:
			// -> creating a new buffer pool should not fail
			globalPool.createBufferPool(10, 10);
		} finally {
			globalPool.destroy();
		}
	}

	@Test(timeout = 10000L)
	public void testWithoutBufferPoolOwnerAndRequiredEqualsMax() throws Exception {
		final int numBuffers = 100;
		NetworkBufferPool globalPool = new NetworkBufferPool(numBuffers, 128);
		ArrayList<BufferPool> bufferPools = new ArrayList<>(10);

		try {
			// without buffer pool owner and required equals max
			createBufferPools(globalPool, bufferPools, 10, 10, 10);

			checkBufferPoolState(globalPool, 100, numBuffers, 0);
			checkNetworkBufferInsufficient(globalPool);

			destroyBufferPools(bufferPools, 0, 10);

			checkBufferPoolState(globalPool, 0, numBuffers, 0);
		} finally {
			globalPool.destroy();
		}
	}

	@Test(timeout = 10000L)
	public void testWithoutBufferPoolOwnerAndRequiredLessThanMax() throws Exception {
		final int numBuffers = 100;
		NetworkBufferPool globalPool = new NetworkBufferPool(numBuffers, 128);
		ArrayList<BufferPool> bufferPools = new ArrayList<>(10);

		try {
			// without buffer pool owner and required less than max
			createBufferPools(globalPool, bufferPools, 10, 20, 10);

			checkBufferPoolState(globalPool, 100, numBuffers, 0);
			checkNetworkBufferInsufficient(globalPool);

			destroyBufferPools(bufferPools, 0, 10);

			checkBufferPoolState(globalPool, 0, numBuffers, 0);
		} finally {
			globalPool.destroy();
		}
	}

	@Test(timeout = 10000L)
	public void testWithPipelinedBufferPoolOwnerAndRequiredEqualsMax() throws Exception {
		final int numBuffers = 100;
		NetworkBufferPool globalPool = new NetworkBufferPool(numBuffers, 128);
		ArrayList<BufferPool> bufferPools = new ArrayList<>(10);
		HashMap<BufferPool, ArrayList<Buffer>> buffPool2Buffers = new HashMap<>(100);

		try {
			// with buffer pool owner (pipelined result partition) and required equals max
			createBufferPools(globalPool, bufferPools, 10, 10, 10);
			setBufferPoolOwner(bufferPools, ResultPartitionType.PIPELINED, buffPool2Buffers, false);

			checkBufferPoolState(globalPool, 100, numBuffers, 0);
			checkNetworkBufferInsufficient(globalPool);

			requestBuffers(buffPool2Buffers, bufferPools, 10);
			destroyBufferPools(bufferPools, 0, 10);

			checkBufferPoolState(globalPool, 0, 0, 0);

			BufferPool bufferPool = globalPool.createBufferPool(10, 10);
			assertTrue(bufferPool.requestBuffer() == null);

			recycleBuffers(buffPool2Buffers);

			Buffer buffer = bufferPool.requestBuffer();
			assertTrue(buffer != null);
			buffer.recycleBuffer();
			bufferPool.lazyDestroy();

			checkBufferPoolState(globalPool, 0, numBuffers, 0);
		} finally {
			globalPool.destroy();
		}
	}

	@Test(timeout = 10000L)
	public void testWithPipelinedBufferPoolOwnerAndRequiredLessThanMax() throws Exception {
		final int numBuffers = 100;
		NetworkBufferPool globalPool = new NetworkBufferPool(numBuffers, 128);
		ArrayList<BufferPool> bufferPools = new ArrayList<>(10);
		HashMap<BufferPool, ArrayList<Buffer>> buffPool2Buffers = new HashMap<>(100);

		try {
			// with buffer pool owner (pipelined result partition) and required less than max
			createBufferPools(globalPool, bufferPools, 10, 20, 10);
			setBufferPoolOwner(bufferPools, ResultPartitionType.PIPELINED, buffPool2Buffers, false);

			checkBufferPoolState(globalPool, 100, numBuffers, 0);
			checkNetworkBufferInsufficient(globalPool);

			requestBuffers(buffPool2Buffers, bufferPools, 10);
			destroyBufferPools(bufferPools, 0, 10);

			checkBufferPoolState(globalPool, 0, 0, 0);

			BufferPool bufferPool = globalPool.createBufferPool(10, 10);
			assertTrue(bufferPool.requestBuffer() == null);

			recycleBuffers(buffPool2Buffers);

			Buffer buffer = bufferPool.requestBuffer();
			assertTrue(buffer != null);
			buffer.recycleBuffer();
			bufferPool.lazyDestroy();

			checkBufferPoolState(globalPool, 0, numBuffers, 0);
		} finally {
			globalPool.destroy();
		}
	}

	@Test(timeout = 10000L)
	public void testWithBlockingBufferPoolOwnerAndRequiredEqualsMax() throws Exception {
		final int numBuffers = 100;
		NetworkBufferPool globalPool = new NetworkBufferPool(numBuffers, 128);
		ArrayList<BufferPool> bufferPools = new ArrayList<>(10);
		HashMap<BufferPool, ArrayList<Buffer>> buffPool2Buffers = new HashMap<>(100);

		try {
			// with buffer pool owner (blocking result partition) and required equals max
			createBufferPools(globalPool, bufferPools, 10, 10, 10);
			setBufferPoolOwner(bufferPools, ResultPartitionType.BLOCKING, buffPool2Buffers, true);

			checkBufferPoolState(globalPool, 100, numBuffers, 0);

			requestBuffers(buffPool2Buffers, bufferPools, 10);
			checkNetworkBufferInsufficient(globalPool);

			bufferPools.get(0).lazyDestroy();

			checkBufferPoolState(globalPool, 100, 0, 1);

			BufferPool bufferPool = globalPool.createBufferPool(10, 10);
			setBufferPoolOwner(bufferPool, ResultPartitionType.BLOCKING, buffPool2Buffers, false);

			checkBufferPoolState(globalPool, 100, 10, 0);

			requestBuffers(buffPool2Buffers, bufferPool, 10);
			buffPool2Buffers.remove(bufferPools.get(0));

			checkBufferPoolState(globalPool, 100, 0, 0);
			checkNetworkBufferInsufficient(globalPool);

			bufferPools.get(1).lazyDestroy();

			checkBufferPoolState(globalPool, 100, 0, 1);

			recycleBuffers(buffPool2Buffers.get(bufferPools.get(1)));

			checkBufferPoolState(globalPool, 100, 10, 1);

			bufferPools.get(1).notifyBufferPoolOwnerReleased();

			checkBufferPoolState(globalPool, 90, 10, 0);

			buffPool2Buffers.remove(bufferPools.get(1));
			notifyBufferPoolOwnerReleased(buffPool2Buffers);

			checkBufferPoolState(globalPool, 0, numBuffers, 0);
		} finally {
			globalPool.destroy();
		}
	}

	@Test(timeout = 10000L)
	public void testWithBlockingBufferPoolOwnerAndRequiredLessThanMax() throws Exception {
		final int numBuffers = 100;
		NetworkBufferPool globalPool = new NetworkBufferPool(numBuffers, 128);
		ArrayList<BufferPool> bufferPools = new ArrayList<>(10);
		HashMap<BufferPool, ArrayList<Buffer>> buffPool2Buffers = new HashMap<>(100);

		try {
			// with buffer pool owner (blocking result partition) and required less than max
			createBufferPools(globalPool, bufferPools, 10, 20, 5);
			setBufferPoolOwner(bufferPools, ResultPartitionType.BLOCKING, buffPool2Buffers, true);

			checkBufferPoolState(globalPool, 50, numBuffers, 0);

			requestBuffers(buffPool2Buffers, bufferPools.subList(0, 4), 10);

			checkBufferPoolState(globalPool, 50, 60, 0);

			requestBuffers(buffPool2Buffers, bufferPools.get(4), 20);

			checkBufferPoolState(globalPool, 50, 40, 0);

			BufferPool bufferPool = globalPool.createBufferPool(50, 50);

			checkBufferPoolState(globalPool, 100, 50, 0);
			checkNetworkBufferInsufficient(globalPool);

			bufferPools.get(4).lazyDestroy();

			checkBufferPoolState(globalPool, 90, 60, 0);

			BufferPool newBufPool = globalPool.createBufferPool(10, 20);

			checkBufferPoolState(globalPool, 100, 60, 0);

			buffPool2Buffers.remove(bufferPools.get(4));
			bufferPool.lazyDestroy();

			checkBufferPoolState(globalPool, 50, 60, 0);

			newBufPool.lazyDestroy();

			checkBufferPoolState(globalPool, 40, 60, 0);

			notifyBufferPoolOwnerReleased(buffPool2Buffers);

			checkBufferPoolState(globalPool, 0, numBuffers, 0);
		} finally {
			globalPool.destroy();
		}
	}

	private void createBufferPools(
		NetworkBufferPool globalPool,
		ArrayList<BufferPool> bufferPools,
		int numRequiredBuffers,
		int maxUsedBuffers,
		int numBufferPools) throws Exception {
		for (int i = 0; i < numBufferPools; ++i) {
			BufferPool localBufferPool = globalPool.createBufferPool(numRequiredBuffers, maxUsedBuffers);
			bufferPools.add(localBufferPool);
		}
	}

	private void checkBufferPoolState(
		NetworkBufferPool globalPool,
		int numTotalRequiredBuffers,
		int numberOfAvailableMemorySegments,
		int numBufferPoolsToDestroy) {

		assertEquals(numTotalRequiredBuffers, globalPool.getNumTotalRequiredBuffers());
		assertEquals(numberOfAvailableMemorySegments, globalPool.getNumberOfAvailableMemorySegments());
		assertEquals(numBufferPoolsToDestroy, globalPool.getNumBufferPoolsToDestroy());
	}

	private void checkNetworkBufferInsufficient(NetworkBufferPool globalPool) {
		try {
			globalPool.createBufferPool(1, 1);
			fail();
		} catch (IOException e) {
			// should throw IOException
		}
	}

	private void destroyBufferPools(ArrayList<BufferPool> bufferPools, int startIndex, int endIndex) {
		for (int i = startIndex; i < endIndex; ++i) {
			bufferPools.get(i).lazyDestroy();
		}
	}

	private void setBufferPoolOwner(
		BufferPool bufferPool,
		ResultPartitionType partitionType,
		HashMap<BufferPool, ArrayList<Buffer>> buffPool2Buffers,
		boolean allowOwnerReleaseMemory) throws IOException {
		InternalResultPartition owner = mock(InternalResultPartition.class);
		when(owner.getPartitionType()).thenReturn(partitionType);
		bufferPool.setBufferPoolOwner(owner);
		if (allowOwnerReleaseMemory) {
			allowOwnerReleaseMemory(buffPool2Buffers, bufferPool, owner);
		}
	}

	private void setBufferPoolOwner(
		ArrayList<BufferPool> bufferPools,
		ResultPartitionType partitionType,
		HashMap<BufferPool, ArrayList<Buffer>> buffPool2Buffers,
		boolean allowOwnerReleaseMemory) throws IOException {
		for (BufferPool bufferPool: bufferPools) {
			setBufferPoolOwner(bufferPool, partitionType, buffPool2Buffers, allowOwnerReleaseMemory);
		}
	}

	private void allowOwnerReleaseMemory(
		HashMap<BufferPool, ArrayList<Buffer>> buffPool2Buffers,
		BufferPool bufferPool,
		InternalResultPartition owner) throws IOException {
		doAnswer(new Answer() {
			@Override
			public Object answer(InvocationOnMock invocationOnMock) throws Throwable {
				for (Buffer buffer: buffPool2Buffers.get(bufferPool)) {
					buffer.recycleBuffer();
				}
				return null;
			}
		}).when(owner).releaseMemory(anyInt());
	}

	private void requestBuffers(
		HashMap<BufferPool, ArrayList<Buffer>> buffPool2Buffers,
		BufferPool bufferPool,
		int numBuffers) throws IOException, InterruptedException {
		ArrayList<Buffer> buffers = new ArrayList<>();
		for (int i = 0; i < numBuffers; ++i) {
			buffers.add(bufferPool.requestBufferBlocking());
		}
		buffPool2Buffers.put(bufferPool, buffers);
	}

	private void requestBuffers(
		HashMap<BufferPool, ArrayList<Buffer>> buffPool2Buffers,
		List<BufferPool> bufferPools,
		int numBuffers) throws IOException, InterruptedException {
		for (BufferPool bufferPool: bufferPools) {
			requestBuffers(buffPool2Buffers, bufferPool, numBuffers);
		}
	}

	private void recycleBuffers(ArrayList<Buffer> buffers) {
		for (Buffer buffer: buffers) {
			buffer.recycleBuffer();
		}
		buffers.clear();
	}

	private void recycleBuffers(HashMap<BufferPool, ArrayList<Buffer>> buffPool2Buffers) {
		for (ArrayList<Buffer> buffers: buffPool2Buffers.values()) {
			recycleBuffers(buffers);
		}
	}

	private void notifyBufferPoolOwnerReleased(HashMap<BufferPool, ArrayList<Buffer>> buffPool2Buffers) {
		for (BufferPool bufPool: buffPool2Buffers.keySet()) {
			bufPool.lazyDestroy();
			for (Buffer buf: buffPool2Buffers.get(bufPool)) {
				buf.recycleBuffer();
			}
			bufPool.notifyBufferPoolOwnerReleased();
		}
	}
}
