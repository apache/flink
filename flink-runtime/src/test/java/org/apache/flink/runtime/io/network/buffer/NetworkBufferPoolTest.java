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
import org.apache.flink.util.TestLogger;

import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;

import java.io.IOException;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.atomic.AtomicReference;

import static org.apache.flink.util.Preconditions.checkNotNull;
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

			NetworkBufferPool globalPool = new NetworkBufferPool(numBuffers, bufferSize, 1);
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
			NetworkBufferPool globalPool = new NetworkBufferPool(10, 128, 1);

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
	 * Tests {@link NetworkBufferPool#requestMemorySegments()} with the {@link NetworkBufferPool}
	 * currently containing the number of required free segments.
	 */
	@Test
	public void testRequestMemorySegmentsLessThanTotalBuffers() throws Exception {
		final int numBuffers = 10;

		NetworkBufferPool globalPool = new NetworkBufferPool(numBuffers, 128, numBuffers / 2);

		List<MemorySegment> memorySegments = Collections.emptyList();
		try {
			memorySegments = globalPool.requestMemorySegments();
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
	 * Tests {@link NetworkBufferPool#requestMemorySegments()} with the number of required
	 * buffers exceeding the capacity of {@link NetworkBufferPool}.
	 */
	@Test
	public void testRequestMemorySegmentsMoreThanTotalBuffers() throws Exception {
		final int numBuffers = 10;

		NetworkBufferPool globalPool = new NetworkBufferPool(numBuffers, 128, numBuffers + 1);

		try {
			globalPool.requestMemorySegments();
			fail("Should throw an IOException");
		} catch (IOException e) {
			assertEquals(globalPool.getNumberOfAvailableMemorySegments(), numBuffers);
		} finally {
			globalPool.destroy();
		}
	}

	/**
	 * Tests {@link NetworkBufferPool} constructor with the invalid argument to
	 * cause exception.
	 */
	@Test(expected = IllegalArgumentException.class)
	public void testRequestMemorySegmentsWithInvalidArgument() {
		// the number of requested buffers should be larger than zero
		NetworkBufferPool globalPool = new NetworkBufferPool(10, 128, 0);
		globalPool.destroy();
		fail("Should throw an IllegalArgumentException");
	}

	/**
	 * Tests {@link NetworkBufferPool#requestMemorySegments()} with the {@link NetworkBufferPool}
	 * currently not containing the number of required free segments (currently occupied by a buffer pool).
	 */
	@Test
	public void testRequestMemorySegmentsWithBuffersTaken() throws IOException, InterruptedException {
		final int numBuffers = 10;

		NetworkBufferPool networkBufferPool = new NetworkBufferPool(numBuffers, 128, numBuffers / 2);

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
			memorySegments = networkBufferPool.requestMemorySegments();
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
	 * Tests {@link NetworkBufferPool#requestMemorySegments()} with an exception occurring during
	 * the call to {@link NetworkBufferPool#redistributeBuffers()}.
	 */
	@Test
	public void testRequestMemorySegmentsExceptionDuringBufferRedistribution() throws IOException {
		final int numBuffers = 3;

		NetworkBufferPool networkBufferPool = new NetworkBufferPool(numBuffers, 128, 2);

		final List<Buffer> buffers = new ArrayList<>(numBuffers);
		List<MemorySegment> memorySegments = Collections.emptyList();
		BufferPool bufferPool = networkBufferPool.createBufferPool(1, numBuffers,
			// make releaseMemory calls always fail:
			numBuffersToRecycle -> {
				throw new TestIOException();
		}, 0, Integer.MAX_VALUE);

		try {
			// take all but one buffer
			for (int i = 0; i < numBuffers - 1; ++i) {
				Buffer buffer = bufferPool.requestBuffer();
				buffers.add(buffer);
				assertNotNull(buffer);
			}

			// this will ask the buffer pool to release its excess buffers which should fail
			memorySegments = networkBufferPool.requestMemorySegments();
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
		final NetworkBufferPool networkBufferPool = new NetworkBufferPool(numBuffers, 128, 1);

		final List<Buffer> buffers = new ArrayList<>(numBuffers);
		BufferPool bufferPool = networkBufferPool.createBufferPool(1, numBuffers,
			numBuffersToRecycle -> {
				throw new TestIOException();
		}, 0, Integer.MAX_VALUE);

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
	 * Tests {@link NetworkBufferPool#requestMemorySegments()}, verifying it may be aborted in
	 * case of a concurrent {@link NetworkBufferPool#destroy()} call.
	 */
	@Test
	public void testRequestMemorySegmentsInterruptable() throws Exception {
		final int numBuffers = 10;

		NetworkBufferPool globalPool = new NetworkBufferPool(numBuffers, 128, 10);
		MemorySegment segment = globalPool.requestMemorySegment();
		assertNotNull(segment);

		final OneShotLatch isRunning = new OneShotLatch();
		CheckedThread asyncRequest = new CheckedThread() {
			@Override
			public void go() throws Exception {
				isRunning.trigger();
				globalPool.requestMemorySegments();
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
	 * Tests {@link NetworkBufferPool#requestMemorySegments()}, verifying it may be aborted and
	 * remains in a defined state even if the waiting is interrupted.
	 */
	@Test
	public void testRequestMemorySegmentsInterruptable2() throws Exception {
		final int numBuffers = 10;

		NetworkBufferPool globalPool = new NetworkBufferPool(numBuffers, 128, 10);
		MemorySegment segment = globalPool.requestMemorySegment();
		assertNotNull(segment);

		final OneShotLatch isRunning = new OneShotLatch();
		CheckedThread asyncRequest = new CheckedThread() {
			@Override
			public void go() throws Exception {
				isRunning.trigger();
				globalPool.requestMemorySegments();
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

	/**
	 * Tests {@link NetworkBufferPool#requestMemorySegments()} and verifies it will end exceptionally
	 * when failing to acquire all the segments in the specific timeout.
	 */
	@Test
	public void testRequestMemorySegmentsTimeout() throws Exception {
		final int numBuffers = 10;
		final int numberOfSegmentsToRequest = 2;
		final Duration requestSegmentsTimeout = Duration.ofMillis(50L);

		NetworkBufferPool globalPool = new NetworkBufferPool(
				numBuffers,
				128,
				numberOfSegmentsToRequest,
				requestSegmentsTimeout);

		BufferPool localBufferPool = globalPool.createBufferPool(0, numBuffers);
		for (int i = 0; i < numBuffers; ++i) {
			localBufferPool.requestBuffer();
		}

		assertEquals(0, globalPool.getNumberOfAvailableMemorySegments());

		CheckedThread asyncRequest = new CheckedThread() {
			@Override
			public void go() throws Exception {
				globalPool.requestMemorySegments();
			}
		};

		asyncRequest.start();

		expectedException.expect(IOException.class);
		expectedException.expectMessage("Timeout");

		try {
			asyncRequest.sync();
		} finally {
			globalPool.destroy();
		}
	}

	/**
	 * Tests {@link NetworkBufferPool#isAvailable()}, verifying that the buffer availability is correctly
	 * maintained after memory segments are requested by {@link NetworkBufferPool#requestMemorySegment()}
	 * and recycled by {@link NetworkBufferPool#recycle(MemorySegment)}.
	 */
	@Test
	public void testIsAvailableOrNotAfterRequestAndRecycleSingleSegment() throws Exception {
		final int numBuffers = 2;

		final NetworkBufferPool globalPool = new NetworkBufferPool(numBuffers, 128, 1);

		try {
			// the global pool should be in available state initially
			assertTrue(globalPool.getAvailableFuture().isDone());

			// request the first segment
			final MemorySegment segment1 = checkNotNull(globalPool.requestMemorySegment());
			assertTrue(globalPool.getAvailableFuture().isDone());

			// request the second segment
			final MemorySegment segment2 = checkNotNull(globalPool.requestMemorySegment());
			assertFalse(globalPool.getAvailableFuture().isDone());

			final CompletableFuture<?> availableFuture = globalPool.getAvailableFuture();

			// recycle the first segment
			globalPool.recycle(segment1);
			assertTrue(availableFuture.isDone());
			assertTrue(globalPool.getAvailableFuture().isDone());

			// recycle the second segment
			globalPool.recycle(segment2);
			assertTrue(globalPool.getAvailableFuture().isDone());

		} finally {
			globalPool.destroy();
		}
	}

	/**
	 * Tests {@link NetworkBufferPool#isAvailable()}, verifying that the buffer availability is correctly
	 * maintained after memory segments are requested by {@link NetworkBufferPool#requestMemorySegments()}
	 * and recycled by {@link NetworkBufferPool#recycleMemorySegments(Collection)}.
	 */
	@Test(timeout = 10000L)
	public void testIsAvailableOrNotAfterRequestAndRecycleMultiSegments() throws Exception {
		final int numberOfSegmentsToRequest = 5;
		final int numBuffers = 2 * numberOfSegmentsToRequest;

		final NetworkBufferPool globalPool = new NetworkBufferPool(numBuffers, 128, numberOfSegmentsToRequest);

		try {
			// the global pool should be in available state initially
			assertTrue(globalPool.getAvailableFuture().isDone());

			// request 5 segments
			List<MemorySegment> segments1 = globalPool.requestMemorySegments();
			assertTrue(globalPool.getAvailableFuture().isDone());
			assertEquals(numberOfSegmentsToRequest, segments1.size());

			// request another 5 segments
			List<MemorySegment> segments2 = globalPool.requestMemorySegments();
			assertFalse(globalPool.getAvailableFuture().isDone());
			assertEquals(numberOfSegmentsToRequest, segments2.size());

			// request another 5 segments
			final CountDownLatch latch = new CountDownLatch(1);
			final List<MemorySegment> segments3 = new ArrayList<>(numberOfSegmentsToRequest);
			CheckedThread asyncRequest = new CheckedThread() {
				@Override
				public void go() throws Exception {
					// this request should be blocked until at least 5 segments are recycled
					segments3.addAll(globalPool.requestMemorySegments());
					latch.countDown();
				}
			};
			asyncRequest.start();

			// recycle 5 segments
			CompletableFuture<?> availableFuture = globalPool.getAvailableFuture();
			globalPool.recycleMemorySegments(segments1);
			assertTrue(availableFuture.isDone());

			// wait util the third request is fulfilled
			latch.await();
			assertFalse(globalPool.getAvailableFuture().isDone());
			assertEquals(numberOfSegmentsToRequest, segments3.size());

			// recycle another 5 segments
			globalPool.recycleMemorySegments(segments2);
			assertTrue(globalPool.getAvailableFuture().isDone());

			// recycle the last 5 segments
			globalPool.recycleMemorySegments(segments3);
			assertTrue(globalPool.getAvailableFuture().isDone());

		} finally {
			globalPool.destroy();
		}
	}

	/**
	 * Tests that blocking request of multi local buffer pools can be fulfilled by recycled segments
	 * to the global network buffer pool.
	 */
	@Test(timeout = 10000L)
	public void testBlockingRequestFromMultiLocalBufferPool() throws Exception {
		final int localPoolRequiredSize = 5;
		final int localPoolMaxSize = 10;
		final int numLocalBufferPool = 2;
		final int numberOfSegmentsToRequest = 10;
		final int numBuffers = numLocalBufferPool * localPoolMaxSize;

		final ExecutorService executorService = Executors.newFixedThreadPool(numLocalBufferPool);
		final NetworkBufferPool globalPool = new NetworkBufferPool(numBuffers, 128, numberOfSegmentsToRequest);
		final List<BufferPool> localBufferPools = new ArrayList<>(numLocalBufferPool);

		try {
			// create local buffer pools
			for (int i  = 0; i < numLocalBufferPool; ++i) {
				final BufferPool localPool = globalPool.createBufferPool(localPoolRequiredSize, localPoolMaxSize);
				localBufferPools.add(localPool);
				assertTrue(localPool.getAvailableFuture().isDone());
			}

			// request some segments from the global pool in two different ways
			final List<MemorySegment> segments = new ArrayList<>(numberOfSegmentsToRequest - 1);
			for (int i = 0; i < numberOfSegmentsToRequest - 1; ++i) {
				segments.add(globalPool.requestMemorySegment());
			}
			final List<MemorySegment> exclusiveSegments = globalPool.requestMemorySegments();
			assertTrue(globalPool.getAvailableFuture().isDone());
			for (final BufferPool localPool: localBufferPools) {
				assertTrue(localPool.getAvailableFuture().isDone());
			}

			// blocking request buffers form local buffer pools
			final CountDownLatch latch = new CountDownLatch(numLocalBufferPool);
			final BlockingQueue<BufferBuilder> segmentsRequested = new ArrayBlockingQueue<>(numBuffers);
			final AtomicReference<Throwable> cause = new AtomicReference<>();
			for (final BufferPool localPool: localBufferPools) {
				executorService.submit(() -> {
					try {
						for (int num = localPoolMaxSize; num > 0; --num) {
							segmentsRequested.add(localPool.requestBufferBuilderBlocking());
						}
					} catch (Exception e) {
						cause.set(e);
					} finally {
						latch.countDown();
					}
				});
			}

			// wait until all available buffers are requested
			while (globalPool.getNumberOfAvailableMemorySegments() > 0) {
				Thread.sleep(100);
				assertNull(cause.get());
			}

			final CompletableFuture<?> globalPoolAvailableFuture = globalPool.getAvailableFuture();
			assertFalse(globalPoolAvailableFuture.isDone());

			final List<CompletableFuture<?>> localPoolAvailableFutures = new ArrayList<>(numLocalBufferPool);
			for (BufferPool localPool: localBufferPools) {
				CompletableFuture<?> localPoolAvailableFuture = localPool.getAvailableFuture();
				localPoolAvailableFutures.add(localPoolAvailableFuture);
				assertFalse(localPoolAvailableFuture.isDone());
			}

			// recycle the previously requested segments
			for (MemorySegment segment: segments) {
				globalPool.recycle(segment);
			}
			globalPool.recycleMemorySegments(exclusiveSegments);

			assertTrue(globalPoolAvailableFuture.isDone());
			for (CompletableFuture<?> localPoolAvailableFuture: localPoolAvailableFutures) {
				assertTrue(localPoolAvailableFuture.isDone());
			}

			// wait until all blocking buffer requests finish
			latch.await();

			assertNull(cause.get());
			assertEquals(0, globalPool.getNumberOfAvailableMemorySegments());
			assertFalse(globalPool.getAvailableFuture().isDone());
			for (BufferPool localPool: localBufferPools) {
				assertFalse(localPool.getAvailableFuture().isDone());
				assertEquals(localPoolMaxSize, localPool.bestEffortGetNumOfUsedBuffers());
			}

			// recycle all the requested buffers
			for (BufferBuilder bufferBuilder: segmentsRequested) {
				bufferBuilder.createBufferConsumer().close();
			}

		} finally {
			for (BufferPool bufferPool: localBufferPools) {
				bufferPool.lazyDestroy();
			}
			executorService.shutdown();
			globalPool.destroy();
		}
	}
}
