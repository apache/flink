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

import org.apache.flink.util.TestLogger;

import org.apache.flink.shaded.guava18.com.google.common.collect.Lists;

import org.junit.After;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.Test;
import org.mockito.Mockito;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.Callable;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;
import static org.mockito.Matchers.any;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.powermock.api.mockito.PowerMockito.spy;

/**
 * Tests for the {@link LocalBufferPool}.
 */
public class LocalBufferPoolTest extends TestLogger {

	private static final int numBuffers = 1024;

	private static final int memorySegmentSize = 128;

	private NetworkBufferPool networkBufferPool;

	private BufferPool localBufferPool;

	private static final ExecutorService executor = Executors.newCachedThreadPool();

	@Before
	public void setupLocalBufferPool() {
		networkBufferPool = new NetworkBufferPool(numBuffers, memorySegmentSize);
		localBufferPool = new LocalBufferPool(networkBufferPool, 1);

		assertEquals(0, localBufferPool.getNumberOfAvailableMemorySegments());
	}

	@After
	public void destroyAndVerifyAllBuffersReturned() throws IOException {
		if (!localBufferPool.isDestroyed()) {
			localBufferPool.lazyDestroy();
		}

		String msg = "Did not return all buffers to memory segment pool after test.";
		assertEquals(msg, numBuffers, networkBufferPool.getNumberOfAvailableMemorySegments());
		// no other local buffer pools used than the one above, but call just in case
		networkBufferPool.destroyAllBufferPools();
		networkBufferPool.destroy();
	}

	@AfterClass
	public static void shutdownExecutor() {
		executor.shutdownNow();
	}

	@Test
	public void testRequestMoreThanAvailable() throws IOException {
		localBufferPool.setNumBuffers(numBuffers);

		List<Buffer> requests = new ArrayList<Buffer>(numBuffers);

		for (int i = 1; i <= numBuffers; i++) {
			Buffer buffer = localBufferPool.requestBuffer();

			assertEquals(i, getNumRequestedFromMemorySegmentPool());
			assertNotNull(buffer);

			requests.add(buffer);
		}

		{
			// One more...
			Buffer buffer = localBufferPool.requestBuffer();
			assertEquals(numBuffers, getNumRequestedFromMemorySegmentPool());
			assertNull(buffer);
		}

		for (Buffer buffer : requests) {
			buffer.recycleBuffer();
		}
	}

	@Test
	public void testRequestAfterDestroy() throws IOException {
		localBufferPool.lazyDestroy();

		try {
			localBufferPool.requestBuffer();
			fail("Call should have failed with an IllegalStateException");
		}
		catch (IllegalStateException e) {
			// we expect exactly that
		}
	}

	@Test
	public void testRecycleAfterDestroy() throws IOException {
		localBufferPool.setNumBuffers(numBuffers);

		List<Buffer> requests = new ArrayList<Buffer>(numBuffers);

		for (int i = 0; i < numBuffers; i++) {
			requests.add(localBufferPool.requestBuffer());
		}

		localBufferPool.lazyDestroy();

		// All buffers have been requested, but can not be returned yet.
		assertEquals(numBuffers, getNumRequestedFromMemorySegmentPool());

		// Recycle should return buffers to memory segment pool
		for (Buffer buffer : requests) {
			buffer.recycleBuffer();
		}
	}

	@Test
	public void testRecycleExcessBuffersAfterRecycling() throws Exception {
		localBufferPool.setNumBuffers(numBuffers);

		List<Buffer> requests = new ArrayList<Buffer>(numBuffers);

		// Request all buffers
		for (int i = 1; i <= numBuffers; i++) {
			requests.add(localBufferPool.requestBuffer());
		}

		assertEquals(numBuffers, getNumRequestedFromMemorySegmentPool());

		// Reduce the number of buffers in the local pool
		localBufferPool.setNumBuffers(numBuffers / 2);

		// Need to wait until we recycle the buffers
		assertEquals(numBuffers, getNumRequestedFromMemorySegmentPool());

		for (int i = 1; i < numBuffers / 2; i++) {
			requests.remove(0).recycleBuffer();
			assertEquals(numBuffers - i, getNumRequestedFromMemorySegmentPool());
		}

		for (Buffer buffer : requests) {
			buffer.recycleBuffer();
		}
	}

	@Test
	public void testRecycleExcessBuffersAfterChangingNumBuffers() throws Exception {
		localBufferPool.setNumBuffers(numBuffers);

		List<Buffer> requests = new ArrayList<Buffer>(numBuffers);

		// Request all buffers
		for (int i = 1; i <= numBuffers; i++) {
			requests.add(localBufferPool.requestBuffer());
		}

		// Recycle all
		for (Buffer buffer : requests) {
			buffer.recycleBuffer();
		}

		assertEquals(numBuffers, localBufferPool.getNumberOfAvailableMemorySegments());

		localBufferPool.setNumBuffers(numBuffers / 2);

		assertEquals(numBuffers / 2, localBufferPool.getNumberOfAvailableMemorySegments());
	}

	@Test(expected = IllegalArgumentException.class)
	public void testSetLessThanRequiredNumBuffers() throws IOException {
		localBufferPool.setNumBuffers(1);

		localBufferPool.setNumBuffers(0);
	}

	// ------------------------------------------------------------------------
	// Pending requests and integration with buffer futures
	// ------------------------------------------------------------------------

	@Test
	public void testPendingRequestWithListenersAfterRecycle() throws Exception {
		BufferListener twoTimesListener = createBufferListener(2);
		BufferListener oneTimeListener = createBufferListener(1);

		localBufferPool.setNumBuffers(2);

		Buffer available1 = localBufferPool.requestBuffer();
		Buffer available2 = localBufferPool.requestBuffer();

		assertNull(localBufferPool.requestBuffer());

		assertTrue(localBufferPool.addBufferListener(twoTimesListener));
		assertTrue(localBufferPool.addBufferListener(oneTimeListener));

		// Recycle the first buffer to notify both of the above listeners once
		// and the twoTimesListener will be added into the registeredListeners
		// queue of buffer pool again
		available1.recycleBuffer();

		verify(oneTimeListener, times(1)).notifyBufferAvailable(any(Buffer.class));
		verify(twoTimesListener, times(1)).notifyBufferAvailable(any(Buffer.class));

		// Recycle the second buffer to only notify the twoTimesListener
		available2.recycleBuffer();

		verify(oneTimeListener, times(1)).notifyBufferAvailable(any(Buffer.class));
		verify(twoTimesListener, times(2)).notifyBufferAvailable(any(Buffer.class));
	}

	@Test
	@SuppressWarnings("unchecked")
	public void testCancelPendingRequestsAfterDestroy() throws IOException {
		BufferListener listener = Mockito.mock(BufferListener.class);

		localBufferPool.setNumBuffers(1);

		Buffer available = localBufferPool.requestBuffer();
		Buffer unavailable = localBufferPool.requestBuffer();

		assertNull(unavailable);

		localBufferPool.addBufferListener(listener);

		localBufferPool.lazyDestroy();

		available.recycleBuffer();

		verify(listener, times(1)).notifyBufferDestroyed();
	}

	// ------------------------------------------------------------------------
	// Concurrent requests
	// ------------------------------------------------------------------------

	@Test
	@SuppressWarnings("unchecked")
	public void testConcurrentRequestRecycle() throws ExecutionException, InterruptedException, IOException {
		int numConcurrentTasks = 128;
		int numBuffersToRequestPerTask = 1024;

		localBufferPool.setNumBuffers(numConcurrentTasks);

		Future<Boolean>[] taskResults = new Future[numConcurrentTasks];
		for (int i = 0; i < numConcurrentTasks; i++) {
			taskResults[i] = executor.submit(new BufferRequesterTask(localBufferPool, numBuffersToRequestPerTask));
		}

		for (int i = 0; i < numConcurrentTasks; i++) {
			assertTrue(taskResults[i].get());
		}
	}

	@Test
	public void testDestroyDuringBlockingRequest() throws Exception {
		// Config
		final int numberOfBuffers = 1;

		localBufferPool.setNumBuffers(numberOfBuffers);

		final CountDownLatch sync = new CountDownLatch(1);

		final Callable<List<Buffer>> requester = new Callable<List<Buffer>>() {

			// Request all buffers in a blocking manner.
			@Override
			public List<Buffer> call() throws Exception {
				final List<Buffer> requested = Lists.newArrayList();

				// Request all available buffers
				for (int i = 0; i < numberOfBuffers; i++) {
					requested.add(localBufferPool.requestBufferBlocking());
				}

				// Notify that we've requested all buffers
				sync.countDown();

				// Try to request the next buffer (but pool should be destroyed either right before
				// the request or more likely during the request).
				try {
					localBufferPool.requestBufferBlocking();
					fail("Call should have failed with an IllegalStateException");
				}
				catch (IllegalStateException e) {
					// we expect exactly that
				}

				return requested;
			}
		};

		Future<List<Buffer>> f = executor.submit(requester);

		sync.await();

		localBufferPool.lazyDestroy();

		// Increase the likelihood that the requested is currently in the request call
		Thread.sleep(50);

		// This should return immediately if everything works as expected
		List<Buffer> requestedBuffers = f.get(60, TimeUnit.SECONDS);

		for (Buffer buffer : requestedBuffers) {
			buffer.recycleBuffer();
		}
	}

	@Test
	public void testBoundedBuffer() throws Exception {
		localBufferPool.lazyDestroy();

		localBufferPool = new LocalBufferPool(networkBufferPool, 1, 2);
		assertEquals(0, localBufferPool.getNumberOfAvailableMemorySegments());
		assertEquals(2, localBufferPool.getMaxNumberOfMemorySegments());

		Buffer buffer1, buffer2;

		// check min number of buffers:
		localBufferPool.setNumBuffers(1);
		assertEquals(0, localBufferPool.getNumberOfAvailableMemorySegments());
		assertNotNull(buffer1 = localBufferPool.requestBuffer());
		assertEquals(0, localBufferPool.getNumberOfAvailableMemorySegments());
		assertNull(localBufferPool.requestBuffer());
		assertEquals(0, localBufferPool.getNumberOfAvailableMemorySegments());
		buffer1.recycleBuffer();
		assertEquals(1, localBufferPool.getNumberOfAvailableMemorySegments());

		// check max number of buffers:
		localBufferPool.setNumBuffers(2);
		assertEquals(1, localBufferPool.getNumberOfAvailableMemorySegments());
		assertNotNull(buffer1 = localBufferPool.requestBuffer());
		assertEquals(0, localBufferPool.getNumberOfAvailableMemorySegments());
		assertNotNull(buffer2 = localBufferPool.requestBuffer());
		assertEquals(0, localBufferPool.getNumberOfAvailableMemorySegments());
		assertNull(localBufferPool.requestBuffer());
		assertEquals(0, localBufferPool.getNumberOfAvailableMemorySegments());
		buffer1.recycleBuffer();
		assertEquals(1, localBufferPool.getNumberOfAvailableMemorySegments());
		buffer2.recycleBuffer();
		assertEquals(2, localBufferPool.getNumberOfAvailableMemorySegments());

		// try to set too large buffer size:
		localBufferPool.setNumBuffers(3);
		assertEquals(2, localBufferPool.getNumberOfAvailableMemorySegments());
		assertNotNull(buffer1 = localBufferPool.requestBuffer());
		assertEquals(1, localBufferPool.getNumberOfAvailableMemorySegments());
		assertNotNull(buffer2 = localBufferPool.requestBuffer());
		assertEquals(0, localBufferPool.getNumberOfAvailableMemorySegments());
		assertNull(localBufferPool.requestBuffer());
		assertEquals(0, localBufferPool.getNumberOfAvailableMemorySegments());
		buffer1.recycleBuffer();
		assertEquals(1, localBufferPool.getNumberOfAvailableMemorySegments());
		buffer2.recycleBuffer();
		assertEquals(2, localBufferPool.getNumberOfAvailableMemorySegments());

		// decrease size again
		localBufferPool.setNumBuffers(1);
		assertEquals(1, localBufferPool.getNumberOfAvailableMemorySegments());
		assertNotNull(buffer1 = localBufferPool.requestBuffer());
		assertEquals(0, localBufferPool.getNumberOfAvailableMemorySegments());
		assertNull(localBufferPool.requestBuffer());
		buffer1.recycleBuffer();
		assertEquals(1, localBufferPool.getNumberOfAvailableMemorySegments());
	}

	// ------------------------------------------------------------------------
	// Helpers
	// ------------------------------------------------------------------------

	private int getNumRequestedFromMemorySegmentPool() {
		return networkBufferPool.getTotalNumberOfMemorySegments() - networkBufferPool.getNumberOfAvailableMemorySegments();
	}

	private BufferListener createBufferListener(int notificationTimes) {
		return spy(new BufferListener() {
			AtomicInteger times = new AtomicInteger(0);

			@Override
			public NotificationResult notifyBufferAvailable(Buffer buffer) {
				int newCount = times.incrementAndGet();
				buffer.recycleBuffer();
				if (newCount < notificationTimes) {
					return NotificationResult.BUFFER_USED_NEED_MORE;
				} else {
					return NotificationResult.BUFFER_USED_NO_NEED_MORE;
				}
			}

			@Override
			public void notifyBufferDestroyed() {
			}
		});
	}

	private static class BufferRequesterTask implements Callable<Boolean> {

		private final BufferProvider bufferProvider;

		private final int numBuffersToRequest;

		private BufferRequesterTask(BufferProvider bufferProvider, int numBuffersToRequest) {
			this.bufferProvider = bufferProvider;
			this.numBuffersToRequest = numBuffersToRequest;
		}

		@Override
		public Boolean call() throws Exception {
			try {
				for (int i = 0; i < numBuffersToRequest; i++) {
					Buffer buffer = bufferProvider.requestBufferBlocking();
					buffer.recycleBuffer();
				}
			}
			catch (Throwable t) {
				return false;
			}

			return true;
		}
	}
}
