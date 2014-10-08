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

import org.apache.flink.runtime.io.network.netty.Util;
import org.apache.flink.runtime.util.event.EventListener;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.mockito.Matchers;
import org.mockito.Mockito;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;
import static org.mockito.Mockito.mock;

public class LocalBufferPoolTest {

	private final static int numBuffers = 1024;

	private final static int memorySegmentSize = 128;

	private NetworkBufferPool networkBufferPool;

	private BufferPool localBufferPool;

	@Before
	public void setupLocalBufferPool() {
		networkBufferPool = new NetworkBufferPool(numBuffers, memorySegmentSize);
		localBufferPool = new LocalBufferPool(networkBufferPool, 1);

		assertEquals(0, localBufferPool.getNumAvailableBuffers());
	}

	@After
	public void destroyAndVerifyAllBuffersReturned() {
		if (!localBufferPool.isDestroyed()) {
			localBufferPool.destroy();
		}

		String msg = "Did not return all buffers to memory segment pool after test.";
		assertEquals(msg, numBuffers, networkBufferPool.getNumAvailableMemorySegments());
	}

	@Test
	public void testRequestMoreThanAvailable() throws IOException {
		localBufferPool.setNumBuffers(numBuffers);

		List<BufferFuture> requests = new ArrayList<BufferFuture>(numBuffers);

		for (int i = 1; i <= numBuffers; i++) {
			BufferFuture bf = localBufferPool.requestBuffer();

			assertEquals(i, getNumRequestedFromMemorySegmentPool());
			assertNotNull(bf.getBuffer());

			requests.add(bf);
		}

		{
			// One more...
			BufferFuture bf = localBufferPool.requestBuffer();
			assertEquals(numBuffers, getNumRequestedFromMemorySegmentPool());
			assertNull(bf.getBuffer());

			bf.cancel();
		}

		// Recycle all... the cancelled BufferFuture should recycle, when it's
		// handed a buffer.
		for (BufferFuture bf : requests) {
			bf.getBuffer().recycle();
		}
	}

	@Test(expected = IllegalArgumentException.class)
	public void testRequestTooLarge() throws IOException {
		// Request too large buffer for the pool
		localBufferPool.requestBuffer(memorySegmentSize * 2);
	}

	@Test
	public void testRequestSmall() throws IOException {
		// Request smaller buffer and verify size
		int size = memorySegmentSize / 2;
		Buffer buffer = localBufferPool.requestBuffer(size).getBuffer();

		assertEquals(size, buffer.getSize());

		buffer.recycle();
	}

	@Test
	public void testHigherPriorityOfLocalBuffers() {
		localBufferPool.setNumBuffers(numBuffers);

		for (int i = 0; i < numBuffers; i++) {
			BufferFuture bf = localBufferPool.requestBuffer();

			// Ensure that local buffers are preferred over the global pool
			assertEquals(1, getNumRequestedFromMemorySegmentPool());
			assertNotNull(bf.getBuffer());

			bf.getBuffer().recycle();
		}
	}

	@Test(expected = IllegalStateException.class)
	public void testRequestAfterDestroy() throws IOException {
		localBufferPool.destroy();

		localBufferPool.requestBuffer();
	}

	@Test
	public void testRecycleAfterDestroy() throws IOException {
		localBufferPool.setNumBuffers(numBuffers);

		List<BufferFuture> requests = new ArrayList<BufferFuture>(numBuffers);

		for (int i = 0; i < numBuffers; i++) {
			requests.add(localBufferPool.requestBuffer());
		}

		localBufferPool.destroy();

		// All buffers have been requested, but can not be returned yet.
		assertEquals(numBuffers, getNumRequestedFromMemorySegmentPool());

		// Recycle should return buffers to memory segment pool
		for (BufferFuture bf : requests) {
			bf.getBuffer().recycle();
		}
	}

	@Test
	public void testRecycleExcessBuffersAfterRecycling() throws Exception {
		localBufferPool.setNumBuffers(numBuffers);

		List<BufferFuture> requests = new ArrayList<BufferFuture>(numBuffers);

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
			requests.remove(0).getBuffer().recycle();
			assertEquals(numBuffers - i, getNumRequestedFromMemorySegmentPool());
		}

		for (BufferFuture bf : requests) {
			bf.getBuffer().recycle();
		}
	}

	@Test
	public void testRecycleExcessBuffersAfterChangingNumBuffers() throws Exception {
		localBufferPool.setNumBuffers(numBuffers);

		List<BufferFuture> requests = new ArrayList<BufferFuture>(numBuffers);

		// Request all buffers
		for (int i = 1; i <= numBuffers; i++) {
			requests.add(localBufferPool.requestBuffer());
		}

		// Recycle all
		for (BufferFuture bf : requests) {
			bf.getBuffer().recycle();
		}

		assertEquals(numBuffers, localBufferPool.getNumAvailableBuffers());

		localBufferPool.setNumBuffers(numBuffers / 2);

		assertEquals(numBuffers / 2, localBufferPool.getNumAvailableBuffers());
	}

	@Test(expected = IllegalArgumentException.class)
	public void testSetLessThanRequiredNumBuffers() {
		localBufferPool.setNumBuffers(1);

		localBufferPool.setNumBuffers(0);
	}

	// ------------------------------------------------------------------------
	// Pending requests and integration with buffer futures
	// ------------------------------------------------------------------------

	@Test
	public void testPendingRequestWithListenerAfterRecycle() throws Exception {
		EventListener<BufferFuture> listener = Mockito.mock(EventListener.class);

		localBufferPool.setNumBuffers(1);

		BufferFuture available = localBufferPool.requestBuffer();
		BufferFuture unavailable = localBufferPool.requestBuffer();

		unavailable.addListener(listener);

		available.getBuffer().recycle();

		assertTrue(unavailable.isDone());
		assertTrue(unavailable.isSuccess());
		assertFalse(unavailable.isCancelled());

		unavailable.getBuffer().recycle();
	}

	@Test
	public void testPendingRequestWithListenerAfterChangingNumBuffers() throws Exception {
		EventListener<BufferFuture> listener = Mockito.mock(EventListener.class);

		localBufferPool.setNumBuffers(1);

		BufferFuture available = localBufferPool.requestBuffer();

		BufferFuture unavailable = localBufferPool.requestBuffer().addListener(listener);

		localBufferPool.setNumBuffers(2);

		// Verify call to listener
		Mockito.verify(listener, Mockito.times(1)).onEvent(Matchers.any(BufferFuture.class));

		assertTrue(unavailable.isDone());
		assertTrue(unavailable.isSuccess());
		assertFalse(unavailable.isCancelled());

		available.getBuffer().recycle();
		unavailable.getBuffer().recycle();
	}

	@Test
	public void testCancelPendingRequestBeforeRecycle() {
		localBufferPool.setNumBuffers(1);

		BufferFuture available = localBufferPool.requestBuffer();

		BufferFuture unavailable = localBufferPool.requestBuffer();

		unavailable.cancel();

		available.getBuffer().recycle();

		assertNull(unavailable.getBuffer());
	}

	@Test
	public void testCancelOneOfTwoPendingRequestsBeforeRecycle() {
		localBufferPool.setNumBuffers(1);

		BufferFuture available = localBufferPool.requestBuffer();

		BufferFuture cancelled = localBufferPool.requestBuffer();
		BufferFuture waiting = localBufferPool.requestBuffer();

		cancelled.cancel();

		available.getBuffer().recycle();

		// The cancelled future should not get the available buffer...
		assertNull(cancelled.getBuffer());
		// ...the still waiting future should, though.
		assertNotNull(waiting.getBuffer());

		waiting.getBuffer().recycle();
	}

	@Test
	public void testCancelPendingRequestsAfterDestroy() throws IOException {
		localBufferPool.setNumBuffers(numBuffers);

		List<BufferFuture> available = new ArrayList<BufferFuture>(numBuffers);
		List<BufferFuture> unavailable = new ArrayList<BufferFuture>(numBuffers);

		for (int i = 0; i < numBuffers; i++) {
			available.add(localBufferPool.requestBuffer());
		}

		for (int i = 0; i < numBuffers; i++) {
			unavailable.add(localBufferPool.requestBuffer());
		}

		localBufferPool.destroy();

		for (int i = 0; i < numBuffers; i++) {
			available.remove(0).getBuffer().recycle();
		}

		for (int i = numBuffers; i < numBuffers; i++) {
			assertTrue(unavailable.remove(0).isCancelled());
		}
	}

	// ------------------------------------------------------------------------
	// Concurrent requests
	// ------------------------------------------------------------------------

	@Test
	public void testConcurrentRequestRecycle() throws ExecutionException, InterruptedException {
		int numConcurrentTasks = 128;
		int numBuffersToRequestPerTask = 1024;

		localBufferPool.setNumBuffers(numConcurrentTasks);

		ExecutorService executor = Executors.newCachedThreadPool(Util.getNamedThreadFactory("Buffer requester"));

		Future<Boolean>[] taskResults = new Future[numConcurrentTasks];
		for (int i = 0; i < numConcurrentTasks; i++) {
			taskResults[i] = executor.submit(new BufferRequesterTask(localBufferPool, numBuffersToRequestPerTask));
		}

		for (int i = 0; i < numConcurrentTasks; i++) {
			assertTrue(taskResults[i].get());
		}
	}

	// ------------------------------------------------------------------------
	// Helpers
	// ------------------------------------------------------------------------

	private int getNumRequestedFromMemorySegmentPool() {
		return networkBufferPool.getNumMemorySegments() - networkBufferPool.getNumAvailableMemorySegments();
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
					Buffer buffer = bufferProvider.requestBuffer().waitForBuffer().getBuffer();
					buffer.recycle();
				}
			}
			catch (Throwable t) {
				return false;
			}

			return true;
		}
	}
}
