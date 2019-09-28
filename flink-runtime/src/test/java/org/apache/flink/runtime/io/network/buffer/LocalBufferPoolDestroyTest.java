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

import org.junit.Test;

import java.util.concurrent.atomic.AtomicReference;

import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;

/**
 * Tests for the destruction of a {@link LocalBufferPool}.
 */
public class LocalBufferPoolDestroyTest {

	/**
	 * Tests that a blocking request fails properly if the buffer pool is
	 * destroyed.
	 *
	 * <p>Starts a Thread, which triggers an unsatisfiable blocking buffer
	 * request. After making sure that the Thread is actually waiting in the
	 * blocking call, the buffer pool is destroyed and we check whether the
	 * request Thread threw the expected Exception.
	 */
	@Test
	public void testDestroyWhileBlockingRequest() throws Exception {
		AtomicReference<Exception> asyncException = new AtomicReference<>();

		NetworkBufferPool networkBufferPool = null;
		LocalBufferPool localBufferPool = null;

		try {
			networkBufferPool = new NetworkBufferPool(1, 4096, 1);
			localBufferPool = new LocalBufferPool(networkBufferPool, 1);

			// Drain buffer pool
			assertNotNull(localBufferPool.requestBuffer());
			assertNull(localBufferPool.requestBuffer());

			// Start request Thread
			Thread thread = new Thread(new BufferRequestTask(localBufferPool, asyncException));
			thread.start();

			// Wait for request
			boolean success = false;

			for (int i = 0; i < 50; i++) {
				StackTraceElement[] stackTrace = thread.getStackTrace();
				success = isInBlockingBufferRequest(stackTrace);

				if (success) {
					break;
				} else {
					// Retry
					Thread.sleep(500);
				}
			}

			// Verify that Thread was in blocking request
			assertTrue("Did not trigger blocking buffer request.", success);

			// Destroy the buffer pool
			localBufferPool.lazyDestroy();

			// Wait for Thread to finish
			thread.join();

			// Verify expected Exception
			assertNotNull("Did not throw expected Exception", asyncException.get());
			assertTrue(asyncException.get() instanceof IllegalStateException);
		} finally {
			if (localBufferPool != null) {
				localBufferPool.lazyDestroy();
			}

			if (networkBufferPool != null) {
				networkBufferPool.destroyAllBufferPools();
				networkBufferPool.destroy();
			}
		}
	}

	/**
	 * Returns whether the stack trace represents a Thread in a blocking buffer
	 * request.
	 *
	 * @param stackTrace Stack trace of the Thread to check
	 *
	 * @return Flag indicating whether the Thread is in a blocking buffer
	 * request or not
	 */
	public static boolean isInBlockingBufferRequest(StackTraceElement[] stackTrace) {
		if (stackTrace.length >= 3) {
			return stackTrace[0].getMethodName().equals("wait") &&
					stackTrace[1].getClassName().equals(LocalBufferPool.class.getName());
		} else {
			return false;
		}
	}

	/**
	 * Task triggering a blocking buffer request (the test assumes that no
	 * buffer is available).
	 */
	private static class BufferRequestTask implements Runnable {

		private final BufferPool bufferPool;
		private final AtomicReference<Exception> asyncException;

		public BufferRequestTask(BufferPool bufferPool, AtomicReference<Exception> asyncException) {
			this.bufferPool = bufferPool;
			this.asyncException = asyncException;
		}

		@Override
		public void run() {
			try {
				String msg = "Test assumption violated: expected no available buffer";
				assertNull(msg, bufferPool.requestBuffer());

				bufferPool.requestBufferBlocking();
			} catch (Exception t) {
				asyncException.set(t);
			}
		}
	}
}
