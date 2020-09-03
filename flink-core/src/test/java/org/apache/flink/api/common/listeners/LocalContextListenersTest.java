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

package org.apache.flink.api.common.listeners;

import org.apache.flink.api.common.ExecutionConfig;
import org.apache.flink.util.TestLogger;

import org.junit.Test;

import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;

import static org.junit.Assert.assertEquals;

/**
 * Unit tests for {@link LocalContextListeners}.
 */
public class LocalContextListenersTest extends TestLogger {

	private static final AtomicInteger contextOpen = new AtomicInteger();
	private static final AtomicInteger threadOpen = new AtomicInteger();

	@Test
	public void testListenerContract() throws Exception {
		ExecutionConfig config = new ExecutionConfig();
		config.registerLocalContextListener(ListenerImpl.class);
		final ClassLoader cl = getClass().getClassLoader();
		final CountDownLatch latch = new CountDownLatch(3);
		final AtomicReference<Exception> fail = new AtomicReference<>();
		final CountDownLatch o1 = new CountDownLatch(1);
		final CountDownLatch c1 = new CountDownLatch(1);
		Thread t1 = new Thread(() -> {
			try {
				LocalContextListeners.open(cl, config);
				o1.countDown();
				c1.await(1, TimeUnit.SECONDS);
				LocalContextListeners.close(cl, config);
			} catch (Exception e) {
				fail.compareAndSet(null, e);
			}
		});
		final CountDownLatch o2 = new CountDownLatch(1);
		final CountDownLatch c2 = new CountDownLatch(1);
		Thread t2 = new Thread(() -> {
			try {
				LocalContextListeners.open(cl, config);
				o2.countDown();
				c2.await(1, TimeUnit.SECONDS);
				LocalContextListeners.close(cl, config);
			} catch (Exception e) {
				fail.compareAndSet(null, e);
			}
		});
		final CountDownLatch o3 = new CountDownLatch(1);
		final CountDownLatch c3 = new CountDownLatch(1);
		Thread t3 = new Thread(() -> {
			try {
				LocalContextListeners.open(cl, config);
				o3.countDown();
				c3.await(1, TimeUnit.SECONDS);
				LocalContextListeners.close(cl, config);
			} catch (Exception e) {
				fail.compareAndSet(null, e);
			}
		});

		assertEquals(0, contextOpen.get());
		assertEquals(0, threadOpen.get());

		t1.start();
		o1.await(1, TimeUnit.SECONDS);
		assertEquals(1, contextOpen.get());
		assertEquals(1, threadOpen.get());

		t2.start();
		o2.await(1, TimeUnit.SECONDS);
		assertEquals(1, contextOpen.get());
		assertEquals(2, threadOpen.get());

		t3.start();
		o3.await(1, TimeUnit.SECONDS);
		assertEquals(1, contextOpen.get());
		assertEquals(3, threadOpen.get());

		c1.countDown();
		t1.join();
		assertEquals(1, contextOpen.get());
		assertEquals(2, threadOpen.get());

		c2.countDown();
		t2.join();
		assertEquals(1, contextOpen.get());
		assertEquals(1, threadOpen.get());

		c3.countDown();
		t3.join();
		assertEquals(0, contextOpen.get());
		assertEquals(0, threadOpen.get());
	}

	/**
	 * Test harness.
	 */
	public static class ListenerImpl implements LocalContextListener {
		@Override
		public void openContext(ExecutionConfig executionConfig) throws Exception {
			contextOpen.incrementAndGet();
		}

		@Override
		public void closeContext(ExecutionConfig executionConfig) throws Exception {
			contextOpen.decrementAndGet();
		}

		@Override
		public void openThread(ExecutionConfig executionConfig) throws Exception {
			threadOpen.incrementAndGet();
		}

		@Override
		public void closeThread(ExecutionConfig executionConfig) throws Exception {
			threadOpen.decrementAndGet();
		}
	}
}
