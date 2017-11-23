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

package org.apache.flink.runtime.io.disk.iomanager;

import org.apache.flink.core.memory.MemorySegmentFactory;
import org.apache.flink.runtime.io.network.buffer.Buffer;
import org.apache.flink.runtime.io.network.buffer.FreeingBufferRecycler;
import org.apache.flink.runtime.io.network.util.TestNotificationListener;

import org.junit.AfterClass;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;

import java.io.IOException;
import java.util.concurrent.Callable;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.atomic.AtomicReference;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;
import static org.mockito.Mockito.mock;

/**
 * Tests for {@link AsynchronousBufferFileWriter}.
 */
public class AsynchronousBufferFileWriterTest {
	@Rule
	public ExpectedException exception = ExpectedException.none();

	private static final IOManager ioManager = new IOManagerAsync();

	private static final Buffer mockBuffer = mock(Buffer.class);

	private AsynchronousBufferFileWriter writer;

	@AfterClass
	public static void shutdown() {
		ioManager.shutdown();
	}

	@Before
	public void setUp() throws IOException {
		writer = new AsynchronousBufferFileWriter(ioManager.createChannel(), new RequestQueue<WriteRequest>());
	}

	@Test
	public void testAddAndHandleRequest() throws Exception {
		addRequest();
		assertEquals("Didn't increment number of outstanding requests.", 1, writer.getNumberOfOutstandingRequests());

		handleRequest();
		assertEquals("Didn't decrement number of outstanding requests.", 0, writer.getNumberOfOutstandingRequests());
	}

	@Test
	public void testAddWithFailingWriter() throws Exception {
		AsynchronousBufferFileWriter writer =
			new AsynchronousBufferFileWriter(ioManager.createChannel(), new RequestQueue<>());
		writer.close();

		exception.expect(IOException.class);

		Buffer buffer = new Buffer(MemorySegmentFactory.allocateUnpooledSegment(4096),
			FreeingBufferRecycler.INSTANCE);
		try {
			writer.writeBlock(buffer);
		} finally {
			if (!buffer.isRecycled()) {
				buffer.recycle();
				Assert.fail("buffer not recycled");
			}
			assertEquals("Shouln't increment number of outstanding requests.", 0, writer.getNumberOfOutstandingRequests());
		}
	}

	@Test
	public void testSubscribe() throws Exception {
		final TestNotificationListener listener = new TestNotificationListener();

		// Unsuccessful subscription, because no outstanding requests
		assertFalse("Allowed to subscribe w/o any outstanding requests.", writer.registerAllRequestsProcessedListener(listener));

		// Successful subscription
		addRequest();
		assertTrue("Didn't allow to subscribe.", writer.registerAllRequestsProcessedListener(listener));

		// Test notification
		handleRequest();

		assertEquals("Listener was not notified.", 1, listener.getNumberOfNotifications());
	}

	@Test
	public void testSubscribeAndClose() throws IOException, InterruptedException {
		final TestNotificationListener listener = new TestNotificationListener();

		final AtomicReference<Throwable> error = new AtomicReference<Throwable>();

		final CountDownLatch sync = new CountDownLatch(1);

		addRequest();
		addRequest();

		writer.registerAllRequestsProcessedListener(listener);

		final Thread asyncCloseThread = new Thread(new Runnable() {
			@Override
			public void run() {
				try {
					writer.close();
				}
				catch (Throwable t) {
					error.set(t);
				}
				finally {
					sync.countDown();
				}
			}
		});

		asyncCloseThread.start();

		handleRequest();
		handleRequest();

		sync.await();

		assertEquals("Listener was not notified.", 1, listener.getNumberOfNotifications());
	}

	@Test
	public void testConcurrentSubscribeAndHandleRequest() throws Exception {
		final ExecutorService executor = Executors.newFixedThreadPool(2);

		final TestNotificationListener listener = new TestNotificationListener();

		final Callable<Boolean> subscriber = new Callable<Boolean>() {
			@Override
			public Boolean call() throws Exception {
				return writer.registerAllRequestsProcessedListener(listener);
			}
		};

		final Callable<Void> requestHandler = new Callable<Void>() {
			@Override
			public Void call() throws Exception {
				handleRequest();
				return null;
			}
		};

		try {
			// Repeat this to provoke races
			for (int i = 0; i < 50000; i++) {
				listener.reset();

				addRequest();

				Future<Void> handleRequestFuture = executor.submit(requestHandler);
				Future<Boolean> subscribeFuture = executor.submit(subscriber);

				handleRequestFuture.get();

				try {
					if (subscribeFuture.get()) {
						assertEquals("Race: Successfully subscribed, but was never notified.", 1, listener.getNumberOfNotifications());
					}
					else {
						assertEquals("Race: Never subscribed successfully, but was notified.", 0, listener.getNumberOfNotifications());
					}
				}
				catch (Throwable t) {
					System.out.println(i);
					Assert.fail(t.getMessage());
				}
			}
		}
		finally {
			executor.shutdownNow();
		}
	}

	// ------------------------------------------------------------------------

	private void addRequest() throws IOException {
		writer.writeBlock(mockBuffer);
	}

	private void handleRequest() {
		writer.handleProcessedBuffer(mockBuffer, null);
	}
}
