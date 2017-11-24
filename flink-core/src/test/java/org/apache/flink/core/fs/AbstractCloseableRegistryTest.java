/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.flink.core.fs;

import org.apache.flink.core.testutils.OneShotLatch;
import org.apache.flink.util.AbstractCloseableRegistry;

import org.junit.Assert;
import org.junit.Test;

import java.io.Closeable;
import java.io.IOException;
import java.util.concurrent.atomic.AtomicInteger;

import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.verify;
import static org.powermock.api.mockito.PowerMockito.spy;

/**
 * Tests for the {@link AbstractCloseableRegistry}.
 */
public abstract class AbstractCloseableRegistryTest<C extends Closeable, T> {

	protected ProducerThread[] streamOpenThreads;
	protected AbstractCloseableRegistry<C, T> closeableRegistry;
	protected AtomicInteger unclosedCounter;

	protected abstract C createCloseable();

	protected abstract AbstractCloseableRegistry<C, T> createRegistry();

	protected abstract ProducerThread<C, T> createProducerThread(
		AbstractCloseableRegistry<C, T> registry,
		AtomicInteger unclosedCounter,
		int maxStreams);

	public void setup(int maxStreams) {
		Assert.assertFalse(SafetyNetCloseableRegistry.isReaperThreadRunning());
		this.closeableRegistry = createRegistry();
		this.unclosedCounter = new AtomicInteger(0);
		this.streamOpenThreads = new ProducerThread[10];
		for (int i = 0; i < streamOpenThreads.length; ++i) {
			streamOpenThreads[i] = createProducerThread(closeableRegistry, unclosedCounter, maxStreams);
		}
	}

	protected void startThreads() {
		for (ProducerThread t : streamOpenThreads) {
			t.start();
		}
	}

	protected void joinThreads() throws InterruptedException {
		for (Thread t : streamOpenThreads) {
			t.join();
		}
	}

	@Test
	public void testClose() throws Exception {

		setup(Integer.MAX_VALUE);
		startThreads();

		for (int i = 0; i < 5; ++i) {
			System.gc();
			Thread.sleep(40);
		}

		closeableRegistry.close();

		joinThreads();

		Assert.assertEquals(0, unclosedCounter.get());
		Assert.assertEquals(0, closeableRegistry.getNumberOfRegisteredCloseables());

		final C testCloseable = spy(createCloseable());

		try {

			closeableRegistry.registerCloseable(testCloseable);

			Assert.fail("Closed registry should not accept closeables!");

		} catch (IOException expected) {
			//expected
		}

		Assert.assertEquals(0, unclosedCounter.get());
		Assert.assertEquals(0, closeableRegistry.getNumberOfRegisteredCloseables());
		verify(testCloseable).close();
	}

	@Test
	public void testNonBlockingClose() throws Exception {
		setup(Integer.MAX_VALUE);

		final OneShotLatch waitRegistryClosedLatch = new OneShotLatch();
		final OneShotLatch blockCloseLatch = new OneShotLatch();

		final C spyCloseable = spy(createCloseable());

		doAnswer(invocationOnMock -> {
			invocationOnMock.callRealMethod();
			waitRegistryClosedLatch.trigger();
			blockCloseLatch.await();
			return null;
		}).when(spyCloseable).close();

		closeableRegistry.registerCloseable(spyCloseable);

		Assert.assertEquals(1, closeableRegistry.getNumberOfRegisteredCloseables());

		Thread closer = new Thread(() -> {
			try {
				closeableRegistry.close();
			} catch (IOException ignore) {

			}
		});

		closer.start();
		waitRegistryClosedLatch.await();

		final C testCloseable = spy(createCloseable());

		try {
			closeableRegistry.registerCloseable(testCloseable);
			Assert.fail("Closed registry should not accept closeables!");
		} catch (IOException ignored) {}

		blockCloseLatch.trigger();
		closer.join();

		verify(spyCloseable).close();
		verify(testCloseable).close();
		Assert.assertEquals(0, closeableRegistry.getNumberOfRegisteredCloseables());
	}

	/**
	 * A testing producer.
	 */
	protected abstract static class ProducerThread<C extends Closeable, T> extends Thread {

		protected final AbstractCloseableRegistry<C, T> registry;
		protected final AtomicInteger refCount;
		protected final int maxStreams;
		protected int numStreams;

		public ProducerThread(AbstractCloseableRegistry<C, T> registry, AtomicInteger refCount, int maxStreams) {
			this.registry = registry;
			this.refCount = refCount;
			this.maxStreams = maxStreams;
			this.numStreams = 0;
		}

		protected abstract void createAndRegisterStream() throws IOException;

		@Override
		public void run() {
			try {
				while (numStreams < maxStreams) {

					createAndRegisterStream();

					try {
						Thread.sleep(2);
					} catch (InterruptedException ignored) {}

					if (maxStreams != Integer.MAX_VALUE) {
						++numStreams;
					}
				}
			} catch (Exception ex) {
				// ignored
			}
		}
	}

	/**
	 * Testing stream which adds itself to a reference counter while not closed.
	 */
	protected static final class TestStream extends FSDataInputStream {

		protected AtomicInteger refCount;

		public TestStream(AtomicInteger refCount) {
			this.refCount = refCount;
			refCount.incrementAndGet();
		}

		@Override
		public void seek(long desired) throws IOException {

		}

		@Override
		public long getPos() throws IOException {
			return 0;
		}

		@Override
		public int read() throws IOException {
			return 0;
		}

		@Override
		public synchronized void close() throws IOException {
			refCount.decrementAndGet();
		}
	}
}
