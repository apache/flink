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

import org.apache.flink.util.AbstractCloseableRegistry;

import org.junit.Assert;
import org.junit.Test;

import java.io.Closeable;
import java.io.IOException;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

/** Tests for the {@link AbstractCloseableRegistry}. */
public abstract class AbstractCloseableRegistryTest<C extends Closeable, T> {

    private static final int TEST_TIMEOUT_SECONDS = 10;

    protected ProducerThread[] streamOpenThreads;
    protected AbstractCloseableRegistry<C, T> closeableRegistry;
    protected AtomicInteger unclosedCounter;

    protected abstract void registerCloseable(Closeable closeable) throws IOException;

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
            streamOpenThreads[i] =
                    createProducerThread(closeableRegistry, unclosedCounter, maxStreams);
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

        assertEquals(0, unclosedCounter.get());
        assertEquals(0, closeableRegistry.getNumberOfRegisteredCloseables());

        final TestCloseable testCloseable = new TestCloseable();

        try {
            registerCloseable(testCloseable);
            fail("Closed registry should not accept closeables!");
        } catch (IOException expected) {
        }

        assertTrue(testCloseable.isClosed());
        assertEquals(0, unclosedCounter.get());
        assertEquals(0, closeableRegistry.getNumberOfRegisteredCloseables());
    }

    @Test
    public void testNonBlockingClose() throws Exception {
        setup(Integer.MAX_VALUE);

        final BlockingTestCloseable blockingCloseable = new BlockingTestCloseable();
        registerCloseable(blockingCloseable);

        assertEquals(1, closeableRegistry.getNumberOfRegisteredCloseables());

        Thread closer =
                new Thread(
                        () -> {
                            try {
                                closeableRegistry.close();
                            } catch (IOException ignore) {

                            }
                        });
        closer.start();
        blockingCloseable.awaitClose(TEST_TIMEOUT_SECONDS, TimeUnit.SECONDS);

        final TestCloseable testCloseable = new TestCloseable();
        try {
            registerCloseable(testCloseable);
            fail("Closed registry should not accept closeables!");
        } catch (IOException ignored) {
        }

        blockingCloseable.unblockClose();
        closer.join();

        assertTrue(testCloseable.isClosed());
        assertEquals(0, closeableRegistry.getNumberOfRegisteredCloseables());
    }

    /** A testing producer. */
    protected abstract static class ProducerThread<C extends Closeable, T> extends Thread {

        protected final AbstractCloseableRegistry<C, T> registry;
        protected final AtomicInteger refCount;
        protected final int maxStreams;
        protected int numStreams;

        public ProducerThread(
                AbstractCloseableRegistry<C, T> registry, AtomicInteger refCount, int maxStreams) {
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
                        Thread.sleep(0);
                    } catch (InterruptedException ignored) {
                    }

                    if (maxStreams != Integer.MAX_VALUE) {
                        ++numStreams;
                    }
                }
            } catch (Exception ex) {
                // ignored
            }
        }
    }

    /** Testing stream which adds itself to a reference counter while not closed. */
    protected static final class TestStream extends FSDataInputStream {

        protected AtomicInteger refCount;

        public TestStream(AtomicInteger refCount) {
            this.refCount = refCount;
            refCount.incrementAndGet();
        }

        @Override
        public void seek(long desired) throws IOException {}

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

    /** A noop {@link Closeable} implementation that blocks inside {@link #close()}. */
    private static class BlockingTestCloseable implements Closeable {

        private final CountDownLatch closeCalledLatch = new CountDownLatch(1);

        private final CountDownLatch blockCloseLatch = new CountDownLatch(1);

        @Override
        public void close() throws IOException {
            closeCalledLatch.countDown();
            try {
                blockCloseLatch.await();
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
            }
        }

        /** Unblocks {@link #close()}. */
        public void unblockClose() {
            blockCloseLatch.countDown();
        }

        /** Causes the current thread to wait until {@link #close()} is called. */
        public void awaitClose(final long timeout, final TimeUnit timeUnit)
                throws InterruptedException {
            assertTrue(closeCalledLatch.await(timeout, timeUnit));
        }
    }

    /** A noop {@link Closeable} implementation that tracks whether it was closed. */
    private static class TestCloseable implements Closeable {

        private final AtomicBoolean closed = new AtomicBoolean();

        @Override
        public void close() throws IOException {
            assertTrue("TestCloseable was already closed", closed.compareAndSet(false, true));
        }

        public boolean isClosed() {
            return closed.get();
        }
    }
}
