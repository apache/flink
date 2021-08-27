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

import org.apache.flink.core.testutils.CheckedThread;
import org.apache.flink.util.AbstractCloseableRegistry;
import org.apache.flink.util.ExceptionUtils;

import org.junit.After;
import org.junit.Assert;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

import java.io.Closeable;
import java.io.IOException;
import java.util.concurrent.atomic.AtomicInteger;

/** Tests for the {@link SafetyNetCloseableRegistry}. */
public class SafetyNetCloseableRegistryTest
        extends AbstractCloseableRegistryTest<
                WrappingProxyCloseable<? extends Closeable>,
                SafetyNetCloseableRegistry.PhantomDelegatingCloseableRef> {

    @Rule public final TemporaryFolder tmpFolder = new TemporaryFolder();

    @Override
    protected void registerCloseable(final Closeable closeable) throws IOException {
        final WrappingProxyCloseable<Closeable> wrappingProxyCloseable =
                new WrappingProxyCloseable<Closeable>() {

                    @Override
                    public void close() throws IOException {
                        closeable.close();
                    }

                    @Override
                    public Closeable getWrappedDelegate() {
                        return closeable;
                    }
                };
        closeableRegistry.registerCloseable(wrappingProxyCloseable);
    }

    @Override
    protected AbstractCloseableRegistry<
                    WrappingProxyCloseable<? extends Closeable>,
                    SafetyNetCloseableRegistry.PhantomDelegatingCloseableRef>
            createRegistry() {
        // SafetyNetCloseableRegistry has a global reaper thread to reclaim leaking resources,
        // in normal cases, that thread will be interrupted in closing of last active registry
        // and then shutdown in background. But in testing codes, some assertions need leaking
        // resources reclaimed, so we override reaper thread to join itself on interrupt. Thus,
        // after close of last active registry, we can assert post-close-invariants safely.
        return new SafetyNetCloseableRegistry(JoinOnInterruptReaperThread::new);
    }

    @Override
    protected AbstractCloseableRegistryTest.ProducerThread<
                    WrappingProxyCloseable<? extends Closeable>,
                    SafetyNetCloseableRegistry.PhantomDelegatingCloseableRef>
            createProducerThread(
                    AbstractCloseableRegistry<
                                    WrappingProxyCloseable<? extends Closeable>,
                                    SafetyNetCloseableRegistry.PhantomDelegatingCloseableRef>
                            registry,
                    AtomicInteger unclosedCounter,
                    int maxStreams) {

        return new AbstractCloseableRegistryTest.ProducerThread<
                WrappingProxyCloseable<? extends Closeable>,
                SafetyNetCloseableRegistry.PhantomDelegatingCloseableRef>(
                registry, unclosedCounter, maxStreams) {

            int count = 0;

            @Override
            protected void createAndRegisterStream() throws IOException {
                String debug = Thread.currentThread().getName() + " " + count;
                TestStream testStream = new TestStream(refCount);

                // this method automatically registers the stream with the given registry.
                @SuppressWarnings("unused")
                ClosingFSDataInputStream pis =
                        ClosingFSDataInputStream.wrapSafe(
                                testStream,
                                (SafetyNetCloseableRegistry) registry,
                                debug); // reference dies here
                ++count;
            }
        };
    }

    @After
    public void tearDown() {
        Assert.assertFalse(SafetyNetCloseableRegistry.isReaperThreadRunning());
    }

    @Test
    public void testCorrectScopesForSafetyNet() throws Exception {
        CheckedThread t1 =
                new CheckedThread() {

                    @Override
                    public void go() throws Exception {
                        try {
                            FileSystem fs1 = FileSystem.getLocalFileSystem();
                            // ensure no safety net in place
                            Assert.assertFalse(fs1 instanceof SafetyNetWrapperFileSystem);
                            FileSystemSafetyNet.initializeSafetyNetForThread();
                            fs1 = FileSystem.getLocalFileSystem();
                            // ensure safety net is in place now
                            Assert.assertTrue(fs1 instanceof SafetyNetWrapperFileSystem);

                            Path tmp =
                                    new Path(tmpFolder.newFolder().toURI().toString(), "test_file");

                            try (FSDataOutputStream stream =
                                    fs1.create(tmp, FileSystem.WriteMode.NO_OVERWRITE)) {
                                CheckedThread t2 =
                                        new CheckedThread() {
                                            @Override
                                            public void go() {
                                                FileSystem fs2 = FileSystem.getLocalFileSystem();
                                                // ensure the safety net does not leak here
                                                Assert.assertFalse(
                                                        fs2 instanceof SafetyNetWrapperFileSystem);
                                                FileSystemSafetyNet.initializeSafetyNetForThread();
                                                fs2 = FileSystem.getLocalFileSystem();
                                                // ensure we can bring another safety net in place
                                                Assert.assertTrue(
                                                        fs2 instanceof SafetyNetWrapperFileSystem);
                                                FileSystemSafetyNet
                                                        .closeSafetyNetAndGuardedResourcesForThread();
                                                fs2 = FileSystem.getLocalFileSystem();
                                                // and that we can remove it again
                                                Assert.assertFalse(
                                                        fs2 instanceof SafetyNetWrapperFileSystem);
                                            }
                                        };

                                t2.start();
                                t2.sync();

                                // ensure stream is still open and was never closed by any
                                // interferences
                                stream.write(42);
                                FileSystemSafetyNet.closeSafetyNetAndGuardedResourcesForThread();

                                // ensure leaking stream was closed
                                try {
                                    stream.write(43);
                                    Assert.fail();
                                } catch (IOException ignore) {

                                }
                                fs1 = FileSystem.getLocalFileSystem();
                                // ensure safety net was removed
                                Assert.assertFalse(fs1 instanceof SafetyNetWrapperFileSystem);
                            } finally {
                                fs1.delete(tmp, false);
                            }
                        } catch (Exception e) {
                            Assert.fail(ExceptionUtils.stringifyException(e));
                        }
                    }
                };

        t1.start();
        t1.sync();
    }

    @Test
    public void testSafetyNetClose() throws Exception {
        setup(20);
        startThreads();

        joinThreads();

        for (int i = 0; i < 5 && unclosedCounter.get() > 0; ++i) {
            System.gc();
            Thread.sleep(50);
        }

        Assert.assertEquals(0, unclosedCounter.get());
        closeableRegistry.close();
    }

    @Test
    public void testReaperThreadSpawnAndStop() throws Exception {
        Assert.assertFalse(SafetyNetCloseableRegistry.isReaperThreadRunning());

        try (SafetyNetCloseableRegistry ignored = new SafetyNetCloseableRegistry()) {
            Assert.assertTrue(SafetyNetCloseableRegistry.isReaperThreadRunning());

            try (SafetyNetCloseableRegistry ignored2 = new SafetyNetCloseableRegistry()) {
                Assert.assertTrue(SafetyNetCloseableRegistry.isReaperThreadRunning());
            }
            Assert.assertTrue(SafetyNetCloseableRegistry.isReaperThreadRunning());
        }
        Assert.assertFalse(SafetyNetCloseableRegistry.isReaperThreadRunning());
    }

    /**
     * Test whether failure to start thread in {@link SafetyNetCloseableRegistry} constructor can
     * lead to failure of subsequent state check.
     */
    @Test
    public void testReaperThreadStartFailed() throws Exception {

        try {
            new SafetyNetCloseableRegistry(() -> new OutOfMemoryReaperThread());
        } catch (java.lang.OutOfMemoryError error) {
        }
        Assert.assertFalse(SafetyNetCloseableRegistry.isReaperThreadRunning());

        // the OOM error will not lead to failure of subsequent constructor call.
        SafetyNetCloseableRegistry closeableRegistry = new SafetyNetCloseableRegistry();
        Assert.assertTrue(SafetyNetCloseableRegistry.isReaperThreadRunning());

        closeableRegistry.close();
    }

    private static class JoinOnInterruptReaperThread
            extends SafetyNetCloseableRegistry.CloseableReaperThread {
        @Override
        public void interrupt() {
            super.interrupt();
            try {
                join();
            } catch (InterruptedException ex) {
                Thread.currentThread().interrupt();
            }
        }
    }

    private static class OutOfMemoryReaperThread
            extends SafetyNetCloseableRegistry.CloseableReaperThread {

        @Override
        public synchronized void start() {
            throw new java.lang.OutOfMemoryError();
        }
    }
}
