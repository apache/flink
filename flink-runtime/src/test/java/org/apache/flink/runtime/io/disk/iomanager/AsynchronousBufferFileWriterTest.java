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
import org.apache.flink.runtime.io.network.buffer.NetworkBuffer;
import org.apache.flink.runtime.io.network.util.TestNotificationListener;

import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.util.concurrent.Callable;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.atomic.AtomicReference;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.assertj.core.api.Assertions.fail;
import static org.mockito.Mockito.mock;

/** Tests for {@link AsynchronousBufferFileWriter}. */
class AsynchronousBufferFileWriterTest {

    private static final IOManager ioManager = new IOManagerAsync();

    private static final Buffer mockBuffer = mock(Buffer.class);

    private AsynchronousBufferFileWriter writer;

    @AfterAll
    static void shutdown() throws Exception {
        ioManager.close();
    }

    @BeforeEach
    void setUp() throws IOException {
        writer =
                new AsynchronousBufferFileWriter(
                        ioManager.createChannel(), new RequestQueue<WriteRequest>());
    }

    @Test
    void testAddAndHandleRequest() throws Exception {
        addRequest();
        assertThat(writer.getNumberOfOutstandingRequests())
                .withFailMessage("Didn't increment number of outstanding requests.")
                .isOne();

        handleRequest();
        assertThat(writer.getNumberOfOutstandingRequests())
                .withFailMessage("Didn't decrement number of outstanding requests.")
                .isZero();
    }

    @Test
    void testAddWithFailingWriter() throws Exception {
        AsynchronousBufferFileWriter writer =
                new AsynchronousBufferFileWriter(ioManager.createChannel(), new RequestQueue<>());
        writer.close();

        Buffer buffer =
                new NetworkBuffer(
                        MemorySegmentFactory.allocateUnpooledSegment(4096),
                        FreeingBufferRecycler.INSTANCE);

        assertThatThrownBy(() -> writer.writeBlock(buffer)).isInstanceOf(IOException.class);

        if (!buffer.isRecycled()) {
            buffer.recycleBuffer();
            fail("buffer not recycled");
        }
        assertThat(writer.getNumberOfOutstandingRequests())
                .withFailMessage("Shouldn't increment number of outstanding requests.")
                .isZero();
    }

    @Test
    void testSubscribe() throws Exception {
        final TestNotificationListener listener = new TestNotificationListener();

        // Unsuccessful subscription, because no outstanding requests
        assertThat(writer.registerAllRequestsProcessedListener(listener))
                .withFailMessage("Allowed to subscribe w/o any outstanding requests.")
                .isFalse();

        // Successful subscription
        addRequest();
        assertThat(writer.registerAllRequestsProcessedListener(listener))
                .withFailMessage("Didn't allow to subscribe.")
                .isTrue();

        // Test notification
        handleRequest();

        assertThat(listener.getNumberOfNotifications())
                .withFailMessage("Listener was not notified.")
                .isOne();
    }

    @Test
    void testSubscribeAndClose() throws IOException, InterruptedException {
        final TestNotificationListener listener = new TestNotificationListener();

        final AtomicReference<Throwable> error = new AtomicReference<Throwable>();

        final CountDownLatch sync = new CountDownLatch(1);

        addRequest();
        addRequest();

        writer.registerAllRequestsProcessedListener(listener);

        final Thread asyncCloseThread =
                new Thread(
                        new Runnable() {
                            @Override
                            public void run() {
                                try {
                                    writer.close();
                                } catch (Throwable t) {
                                    error.set(t);
                                } finally {
                                    sync.countDown();
                                }
                            }
                        });

        asyncCloseThread.start();

        handleRequest();
        handleRequest();

        sync.await();

        assertThat(listener.getNumberOfNotifications())
                .withFailMessage("Listener was not notified.")
                .isOne();
    }

    @Test
    void testConcurrentSubscribeAndHandleRequest() throws Exception {
        final ExecutorService executor = Executors.newFixedThreadPool(2);

        final TestNotificationListener listener = new TestNotificationListener();

        final Callable<Boolean> subscriber =
                new Callable<Boolean>() {
                    @Override
                    public Boolean call() throws Exception {
                        return writer.registerAllRequestsProcessedListener(listener);
                    }
                };

        final Callable<Void> requestHandler =
                new Callable<Void>() {
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
                        assertThat(listener.getNumberOfNotifications())
                                .withFailMessage(
                                        "Race: Successfully subscribed, but was never notified.")
                                .isOne();
                    } else {
                        assertThat(listener.getNumberOfNotifications())
                                .withFailMessage(
                                        "Race: Never subscribed successfully, but was notified.")
                                .isZero();
                    }
                } catch (Throwable t) {
                    System.out.println(i);
                    fail(t.getMessage());
                }
            }
        } finally {
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
