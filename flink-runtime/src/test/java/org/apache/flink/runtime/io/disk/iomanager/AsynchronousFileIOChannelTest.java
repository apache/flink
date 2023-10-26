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

import org.apache.flink.core.memory.MemorySegment;
import org.apache.flink.core.memory.MemorySegmentFactory;
import org.apache.flink.runtime.io.network.buffer.Buffer;
import org.apache.flink.runtime.io.network.buffer.BufferBuilderTestUtils;
import org.apache.flink.runtime.io.network.util.TestNotificationListener;
import org.apache.flink.testutils.executor.TestExecutorExtension;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.RegisterExtension;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.Collections;
import java.util.LinkedList;
import java.util.List;
import java.util.Random;
import java.util.concurrent.Callable;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

class AsynchronousFileIOChannelTest {

    private static final Logger LOG = LoggerFactory.getLogger(AsynchronousFileIOChannelTest.class);

    @RegisterExtension
    private static final TestExecutorExtension<ExecutorService> EXECUTOR_EXTENSION =
            new TestExecutorExtension<>(Executors::newCachedThreadPool);

    @Test
    void testAllRequestsProcessedListenerNotification() throws Exception {
        // -- Config ----------------------------------------------------------
        final int numberOfRuns = 10;
        final int numberOfRequests = 100;

        // -- Setup -----------------------------------------------------------

        final Random random = new Random();

        final RequestQueue<WriteRequest> requestQueue = new RequestQueue<>();

        final RequestDoneCallback<Buffer> ioChannelCallback = new NoOpCallback<>();

        final TestNotificationListener listener = new TestNotificationListener();

        // -- The Test --------------------------------------------------------
        try (final IOManagerAsync ioManager = new IOManagerAsync()) {
            // Repeatedly add requests and process them and have one thread try to register as a
            // listener until the channel is closed and all requests are processed.

            for (int run = 0; run < numberOfRuns; run++) {
                final TestAsyncFileIOChannel ioChannel =
                        new TestAsyncFileIOChannel(
                                ioManager.createChannel(), requestQueue, ioChannelCallback, true);

                final CountDownLatch sync = new CountDownLatch(3);

                // The mock requests
                final Buffer buffer = BufferBuilderTestUtils.buildSomeBuffer();
                final WriteRequest request = new NoOpWriteRequest();

                // Add requests task
                Callable<Void> addRequestsTask =
                        () -> {
                            for (int i = 0; i < numberOfRuns; i++) {
                                LOG.debug("Starting run {}.", i + 1);

                                for (int j = 0; j < numberOfRequests; j++) {
                                    ioChannel.addRequest(request);
                                }

                                LOG.debug(
                                        "Added all ({}) requests of run {}.",
                                        numberOfRequests,
                                        i + 1);

                                int sleep = random.nextInt(10);
                                LOG.debug("Sleeping for {} ms before next run.", sleep);

                                Thread.sleep(sleep);
                            }

                            LOG.debug("Done. Closing channel.");
                            ioChannel.close();

                            sync.countDown();

                            return null;
                        };

                // Process requests task
                Callable<Void> processRequestsTask =
                        () -> {
                            int total = numberOfRequests * numberOfRuns;
                            for (int i = 0; i < total; i++) {
                                requestQueue.take();

                                ioChannel.handleProcessedBuffer(buffer, null);
                            }

                            LOG.debug("Processed all ({}) requests.", numberOfRequests);

                            sync.countDown();

                            return null;
                        };

                // Listener
                Callable<Void> registerListenerTask =
                        () -> {
                            while (true) {
                                int current = listener.getNumberOfNotifications();

                                if (ioChannel.registerAllRequestsProcessedListener(listener)) {
                                    listener.waitForNotification(current);
                                } else if (ioChannel.isClosed()) {
                                    break;
                                }
                            }

                            LOG.debug("Stopping listener. Channel closed.");

                            sync.countDown();

                            return null;
                        };

                // Run tasks in random order
                final List<Callable<?>> tasks = new LinkedList<>();
                tasks.add(addRequestsTask);
                tasks.add(processRequestsTask);
                tasks.add(registerListenerTask);

                Collections.shuffle(tasks);

                for (Callable<?> task : tasks) {
                    EXECUTOR_EXTENSION.getExecutor().submit(task);
                }

                assertThat(sync.await(2, TimeUnit.MINUTES))
                        .withFailMessage(
                                "Test failed due to a timeout. This indicates a deadlock due to the way"
                                        + "that listeners are registered/notified in the asynchronous file I/O"
                                        + "channel.")
                        .isTrue();

                listener.reset();
            }
        }
    }

    @Test
    void testClosedButAddRequestAndRegisterListenerRace() throws Exception {
        // -- Config ----------------------------------------------------------
        final int numberOfRuns = 1024;

        // -- Setup -----------------------------------------------------------

        final RequestQueue<WriteRequest> requestQueue = new RequestQueue<>();

        @SuppressWarnings("unchecked")
        final RequestDoneCallback<Buffer> ioChannelCallback = new NoOpCallback<>();

        final TestNotificationListener listener = new TestNotificationListener();

        // -- The Test --------------------------------------------------------
        try (final IOManagerAsync ioManager = new IOManagerAsync()) {
            // Repeatedly close the channel and add a request.
            for (int i = 0; i < numberOfRuns; i++) {
                final TestAsyncFileIOChannel ioChannel =
                        new TestAsyncFileIOChannel(
                                ioManager.createChannel(), requestQueue, ioChannelCallback, true);

                final CountDownLatch sync = new CountDownLatch(2);

                final WriteRequest request = new NoOpWriteRequest();

                ioChannel.close();

                // Add request task
                Callable<Void> addRequestTask =
                        () -> {
                            try {
                                ioChannel.addRequest(request);
                            } catch (Throwable expected) {
                            } finally {
                                sync.countDown();
                            }

                            return null;
                        };

                // Listener
                Callable<Void> registerListenerTask =
                        () -> {
                            try {
                                while (true) {
                                    int current = listener.getNumberOfNotifications();

                                    if (ioChannel.registerAllRequestsProcessedListener(listener)) {
                                        listener.waitForNotification(current);
                                    } else if (ioChannel.isClosed()) {
                                        break;
                                    }
                                }
                            } finally {
                                sync.countDown();
                            }

                            return null;
                        };

                ExecutorService executor = EXECUTOR_EXTENSION.getExecutor();
                executor.submit(addRequestTask);
                executor.submit(registerListenerTask);

                assertThat(sync.await(2, TimeUnit.MINUTES))
                        .withFailMessage(
                                "Test failed due to a timeout. This indicates a deadlock due to the way"
                                        + "that listeners are registered/notified in the asynchronous file I/O"
                                        + "channel.")
                        .isTrue();
            }
        }
    }

    @Test
    void testClosingWaits() throws Exception {
        try (final IOManagerAsync ioMan = new IOManagerAsync()) {

            final int NUM_BLOCKS = 100;
            final MemorySegment seg = MemorySegmentFactory.allocateUnpooledSegment(32 * 1024);

            final AtomicInteger callbackCounter = new AtomicInteger();
            final AtomicBoolean exceptionOccurred = new AtomicBoolean();

            final RequestDoneCallback<MemorySegment> callback =
                    new RequestDoneCallback<MemorySegment>() {

                        @Override
                        public void requestSuccessful(MemorySegment buffer) {
                            // we do the non safe variant. the callbacks should come in order from
                            // the same thread, so it should always work
                            callbackCounter.set(callbackCounter.get() + 1);

                            if (buffer != seg) {
                                exceptionOccurred.set(true);
                            }
                        }

                        @Override
                        public void requestFailed(MemorySegment buffer, IOException e) {
                            exceptionOccurred.set(true);
                        }
                    };

            BlockChannelWriterWithCallback<MemorySegment> writer =
                    ioMan.createBlockChannelWriter(ioMan.createChannel(), callback);
            try {
                for (int i = 0; i < NUM_BLOCKS; i++) {
                    writer.writeBlock(seg);
                }

                writer.close();

                assertThat(callbackCounter).hasValue(NUM_BLOCKS);
                assertThat(exceptionOccurred).isFalse();
            } finally {
                writer.closeAndDelete();
            }
        }
    }

    @Test
    void testExceptionForwardsToClose() throws Exception {
        try (IOManagerAsync ioMan = new IOManagerAsync()) {
            testExceptionForwardsToClose(ioMan, 100, 1);
            testExceptionForwardsToClose(ioMan, 100, 50);
            testExceptionForwardsToClose(ioMan, 100, 100);
        }
    }

    private void testExceptionForwardsToClose(
            IOManagerAsync ioMan, final int numBlocks, final int failingBlock) throws IOException {
        MemorySegment seg = MemorySegmentFactory.allocateUnpooledSegment(32 * 1024);
        FileIOChannel.ID channelId = ioMan.createChannel();

        BlockChannelWriterWithCallback<MemorySegment> writer =
                new AsynchronousBlockWriterWithCallback(
                        channelId, ioMan.getWriteRequestQueue(channelId), new NoOpCallback<>()) {

                    private int numBlocks;

                    @Override
                    public void writeBlock(MemorySegment segment) throws IOException {
                        numBlocks++;

                        if (numBlocks == failingBlock) {
                            this.requestsNotReturned.incrementAndGet();
                            this.requestQueue.add(new FailingWriteRequest(this, segment));
                        } else {
                            super.writeBlock(segment);
                        }
                    }
                };

        assertThatThrownBy(
                        () -> {
                            try {
                                for (int i = 0; i < numBlocks; i++) {
                                    writer.writeBlock(seg);
                                }

                                writer.close();
                            } finally {
                                writer.closeAndDelete();
                            }
                        })
                .isInstanceOf(IOException.class);
    }

    private static class NoOpCallback<T> implements RequestDoneCallback<T> {

        @Override
        public void requestSuccessful(T buffer) {}

        @Override
        public void requestFailed(T buffer, IOException e) {}
    }

    private static class NoOpWriteRequest implements WriteRequest {
        @Override
        public void requestDone(IOException ioex) {}

        @Override
        public void write() {}
    }

    private static class FailingWriteRequest implements WriteRequest {

        private final AsynchronousFileIOChannel<MemorySegment, WriteRequest> channel;

        private final MemorySegment segment;

        protected FailingWriteRequest(
                AsynchronousFileIOChannel<MemorySegment, WriteRequest> targetChannel,
                MemorySegment segment) {
            this.channel = targetChannel;
            this.segment = segment;
        }

        @Override
        public void write() throws IOException {
            throw new IOException();
        }

        @Override
        public void requestDone(IOException ioex) {
            this.channel.handleProcessedBuffer(this.segment, ioex);
        }
    }

    private static class TestAsyncFileIOChannel
            extends AsynchronousFileIOChannel<Buffer, WriteRequest> {

        protected TestAsyncFileIOChannel(
                ID channelID,
                RequestQueue<WriteRequest> requestQueue,
                RequestDoneCallback<Buffer> callback,
                boolean writeEnabled)
                throws IOException {

            super(channelID, requestQueue, callback, writeEnabled);
        }

        int getNumberOfOutstandingRequests() {
            return requestsNotReturned.get();
        }
    }
}
