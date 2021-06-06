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

package org.apache.flink.streaming.connectors.dynamodb.batch;

import org.apache.flink.streaming.connectors.dynamodb.ProducerException;
import org.apache.flink.streaming.connectors.dynamodb.ProducerWriteRequest;
import org.apache.flink.streaming.connectors.dynamodb.ProducerWriteResponse;

import org.apache.flink.shaded.guava30.com.google.common.util.concurrent.ThreadFactoryBuilder;

import org.junit.Test;

import java.util.Collections;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.atomic.AtomicInteger;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;

/** Unit tests for {@link BatchAsyncProcessor}. */
public class BatchAsyncProcessorTest {

    private ExecutorService createExecutorService() {
        return Executors.newCachedThreadPool(
                (new ThreadFactoryBuilder())
                        .setDaemon(true)
                        .setNameFormat("dynamo-daemon-%04d")
                        .build());
    }

    private static class CountingCompletionHandler
            implements BatchAsyncProcessor.CompletionHandler {
        private final AtomicInteger completionCounter = new AtomicInteger(0);
        private final AtomicInteger exceptionCounter = new AtomicInteger(0);

        @Override
        public void onCompletion(ProducerWriteResponse response) {
            completionCounter.incrementAndGet();
        }

        @Override
        public void onException(Throwable error) {
            exceptionCounter.incrementAndGet();
        }

        public int completionInvocations() {
            return completionCounter.get();
        }

        public int exceptionInvocations() {
            return exceptionCounter.get();
        }
    }

    private static class MockWriterProvider extends BatchWriterProvider {

        private final Callable<ProducerWriteResponse> mockWriter;

        public MockWriterProvider(Callable<ProducerWriteResponse> writer) {
            super(null, null, null);
            this.mockWriter = writer;
        }

        @Override
        public Callable<ProducerWriteResponse> createWriter(ProducerWriteRequest request) {
            return mockWriter;
        }
    }

    private ProducerWriteRequest createMockRequest() {
        return new ProducerWriteRequest<>("id", "table", Collections.EMPTY_LIST);
    }

    private void waitUntilAllProcessed(BatchAsyncProcessor processor) {
        try {
            while (processor.getOutstandingRecordsCount() > 0) {
                Thread.sleep(1000);
            }
        } catch (InterruptedException e) {
            processor.shutdown();
        }
    }

    @Test(expected = ProducerException.class)
    public void testDoesNotAcceptNewMessagesOnShutdown() {
        BatchAsyncProcessor processor =
                new BatchAsyncProcessor(
                        3,
                        createExecutorService(),
                        new MockWriterProvider(null),
                        new CountingCompletionHandler());

        processor.shutdown();
        processor.accept(createMockRequest());
    }

    @Test(expected = ProducerException.class)
    public void testDoesNotAcceptMessagesWhenNotStarted() {
        BatchAsyncProcessor processor =
                new BatchAsyncProcessor(
                        3,
                        createExecutorService(),
                        new MockWriterProvider(null),
                        new CountingCompletionHandler());

        processor.accept(createMockRequest());
    }

    @Test
    public void testOnCompletionHandlerIsCalled() {
        CountingCompletionHandler handler = new CountingCompletionHandler();

        Callable<ProducerWriteResponse> writer =
                () -> new ProducerWriteResponse("test", true, 1, null, 10L);

        BatchAsyncProcessor processor =
                new BatchAsyncProcessor(
                        10, createExecutorService(), new MockWriterProvider(writer), handler);

        processor.start();

        processor.accept(createMockRequest());
        processor.accept(createMockRequest());
        processor.accept(createMockRequest());

        waitUntilAllProcessed(processor);

        assertEquals(
                "total number of times completion handler is called",
                3,
                handler.completionInvocations());
        assertEquals(
                "total number of times exception handler is called",
                0,
                handler.exceptionInvocations());

        processor.shutdown();
    }

    @Test
    public void testOnExceptionHandlerIsCalled() throws Exception {
        CountingCompletionHandler handler = new CountingCompletionHandler();

        Callable<ProducerWriteResponse> writer =
                () -> {
                    throw new Exception("Writer Exception");
                };

        BatchAsyncProcessor processor =
                new BatchAsyncProcessor(
                        10, createExecutorService(), new MockWriterProvider(writer), handler);

        processor.start();

        processor.accept(createMockRequest());
        processor.accept(createMockRequest());

        waitUntilAllProcessed(processor);

        assertEquals(
                "total number of times completion handler is called",
                0,
                handler.completionInvocations());
        assertEquals(
                "total number of times exception handler is called",
                2,
                handler.exceptionInvocations());

        processor.shutdown();
    }

    @Test
    public void testShutdownGracefully() {
        Callable<ProducerWriteResponse> sleepingWriter =
                () -> {
                    Thread.sleep(1000);
                    return new ProducerWriteResponse("test", true, 1, null, 10L);
                };

        CountingCompletionHandler handler = new CountingCompletionHandler();

        BatchAsyncProcessor processor =
                new BatchAsyncProcessor(
                        6,
                        createExecutorService(),
                        new MockWriterProvider(sleepingWriter),
                        handler);

        processor.start();

        processor.accept(createMockRequest());
        processor.accept(createMockRequest());
        processor.accept(createMockRequest());
        processor.accept(createMockRequest());
        processor.accept(createMockRequest());
        processor.accept(createMockRequest());

        processor.shutdown();

        assertFalse("processor is not started", processor.isStarted());
        assertEquals(
                "total number of times completion handler is called",
                6,
                handler.completionInvocations());
    }
}
