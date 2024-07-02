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

package org.apache.flink.connector.base.sink.writer;

import org.apache.flink.api.connector.sink2.WriterInitContext;
import org.apache.flink.connector.base.sink.writer.config.AsyncSinkWriterConfiguration;
import org.apache.flink.streaming.runtime.tasks.TestProcessingTimeService;
import org.apache.flink.util.FlinkRuntimeException;

import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeoutException;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatExceptionOfType;
import static org.junit.jupiter.api.Assertions.fail;

/** Test for timeout functionalities of {@link AsyncSinkWriter}. */
public class AsyncSinkWriterTimeoutTest {

    private final ExecutorService executorService = Executors.newFixedThreadPool(5);
    private final List<Long> destination = new ArrayList<>();

    @Test
    void writerShouldNotRetryIfRequestIsProcessedBeforeTimeout() throws Exception {
        TestSinkInitContextAnyThreadMailbox context = new TestSinkInitContextAnyThreadMailbox();
        TestProcessingTimeService tpts = context.getTestProcessingTimeService();
        TimeoutWriter writer = new TimeoutWriter(context, 1, 10, 100, false);
        tpts.setCurrentTime(0);
        writer.write("1", null);
        tpts.setCurrentTime(10);
        writer.deliverMessage();
        tpts.setCurrentTime(120);
        writer.flush(false);
        assertThat(destination).containsExactly(1L);
    }

    @Test
    void writerShouldRetryOnTimeoutIfFailOnErrorIsFalse() throws Exception {
        TestSinkInitContextAnyThreadMailbox context = new TestSinkInitContextAnyThreadMailbox();
        TestProcessingTimeService tpts = context.getTestProcessingTimeService();
        TimeoutWriter writer = new TimeoutWriter(context, 1, 10, 100, false);
        tpts.setCurrentTime(0);
        writer.write("1", null);
        // element should be requeued back after timeout
        tpts.setCurrentTime(110);
        // deliver initial message after timeout
        writer.deliverMessage();
        // flush outstanding mailbox messages containing resubmission of the element
        writer.flush(false);
        assertThat(destination).containsExactly(1L, 1L);
    }

    @Test
    void writerShouldFailOnTimeoutIfFailOnErrorIsTrue() throws Exception {
        TestSinkInitContextAnyThreadMailbox context = new TestSinkInitContextAnyThreadMailbox();
        TestProcessingTimeService tpts = context.getTestProcessingTimeService();
        TimeoutWriter writer = new TimeoutWriter(context, 1, 10, 100, true);
        tpts.setCurrentTime(0);
        writer.write("1", null);
        tpts.setCurrentTime(110);
        writer.deliverMessage();
        assertThatExceptionOfType(FlinkRuntimeException.class)
                .isThrownBy(() -> writer.flush(false))
                .withCauseInstanceOf(TimeoutException.class)
                .havingCause()
                .withMessageContaining(
                        "Request timed out after 100ms with failOnTimeout set to true.");
    }

    @Test
    void writerShouldDiscardRetriedEntriesOnTimeout() throws Exception {
        TestSinkInitContextAnyThreadMailbox context = new TestSinkInitContextAnyThreadMailbox();
        TestProcessingTimeService tpts = context.getTestProcessingTimeService();
        TimeoutWriter writer = new TimeoutWriter(context, 1, 10, 100, false);
        writer.setShouldFailRequest(true);
        tpts.setCurrentTime(0);
        writer.write("1", null);
        tpts.setCurrentTime(110);
        // deliver initial message after timeout, message request would fail but elements should be
        // discarded as timeout already occurred
        writer.deliverMessage();
        // flush outstanding mailbox messages to resubmit timed out elements
        writer.flush(false);
        assertThat(destination).containsExactly(1L);
    }

    @Test
    void writerShouldFailOnFatalError() throws Exception {
        TestSinkInitContextAnyThreadMailbox context = new TestSinkInitContextAnyThreadMailbox();
        TestProcessingTimeService tpts = context.getTestProcessingTimeService();
        TimeoutWriter writer = new TimeoutWriter(context, 1, 10, 100, true);
        tpts.setCurrentTime(0);
        writer.setFatalError(new FlinkRuntimeException("Fatal error"));
        writer.write("1", null);
        writer.deliverMessage();
        assertThatExceptionOfType(FlinkRuntimeException.class)
                .isThrownBy(() -> writer.flush(false))
                .withMessage("Fatal error");
    }

    /** Test that the writer can handle a timeout. */
    private class TimeoutWriter extends AsyncSinkWriter<String, Long> {
        private Exception fatalError;
        private final CountDownLatch completionLatch;
        private Future<?> submitFuture;

        private boolean shouldFailRequest = false;

        public TimeoutWriter(
                WriterInitContext writerInitContext,
                int maxBatchSize,
                long maximumTimeInBufferMs,
                long requestTimeout,
                boolean failOnTimeout) {
            super(
                    (ElementConverter<String, Long>) (element, context) -> Long.parseLong(element),
                    writerInitContext,
                    AsyncSinkWriterConfiguration.builder()
                            .setMaxBatchSize(maxBatchSize)
                            .setMaxBatchSizeInBytes(Long.MAX_VALUE)
                            .setMaxInFlightRequests(Integer.MAX_VALUE)
                            .setMaxBufferedRequests(Integer.MAX_VALUE)
                            .setMaxTimeInBufferMS(maximumTimeInBufferMs)
                            .setMaxRecordSizeInBytes(Long.MAX_VALUE)
                            .setRequestTimeoutMS(requestTimeout)
                            .setFailOnTimeout(failOnTimeout)
                            .build(),
                    Collections.emptyList());
            this.completionLatch = new CountDownLatch(1);
        }

        @Override
        protected void submitRequestEntries(
                List<Long> requestEntries, ResultHandler<Long> resultHandler) {
            submitFuture =
                    executorService.submit(
                            () -> {
                                while (completionLatch.getCount() > 0) {
                                    try {
                                        completionLatch.await();
                                    } catch (InterruptedException e) {
                                        fail("Submission thread must not be interrupted.");
                                    }
                                }
                                submitRequestEntriesSync(requestEntries, resultHandler);
                            });
        }

        private void submitRequestEntriesSync(
                List<Long> requestEntries, ResultHandler<Long> resultHandler) {
            if (fatalError != null) {
                resultHandler.completeExceptionally(fatalError);
            } else if (shouldFailRequest) {
                shouldFailRequest = false;
                resultHandler.retryForEntries(requestEntries);
            } else {
                destination.addAll(requestEntries);
                resultHandler.complete();
            }
        }

        @Override
        protected long getSizeInBytes(Long requestEntry) {
            return 8;
        }

        public void setFatalError(Exception fatalError) {
            this.fatalError = fatalError;
        }

        public void setShouldFailRequest(boolean shouldFailRequest) {
            this.shouldFailRequest = shouldFailRequest;
        }

        public void deliverMessage() throws InterruptedException, ExecutionException {
            completionLatch.countDown();
            submitFuture.get();
        }
    }
}
