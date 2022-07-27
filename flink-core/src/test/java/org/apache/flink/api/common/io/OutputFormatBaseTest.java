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

package org.apache.flink.api.common.io;

import org.apache.flink.configuration.Configuration;
import org.apache.flink.core.testutils.CheckedThread;
import org.apache.flink.util.Preconditions;
import org.apache.flink.util.concurrent.FutureUtils;

import org.junit.jupiter.api.Test;

import javax.annotation.Nullable;

import java.io.IOException;
import java.time.Duration;
import java.util.LinkedList;
import java.util.Queue;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionStage;
import java.util.concurrent.TimeoutException;
import java.util.function.Function;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/** Tests for the {@link OutputFormatBase}. */
class OutputFormatBaseTest {

    private static final Duration DEFAULT_MAX_CONCURRENT_REQUESTS_TIMEOUT =
            Duration.ofMillis(Long.MAX_VALUE);

    @Test
    void testSuccessfulWrite() throws Exception {
        try (TestOutputFormat testOutputFormat = createOpenedTestOutputFormat()) {

            testOutputFormat.enqueueCompletableFuture(CompletableFuture.completedFuture(null));

            final int originalPermits = testOutputFormat.getAvailablePermits();
            assertThat(originalPermits).isGreaterThan(0);
            assertThat(testOutputFormat.getAcquiredPermits()).isEqualTo(0);

            testOutputFormat.writeRecord("hello");

            assertThat(testOutputFormat.getAvailablePermits()).isEqualTo(originalPermits);
            assertThat(testOutputFormat.getAcquiredPermits()).isEqualTo(0);
        }
    }

    @Test
    void testThrowErrorOnClose() throws Exception {
        TestOutputFormat testOutputFormat = createTestOutputFormat();
        testOutputFormat.open(1, 1);

        Exception cause = new RuntimeException();
        testOutputFormat.enqueueCompletableFuture(FutureUtils.completedExceptionally(cause));
        testOutputFormat.writeRecord("none");

        assertThatThrownBy(() -> testOutputFormat.close())
                .isInstanceOf(IOException.class)
                .hasCauseReference(cause);
    }

    @Test
    void testThrowErrorOnWrite() throws Exception {
        try (TestOutputFormat testOutputFormat = createOpenedTestOutputFormat()) {
            Exception cause = new RuntimeException();
            testOutputFormat.enqueueCompletableFuture(FutureUtils.completedExceptionally(cause));

            testOutputFormat.writeRecord("none");

            // should fail because the first write failed and the second will check for asynchronous
            // errors (throwable set by the async callback)
            assertThatThrownBy(
                            () -> testOutputFormat.writeRecord("none"),
                            "Sending of second value should have failed.")
                    .isInstanceOf(IOException.class)
                    .hasCauseReference(cause);
            assertThat(testOutputFormat.getAcquiredPermits()).isEqualTo(0);
        }
    }

    @Test
    void testWaitForPendingUpdatesOnClose() throws Exception {
        try (TestOutputFormat testOutputFormat = createOpenedTestOutputFormat()) {

            CompletableFuture<Void> completableFuture = new CompletableFuture<>();
            testOutputFormat.enqueueCompletableFuture(completableFuture);

            testOutputFormat.writeRecord("hello");
            assertThat(testOutputFormat.getAcquiredPermits()).isEqualTo(1);

            CheckedThread checkedThread =
                    new CheckedThread("Flink-OutputFormatBaseTest") {
                        @Override
                        public void go() throws Exception {
                            testOutputFormat.close();
                        }
                    };
            checkedThread.start();
            while (checkedThread.getState() != Thread.State.TIMED_WAITING) {
                Thread.sleep(5);
            }

            assertThat(testOutputFormat.getAcquiredPermits()).isEqualTo(1);
            // start writing
            completableFuture.complete(null);
            // wait for the close
            checkedThread.sync();
            assertThat(testOutputFormat.getAcquiredPermits()).isEqualTo(0);
        }
    }

    @Test
    void testReleaseOnSuccess() throws Exception {
        try (TestOutputFormat openedTestOutputFormat = createOpenedTestOutputFormat()) {

            assertThat(openedTestOutputFormat.getAvailablePermits()).isEqualTo(1);
            assertThat(openedTestOutputFormat.getAcquiredPermits()).isEqualTo(0);

            CompletableFuture<Void> completableFuture = new CompletableFuture<>();
            openedTestOutputFormat.enqueueCompletableFuture(completableFuture);
            openedTestOutputFormat.writeRecord("hello");

            assertThat(openedTestOutputFormat.getAvailablePermits()).isEqualTo(0);
            assertThat(openedTestOutputFormat.getAcquiredPermits()).isEqualTo(1);

            // start writing
            completableFuture.complete(null);

            assertThat(openedTestOutputFormat.getAvailablePermits()).isEqualTo(1);
            assertThat(openedTestOutputFormat.getAcquiredPermits()).isEqualTo(0);
        }
    }

    @Test
    void testReleaseOnFailure() throws Exception {
        TestOutputFormat testOutputFormat = createOpenedTestOutputFormat();

        assertThat(testOutputFormat.getAvailablePermits()).isEqualTo(1);
        assertThat(testOutputFormat.getAcquiredPermits()).isEqualTo(0);

        CompletableFuture<Void> completableFuture = new CompletableFuture<>();
        testOutputFormat.enqueueCompletableFuture(completableFuture);
        testOutputFormat.writeRecord("none");

        assertThat(testOutputFormat.getAvailablePermits()).isEqualTo(0);
        assertThat(testOutputFormat.getAcquiredPermits()).isEqualTo(1);

        completableFuture.completeExceptionally(new RuntimeException());

        assertThat(testOutputFormat.getAvailablePermits()).isEqualTo(1);
        assertThat(testOutputFormat.getAcquiredPermits()).isEqualTo(0);
        assertThatThrownBy(() -> testOutputFormat.close());
    }

    @Test
    void testReleaseOnThrowingSend() throws Exception {
        Function<String, CompletionStage<Void>> failingSendFunction =
                ignoredRecord -> {
                    throw new RuntimeException("expected");
                };

        try (TestOutputFormat testOutputFormat =
                createOpenedTestOutputFormat(failingSendFunction)) {

            assertThat(testOutputFormat.getAvailablePermits()).isEqualTo(1);
            assertThat(testOutputFormat.getAcquiredPermits()).isEqualTo(0);

            try {
                testOutputFormat.writeRecord("none");
            } catch (RuntimeException ignored) {
                /// there is no point asserting on the exception that we have set,
                // just avoid the test failure
            }
            // writeRecord acquires a permit that is then released when send fails
            assertThat(testOutputFormat.getAvailablePermits()).isEqualTo(1);
            assertThat(testOutputFormat.getAcquiredPermits()).isEqualTo(0);
        }
    }

    @Test
    void testMaxConcurrentRequestsReached() throws Exception {
        try (TestOutputFormat testOutputFormat =
                createOpenedTestOutputFormat(Duration.ofMillis(1))) {
            CompletableFuture<Void> completableFuture = new CompletableFuture<>();
            testOutputFormat.enqueueCompletableFuture(completableFuture);
            testOutputFormat.enqueueCompletableFuture(completableFuture);
            testOutputFormat.writeRecord("writeRecord #1");

            // writing a second time while the first request is still not completed and the
            // outputFormat is set for maxConcurrentRequests=1 will fail
            assertThatThrownBy(
                            () -> testOutputFormat.writeRecord("writeRecord #2"),
                            "Sending value should have experienced a TimeoutException.")
                    .hasCauseInstanceOf(TimeoutException.class);
            completableFuture.complete(null);
        }
    }

    private static TestOutputFormat createTestOutputFormat() {
        final TestOutputFormat testOutputFormat =
                new TestOutputFormat(1, DEFAULT_MAX_CONCURRENT_REQUESTS_TIMEOUT);
        testOutputFormat.configure(new Configuration());
        return testOutputFormat;
    }

    private static TestOutputFormat createOpenedTestOutputFormat() {
        return createOpenedTestOutputFormat(DEFAULT_MAX_CONCURRENT_REQUESTS_TIMEOUT);
    }

    private static TestOutputFormat createOpenedTestOutputFormat(
            Duration maxConcurrentRequestsTimeout) {
        final TestOutputFormat testOutputFormat =
                new TestOutputFormat(1, maxConcurrentRequestsTimeout);
        testOutputFormat.configure(new Configuration());
        testOutputFormat.open(1, 1);
        return testOutputFormat;
    }

    private static TestOutputFormat createOpenedTestOutputFormat(
            Function<String, CompletionStage<Void>> sendFunction) {
        final TestOutputFormat testOutputFormat =
                new TestOutputFormat(1, DEFAULT_MAX_CONCURRENT_REQUESTS_TIMEOUT, sendFunction);
        testOutputFormat.configure(new Configuration());
        testOutputFormat.open(1, 1);
        return testOutputFormat;
    }

    private static class TestOutputFormat extends OutputFormatBase<String, Void>
            implements AutoCloseable {
        private static final long serialVersionUID = 6646648756749403023L;

        private final Queue<CompletionStage<Void>> tasksQueue = new LinkedList<>();
        @Nullable private final Function<String, CompletionStage<Void>> sendFunction;

        private TestOutputFormat(int maxConcurrentRequests, Duration maxConcurrentRequestsTimeout) {
            super(maxConcurrentRequests, maxConcurrentRequestsTimeout);
            sendFunction = null;
        }

        private TestOutputFormat(
                int maxConcurrentRequests,
                Duration maxConcurrentRequestsTimeout,
                Function<String, CompletionStage<Void>> sendFunction) {
            super(maxConcurrentRequests, maxConcurrentRequestsTimeout);
            this.sendFunction = sendFunction;
        }

        @Override
        protected CompletionStage<Void> send(String record) {
            return sendFunction == null ? tasksQueue.poll() : sendFunction.apply(record);
        }

        void enqueueCompletableFuture(CompletableFuture<Void> completableFuture) {
            Preconditions.checkNotNull(completableFuture);
            tasksQueue.offer(completableFuture);
        }

        @Override
        public void configure(Configuration parameters) {}
    }
}
