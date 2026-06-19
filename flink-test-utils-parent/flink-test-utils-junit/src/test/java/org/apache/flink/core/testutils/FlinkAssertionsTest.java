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

package org.apache.flink.core.testutils;

import org.junit.jupiter.api.Test;

import java.time.Duration;
import java.util.concurrent.CancellationException;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;

import static org.apache.flink.core.testutils.FlinkAssertions.assertThatFuture;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/** Test suite for {@link FlinkAssertions}. */
class FlinkAssertionsTest {

    @Test
    void testEventuallySucceedsWithCompletedFuture() {
        final CompletableFuture<String> completedFuture =
                CompletableFuture.completedFuture("Apache Flink");
        assertThatFuture(completedFuture).eventuallySucceeds().isEqualTo("Apache Flink");
    }

    @Test
    void testEventuallySucceedsWithFailedFuture() {
        final CompletableFuture<String> failedFuture = new CompletableFuture<>();
        failedFuture.completeExceptionally(new IllegalStateException("Squirrel"));
        assertThatThrownBy(() -> assertThatFuture(failedFuture).eventuallySucceeds())
                .isInstanceOf(AssertionError.class)
                .hasMessageContaining("Squirrel");
    }

    @Test
    void testEventuallySucceedsWithIncompleteFutureTimesOut() throws Exception {
        assertWaitingForInterrupt(
                () ->
                        assertThatThrownBy(
                                        () ->
                                                assertThatFuture(new CompletableFuture<>())
                                                        .eventuallySucceeds())
                                .isInstanceOf(AssertionError.class),
                Duration.ofMillis(10));
    }

    @Test
    void testEventuallyFailsWithCompletedFuture() {
        final CompletableFuture<String> completedFuture =
                CompletableFuture.completedFuture("Apache Flink");
        assertThatThrownBy(
                        () ->
                                assertThatFuture(completedFuture)
                                        .eventuallyFailsWith(IllegalStateException.class))
                .isInstanceOf(AssertionError.class)
                .hasMessageContaining("Apache Flink");
    }

    @Test
    void testEventuallyFailsWithFailedFuture() {
        final CompletableFuture<String> failedFuture = new CompletableFuture<>();
        failedFuture.completeExceptionally(new IllegalStateException());
        assertThatFuture(failedFuture)
                .eventuallyFailsWith(ExecutionException.class)
                .withCauseInstanceOf(IllegalStateException.class);
    }

    @Test
    void testEventuallyFailsWithFailedFutureWithDifferentException() {
        final CompletableFuture<String> failedFuture = new CompletableFuture<>();
        failedFuture.completeExceptionally(new IllegalStateException());
        assertThatThrownBy(
                        () ->
                                assertThatFuture(failedFuture)
                                        .eventuallyFailsWith(CancellationException.class))
                .isInstanceOf(AssertionError.class)
                .hasMessageContaining("CancellationException");
    }

    @Test
    void testEventuallyFailsWithIncompleteFutureTimesOut() throws Exception {
        assertWaitingForInterrupt(
                () ->
                        assertThatFuture(new CompletableFuture<>())
                                .eventuallyFails()
                                .withThrowableOfType(InterruptedException.class),
                Duration.ofMillis(10));
    }

    @Test
    void testWillNotCompleteWithin() {
        CompletableFuture<Void> future = new CompletableFuture<>();
        assertThatFuture(future).willNotCompleteWithin(Duration.ofMillis(1));

        // test completed future.
        assertThatThrownBy(
                        () ->
                                assertThatFuture(CompletableFuture.completedFuture(null))
                                        .willNotCompleteWithin(Duration.ofMillis(1)))
                .isInstanceOf(AssertionError.class)
                .hasMessageContaining("have not completed");

        // test completed exceptionally future.
        CompletableFuture<Void> failedFuture = new CompletableFuture<>();
        failedFuture.completeExceptionally(new RuntimeException());
        assertThatThrownBy(
                        () ->
                                assertThatFuture(failedFuture)
                                        .willNotCompleteWithin(Duration.ofMillis(1)))
                .isInstanceOf(AssertionError.class)
                .hasMessageContaining("have not completed");
    }

    private static void assertWaitingForInterrupt(Runnable runnable, Duration timeout)
            throws Exception {
        final CheckedThread thread =
                new CheckedThread() {
                    @Override
                    public void go() {
                        runnable.run();
                    }
                };
        thread.start();
        Thread.sleep(timeout.toMillis());
        assertThat(thread.isAlive()).isTrue();
        thread.interrupt();
        thread.sync();
    }
}
