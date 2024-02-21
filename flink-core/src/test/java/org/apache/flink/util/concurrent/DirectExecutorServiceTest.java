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

package org.apache.flink.util.concurrent;

import org.apache.flink.util.function.ThrowingConsumer;

import org.junit.jupiter.api.Test;

import java.util.Collections;
import java.util.List;
import java.util.concurrent.Callable;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.RejectedExecutionException;
import java.util.concurrent.TimeUnit;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatNoException;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

class DirectExecutorServiceTest {

    @Test
    void testExecute() {
        final CompletableFuture<Thread> future = new CompletableFuture<>();
        testTaskSubmissionBeforeShutdown(
                testInstance ->
                        testInstance.execute(() -> future.complete(Thread.currentThread())));

        assertThat(future).isCompletedWithValue(Thread.currentThread());
    }

    @Test
    void testSubmitRunnable() {
        final CompletableFuture<Thread> future = new CompletableFuture<>();
        testTaskSubmissionBeforeShutdown(
                testInstance -> testInstance.submit(() -> future.complete(Thread.currentThread())));

        assertThat(future).isCompletedWithValue(Thread.currentThread());
    }

    @Test
    void testSubmitCallable() {
        final CompletableFuture<Thread> future = new CompletableFuture<>();
        testTaskSubmissionBeforeShutdown(
                testInstance -> testInstance.submit(callableFromFuture(future)));

        assertThat(future).isCompletedWithValue(Thread.currentThread());
    }

    @Test
    void testSubmitRunnableWithResult() {
        final CompletableFuture<Thread> future = new CompletableFuture<>();
        testTaskSubmissionBeforeShutdown(
                testInstance ->
                        testInstance.submit(() -> future.complete(Thread.currentThread()), null));

        assertThat(future).isCompletedWithValue(Thread.currentThread());
    }

    @Test
    void testInvokeAll() {
        final CompletableFuture<Thread> future = new CompletableFuture<>();
        testTaskSubmissionBeforeShutdown(
                testInstance -> testInstance.invokeAll(callableCollectionFromFuture(future)));

        assertThat(future).isCompletedWithValue(Thread.currentThread());
    }

    @Test
    void testInvokeAllWithTimeout() {
        final CompletableFuture<Thread> future = new CompletableFuture<>();
        testTaskSubmissionBeforeShutdown(
                testInstance ->
                        testInstance.invokeAll(
                                callableCollectionFromFuture(future), 1, TimeUnit.DAYS));

        assertThat(future).isCompletedWithValue(Thread.currentThread());
    }

    @Test
    void testInvokeAny() {
        final CompletableFuture<Thread> future = new CompletableFuture<>();
        testTaskSubmissionBeforeShutdown(
                testInstance -> testInstance.invokeAny(callableCollectionFromFuture(future)));

        assertThat(future).isCompletedWithValue(Thread.currentThread());
    }

    @Test
    void testInvokeAnyWithTimeout() {
        final CompletableFuture<Thread> future = new CompletableFuture<>();
        testTaskSubmissionBeforeShutdown(
                testInstance ->
                        testInstance.invokeAny(
                                callableCollectionFromFuture(future), 1, TimeUnit.DAYS));

        assertThat(future).isCompletedWithValue(Thread.currentThread());
    }

    private void testTaskSubmissionBeforeShutdown(
            ThrowingConsumer<ExecutorService, Throwable> taskSubmission) {
        final ExecutorService testInstance = new DirectExecutorService(true);
        testSuccessfulTaskSubmission(testInstance, taskSubmission);

        testInstance.shutdown();
    }

    @Test
    void testExecuteWithNoopShutdown() {
        final CompletableFuture<Thread> future = new CompletableFuture<>();
        testWithNoopShutdown(
                testInstance ->
                        testInstance.execute(() -> future.complete(Thread.currentThread())));

        assertThat(future).isCompletedWithValue(Thread.currentThread());
    }

    @Test
    void testSubmitRunnableWithNoopShutdown() {
        final CompletableFuture<Thread> future = new CompletableFuture<>();
        testWithNoopShutdown(
                testInstance -> testInstance.submit(() -> future.complete(Thread.currentThread())));

        assertThat(future).isCompletedWithValue(Thread.currentThread());
    }

    @Test
    void testSubmitCallableWithNoopShutdown() {
        final CompletableFuture<Thread> future = new CompletableFuture<>();
        testWithNoopShutdown(testInstance -> testInstance.submit(callableFromFuture(future)));

        assertThat(future).isCompletedWithValue(Thread.currentThread());
    }

    @Test
    void testSubmitRunnableWithResultAndNoopShutdown() {
        final CompletableFuture<Thread> future = new CompletableFuture<>();
        testWithNoopShutdown(
                testInstance ->
                        testInstance.submit(() -> future.complete(Thread.currentThread()), null));

        assertThat(future).isCompletedWithValue(Thread.currentThread());
    }

    @Test
    void testInvokeAllWithNoopShutdown() {
        final CompletableFuture<Thread> future = new CompletableFuture<>();
        testWithNoopShutdown(
                testInstance -> testInstance.invokeAll(callableCollectionFromFuture(future)));

        assertThat(future).isCompletedWithValue(Thread.currentThread());
    }

    @Test
    void testInvokeAllWithTimeoutAndNoopShutdown() {
        final CompletableFuture<Thread> future = new CompletableFuture<>();
        testWithNoopShutdown(
                testInstance ->
                        testInstance.invokeAll(
                                callableCollectionFromFuture(future), 1, TimeUnit.DAYS));

        assertThat(future).isCompletedWithValue(Thread.currentThread());
    }

    @Test
    void testInvokeAnyWithNoopShutdown() {
        final CompletableFuture<Thread> future = new CompletableFuture<>();
        testWithNoopShutdown(
                testInstance -> testInstance.invokeAny(callableCollectionFromFuture(future)));

        assertThat(future).isCompletedWithValue(Thread.currentThread());
    }

    @Test
    void testInvokeAnyWithTimeoutAndNoopShutdown() {
        final CompletableFuture<Thread> future = new CompletableFuture<>();
        testWithNoopShutdown(
                testInstance ->
                        testInstance.invokeAny(
                                callableCollectionFromFuture(future), 1, TimeUnit.DAYS));

        assertThat(future).isCompletedWithValue(Thread.currentThread());
    }

    private void testWithNoopShutdown(ThrowingConsumer<ExecutorService, Throwable> taskSubmission) {
        final ExecutorService testInstance = new DirectExecutorService(false);
        testInstance.shutdown();

        testSuccessfulTaskSubmission(testInstance, taskSubmission);
    }

    private static List<Callable<Void>> callableCollectionFromFuture(
            CompletableFuture<Thread> future) {
        return Collections.singletonList(callableFromFuture(future));
    }

    private static Callable<Void> callableFromFuture(CompletableFuture<Thread> future) {
        return () -> {
            future.complete(Thread.currentThread());
            return null;
        };
    }

    private void testSuccessfulTaskSubmission(
            ExecutorService testInstance,
            ThrowingConsumer<ExecutorService, Throwable> taskSubmission) {
        assertThatNoException().isThrownBy(() -> taskSubmission.accept(testInstance));
    }

    @Test
    void testRejectedExecute() {
        testRejectedExecutionException(testInstance -> testInstance.execute(() -> {}));
    }

    @Test
    void testRejectedSubmitRunnable() {
        testRejectedExecutionException(testInstance -> testInstance.submit(() -> {}));
    }

    @Test
    void testRejectedSubmitCallable() {
        testRejectedExecutionException(testInstance -> testInstance.submit(() -> null));
    }

    @Test
    void testRejectedSubmitWithResult() {
        testRejectedExecutionException(testInstance -> testInstance.submit(() -> {}, null));
    }

    @Test
    void testRejectedInvokeAll() {
        testRejectedExecutionException(
                testInstance -> testInstance.invokeAll(Collections.singleton(() -> null)));
    }

    @Test
    void testRejectedInvokeAllWithEmptyList() {
        testRejectedExecutionException(
                testInstance -> testInstance.invokeAll(Collections.emptyList()));
    }

    @Test
    void testRejectedInvokeAllWithTimeout() {
        testRejectedExecutionException(
                testInstance ->
                        testInstance.invokeAll(
                                Collections.singleton(() -> null), 1L, TimeUnit.DAYS));
    }

    @Test
    void testRejectedInvokeAllWithEmptyListAndTimeout() {
        testRejectedExecutionException(
                testInstance -> testInstance.invokeAll(Collections.emptyList(), 1L, TimeUnit.DAYS));
    }

    @Test
    void testRejectedInvokeAny() {
        testRejectedExecutionException(
                testInstance -> testInstance.invokeAny(Collections.singleton(() -> null)));
    }

    @Test
    void testRejectedInvokeAnyWithEmptyList() {
        testRejectedExecutionException(
                testInstance -> testInstance.invokeAny(Collections.emptyList()));
    }

    @Test
    void testRejectedInvokeAnyWithTimeout() {
        testRejectedExecutionException(
                testInstance ->
                        testInstance.invokeAll(
                                Collections.singleton(() -> null), 1L, TimeUnit.DAYS));
    }

    @Test
    void testRejectedInvokeAnyWithEmptyListAndTimeout() {
        testRejectedExecutionException(
                testInstance -> testInstance.invokeAll(Collections.emptyList(), 1L, TimeUnit.DAYS));
    }

    private void testRejectedExecutionException(
            ThrowingConsumer<ExecutorService, Throwable> taskSubmission) {
        final ExecutorService testInstance = new DirectExecutorService(true);
        testInstance.shutdown();

        assertThatThrownBy(() -> taskSubmission.accept(testInstance))
                .isInstanceOf(RejectedExecutionException.class);
    }
}
