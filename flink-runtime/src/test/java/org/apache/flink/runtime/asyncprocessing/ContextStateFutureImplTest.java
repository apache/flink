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

package org.apache.flink.runtime.asyncprocessing;

import org.apache.flink.core.state.StateFutureImpl.AsyncFrameworkExceptionHandler;
import org.apache.flink.core.state.StateFutureUtils;
import org.apache.flink.runtime.asyncprocessing.EpochManager.Epoch;
import org.apache.flink.runtime.state.KeyGroupRangeAssignment;
import org.apache.flink.util.FlinkException;
import org.apache.flink.util.FlinkRuntimeException;
import org.apache.flink.util.function.ThrowingRunnable;

import org.junit.Test;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.LinkedList;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.fail;

/** Tests for {@link ContextStateFutureImpl}. */
public class ContextStateFutureImplTest {

    @Test
    public void testThenApply() {
        SingleStepRunner runner = new SingleStepRunner();
        RecordContext<String> recordContext = buildRecordContext("a", "b");
        TestAsyncFrameworkExceptionHandler exceptionHandler =
                new TestAsyncFrameworkExceptionHandler();

        // validate
        ContextStateFutureImpl<Void> future =
                new ContextStateFutureImpl<>(runner::submit, exceptionHandler, recordContext);
        assertThat(recordContext.getReferenceCount()).isEqualTo(1);
        future.thenApply((v) -> 1L);
        future.complete(null);
        assertThat(runner.runThrough()).isTrue();
        assertThat(recordContext.getReferenceCount()).isEqualTo(0);

        // validate completion before callback
        future = new ContextStateFutureImpl<>(runner::submit, exceptionHandler, recordContext);
        assertThat(recordContext.getReferenceCount()).isEqualTo(1);
        future.complete(null);
        assertThat(recordContext.getReferenceCount()).isEqualTo(0);
        future.thenApply((v) -> 1L);
        assertThat(recordContext.getReferenceCount()).isEqualTo(0);
        assertThat(runner.runThrough()).isFalse();

        // validate exception
        future =
                new ContextStateFutureImpl<>(
                        (runnable) -> {
                            runner.submit(() -> runnable.run());
                        },
                        exceptionHandler,
                        recordContext);
        future.thenApply(
                (v) -> {
                    throw new FlinkRuntimeException("Artificial exception for thenApply().");
                });
        future.complete(null);
        try {
            runner.runThrough();
            fail("Should throw an exception.");
        } catch (Throwable e) {
            assertThat(e).isInstanceOf(FlinkRuntimeException.class);
            assertThat(e.getMessage()).isEqualTo("Artificial exception for thenApply().");
        }
    }

    @Test
    public void testThenAccept() {
        SingleStepRunner runner = new SingleStepRunner();
        RecordContext<String> recordContext = buildRecordContext("a", "b");
        TestAsyncFrameworkExceptionHandler exceptionHandler =
                new TestAsyncFrameworkExceptionHandler();

        // validate
        ContextStateFutureImpl<Void> future =
                new ContextStateFutureImpl<>(runner::submit, exceptionHandler, recordContext);
        assertThat(recordContext.getReferenceCount()).isEqualTo(1);
        future.thenAccept((v) -> {});
        future.complete(null);
        assertThat(runner.runThrough()).isTrue();
        assertThat(recordContext.getReferenceCount()).isEqualTo(0);

        // validate completion before callback
        future = new ContextStateFutureImpl<>(runner::submit, exceptionHandler, recordContext);
        assertThat(recordContext.getReferenceCount()).isEqualTo(1);
        future.complete(null);
        assertThat(recordContext.getReferenceCount()).isEqualTo(0);
        future.thenAccept((v) -> {});
        assertThat(recordContext.getReferenceCount()).isEqualTo(0);
        assertThat(runner.runThrough()).isFalse();

        // validate exception
        future =
                new ContextStateFutureImpl<>(
                        (runnable) -> {
                            runner.submit(() -> runnable.run());
                        },
                        exceptionHandler,
                        recordContext);
        try {
            future.complete(null);
            future.thenAccept(
                    (v) -> {
                        throw new FlinkRuntimeException("Artificial exception for thenAccept().");
                    });
            fail("Should throw an exception.");
        } catch (Throwable e) {
            assertThat(e).isInstanceOf(FlinkRuntimeException.class);
            assertThat(e.getMessage()).isEqualTo("Artificial exception for thenAccept().");
            assertThat(exceptionHandler.exception).isNull();
        }
    }

    @Test
    public void testThenCompose() {
        SingleStepRunner runner = new SingleStepRunner();
        RecordContext<String> recordContext = buildRecordContext("a", "b");
        TestAsyncFrameworkExceptionHandler exceptionHandler =
                new TestAsyncFrameworkExceptionHandler();

        // validate
        ContextStateFutureImpl<Void> future =
                new ContextStateFutureImpl<>(runner::submit, exceptionHandler, recordContext);
        assertThat(recordContext.getReferenceCount()).isEqualTo(1);
        future.thenCompose((v) -> StateFutureUtils.completedFuture(1L));
        future.complete(null);
        assertThat(runner.runThrough()).isTrue();
        assertThat(recordContext.getReferenceCount()).isEqualTo(0);

        // validate completion before callback
        future = new ContextStateFutureImpl<>(runner::submit, exceptionHandler, recordContext);
        assertThat(recordContext.getReferenceCount()).isEqualTo(1);
        future.complete(null);
        assertThat(recordContext.getReferenceCount()).isEqualTo(0);
        future.thenCompose((v) -> StateFutureUtils.completedFuture(1L));
        assertThat(recordContext.getReferenceCount()).isEqualTo(0);
        assertThat(runner.runThrough()).isFalse();

        // validate exception
        future =
                new ContextStateFutureImpl<>(
                        (runnable) -> {
                            runner.submit(() -> runnable.run());
                        },
                        exceptionHandler,
                        recordContext);

        future.thenCompose(
                (v) -> {
                    throw new FlinkException("Artificial exception for thenCompose().");
                });
        future.complete(null);
        try {
            runner.runThrough();
            fail("Should throw an exception.");
        } catch (Throwable e) {
            assertThat(e).isInstanceOf(RuntimeException.class);
            assertThat(e.getMessage())
                    .isEqualTo(
                            "org.apache.flink.util.FlinkException: Artificial exception for thenCompose().");
        }
    }

    @Test
    public void testThenCombine() {
        SingleStepRunner runner = new SingleStepRunner();
        RecordContext<String> recordContext = buildRecordContext("a", "b");
        TestAsyncFrameworkExceptionHandler exceptionHandler =
                new TestAsyncFrameworkExceptionHandler();

        // validate
        ContextStateFutureImpl<Void> future1 =
                new ContextStateFutureImpl<>(runner::submit, exceptionHandler, recordContext);
        ContextStateFutureImpl<Void> future2 =
                new ContextStateFutureImpl<>(runner::submit, exceptionHandler, recordContext);
        assertThat(recordContext.getReferenceCount()).isEqualTo(2);
        future1.thenCombine(future2, (v1, v2) -> 1L);
        future1.complete(null);
        future2.complete(null);
        assertThat(runner.runThrough()).isTrue();
        assertThat(recordContext.getReferenceCount()).isEqualTo(0);

        // validate future1 completion before callback
        future1 = new ContextStateFutureImpl<>(runner::submit, exceptionHandler, recordContext);
        future2 = new ContextStateFutureImpl<>(runner::submit, exceptionHandler, recordContext);
        assertThat(recordContext.getReferenceCount()).isEqualTo(2);
        future1.complete(null);
        future1.thenCombine(future2, (v1, v2) -> 1L);
        assertThat(recordContext.getReferenceCount()).isGreaterThan(1);
        future2.complete(null);
        assertThat(runner.runThrough()).isTrue();
        assertThat(recordContext.getReferenceCount()).isEqualTo(0);

        // validate future2 completion before callback
        future1 = new ContextStateFutureImpl<>(runner::submit, exceptionHandler, recordContext);
        future2 = new ContextStateFutureImpl<>(runner::submit, exceptionHandler, recordContext);
        assertThat(recordContext.getReferenceCount()).isEqualTo(2);
        future2.complete(null);
        future1.thenCombine(future2, (v1, v2) -> 1L);
        assertThat(recordContext.getReferenceCount()).isGreaterThan(1);
        future1.complete(null);
        assertThat(runner.runThrough()).isTrue();
        assertThat(recordContext.getReferenceCount()).isEqualTo(0);

        // validate both future1 and future2 completion before callback
        future1 = new ContextStateFutureImpl<>(runner::submit, exceptionHandler, recordContext);
        future2 = new ContextStateFutureImpl<>(runner::submit, exceptionHandler, recordContext);
        assertThat(recordContext.getReferenceCount()).isEqualTo(2);
        future1.complete(null);
        future2.complete(null);
        future1.thenCombine(future2, (v1, v2) -> 1L);
        assertThat(runner.runThrough()).isFalse();
        assertThat(recordContext.getReferenceCount()).isEqualTo(0);
    }

    @Test
    public void testComplex() {
        SingleStepRunner runner = new SingleStepRunner();
        RecordContext<String> recordContext = buildRecordContext("a", "b");
        TestAsyncFrameworkExceptionHandler exceptionHandler =
                new TestAsyncFrameworkExceptionHandler();

        for (int i = 0; i < 32; i++) { // 2^5 for completion status combination
            // Each bit of 'i' represents the complete status when the user code executes.
            // 1 stands for the completion of corresponding future before the user code execution.
            // While 0, on the contrary, represents the scenario where the user code is executed
            // first and then the future completes.
            ArrayList<ContextStateFutureImpl<Void>> futures = new ArrayList<>(6);
            for (int j = 0; j < 5; j++) {
                ContextStateFutureImpl<Void> future =
                        new ContextStateFutureImpl<>(
                                runner::submit, exceptionHandler, recordContext);
                futures.add(future);
                // Complete future before user code.
                if (((i >>> j) & 1) == 1) {
                    future.complete(null);
                }
            }

            // Simulate user code logic.
            StateFutureUtils.combineAll(
                            Arrays.asList(futures.get(0), futures.get(1), futures.get(2)))
                    .thenCombine(
                            futures.get(3),
                            (a, b) -> {
                                return 1L;
                            })
                    .thenCompose(
                            (a) -> {
                                return futures.get(4);
                            })
                    .thenApply(
                            (e) -> {
                                return 2L;
                            })
                    .thenAccept((b) -> {});

            // Complete unfinished future.
            for (int j = 0; j < 5; j++) {
                if (((i >>> j) & 1) == 0) {
                    futures.get(j).complete(null);
                }
            }

            if (i == 31) {
                // All completed before user code.
                assertThat(recordContext.getReferenceCount())
                        .withFailMessage("The reference counted tests fail for profile id %d", i)
                        .isEqualTo(0);
                assertThat(runner.runThrough())
                        .withFailMessage("The reference counted tests fail for profile id %d", i)
                        .isFalse();
            } else {
                assertThat(recordContext.getReferenceCount())
                        .withFailMessage("The reference counted tests fail for profile id %d", i)
                        .isGreaterThan(0);
                assertThat(runner.runThrough())
                        .withFailMessage("The reference counted tests fail for profile id %d", i)
                        .isTrue();
                assertThat(recordContext.getReferenceCount())
                        .withFailMessage("The reference counted tests fail for profile id %d", i)
                        .isEqualTo(0);
            }
        }
    }

    private <K> RecordContext<K> buildRecordContext(Object record, K key) {
        int keyGroup = KeyGroupRangeAssignment.assignToKeyGroup(key, 128);
        return new RecordContext<>(record, key, (e) -> {}, keyGroup, new Epoch(0));
    }

    /** A runner that performs single-step debugging. */
    public static class SingleStepRunner {
        private final LinkedList<ThrowingRunnable<? extends Exception>> runnables =
                new LinkedList<>();

        public void submit(ThrowingRunnable<? extends Exception> runnable) {
            runnables.add(runnable);
        }

        public boolean runThrough() {
            boolean run = false;
            while (!runnables.isEmpty()) {
                ThrowingRunnable.unchecked(runnables.poll()).run();
                run = true;
            }
            return run;
        }
    }

    static class TestAsyncFrameworkExceptionHandler implements AsyncFrameworkExceptionHandler {
        String message;
        Throwable exception;

        public void handleException(String message, Throwable exception) {
            this.message = message;
            this.exception = exception;
        }
    }
}
