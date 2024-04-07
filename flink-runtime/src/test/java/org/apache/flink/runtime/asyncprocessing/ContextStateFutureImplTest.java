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

import org.apache.flink.core.state.StateFutureUtils;

import org.junit.Test;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.LinkedList;

import static org.assertj.core.api.Assertions.assertThat;

/** Tests for {@link ContextStateFutureImpl}. */
public class ContextStateFutureImplTest {

    @Test
    public void testThenApply() {
        SingleStepRunner runner = new SingleStepRunner();
        RecordContext<String, String> recordContext = new RecordContext<>("a", "b", (e) -> {});

        // validate
        ContextStateFutureImpl<Void> future =
                new ContextStateFutureImpl<>(runner::submit, recordContext);
        assertThat(recordContext.getReferenceCount()).isEqualTo(1);
        future.thenApply((v) -> 1L);
        future.complete(null);
        assertThat(runner.runThrough()).isTrue();
        assertThat(recordContext.getReferenceCount()).isEqualTo(0);

        // validate completion before callback
        future = new ContextStateFutureImpl<>(runner::submit, recordContext);
        assertThat(recordContext.getReferenceCount()).isEqualTo(1);
        future.complete(null);
        assertThat(recordContext.getReferenceCount()).isEqualTo(0);
        future.thenApply((v) -> 1L);
        assertThat(recordContext.getReferenceCount()).isEqualTo(0);
        assertThat(runner.runThrough()).isFalse();
    }

    @Test
    public void testThenAccept() {
        SingleStepRunner runner = new SingleStepRunner();
        RecordContext<String, String> recordContext = new RecordContext<>("a", "b", (e) -> {});

        // validate
        ContextStateFutureImpl<Void> future =
                new ContextStateFutureImpl<>(runner::submit, recordContext);
        assertThat(recordContext.getReferenceCount()).isEqualTo(1);
        future.thenAccept((v) -> {});
        future.complete(null);
        assertThat(runner.runThrough()).isTrue();
        assertThat(recordContext.getReferenceCount()).isEqualTo(0);

        // validate completion before callback
        future = new ContextStateFutureImpl<>(runner::submit, recordContext);
        assertThat(recordContext.getReferenceCount()).isEqualTo(1);
        future.complete(null);
        assertThat(recordContext.getReferenceCount()).isEqualTo(0);
        future.thenAccept((v) -> {});
        assertThat(recordContext.getReferenceCount()).isEqualTo(0);
        assertThat(runner.runThrough()).isFalse();
    }

    @Test
    public void testThenCompose() {
        SingleStepRunner runner = new SingleStepRunner();
        RecordContext<String, String> recordContext = new RecordContext<>("a", "b", (e) -> {});

        // validate
        ContextStateFutureImpl<Void> future =
                new ContextStateFutureImpl<>(runner::submit, recordContext);
        assertThat(recordContext.getReferenceCount()).isEqualTo(1);
        future.thenCompose((v) -> StateFutureUtils.completedFuture(1L));
        future.complete(null);
        assertThat(runner.runThrough()).isTrue();
        assertThat(recordContext.getReferenceCount()).isEqualTo(0);

        // validate completion before callback
        future = new ContextStateFutureImpl<>(runner::submit, recordContext);
        assertThat(recordContext.getReferenceCount()).isEqualTo(1);
        future.complete(null);
        assertThat(recordContext.getReferenceCount()).isEqualTo(0);
        future.thenCompose((v) -> StateFutureUtils.completedFuture(1L));
        assertThat(recordContext.getReferenceCount()).isEqualTo(0);
        assertThat(runner.runThrough()).isFalse();
    }

    @Test
    public void testThenCombine() {
        SingleStepRunner runner = new SingleStepRunner();
        RecordContext<String, String> recordContext = new RecordContext<>("a", "b", (e) -> {});

        // validate
        ContextStateFutureImpl<Void> future1 =
                new ContextStateFutureImpl<>(runner::submit, recordContext);
        ContextStateFutureImpl<Void> future2 =
                new ContextStateFutureImpl<>(runner::submit, recordContext);
        assertThat(recordContext.getReferenceCount()).isEqualTo(2);
        future1.thenCombine(future2, (v1, v2) -> 1L);
        future1.complete(null);
        future2.complete(null);
        assertThat(runner.runThrough()).isTrue();
        assertThat(recordContext.getReferenceCount()).isEqualTo(0);

        // validate future1 completion before callback
        future1 = new ContextStateFutureImpl<>(runner::submit, recordContext);
        future2 = new ContextStateFutureImpl<>(runner::submit, recordContext);
        assertThat(recordContext.getReferenceCount()).isEqualTo(2);
        future1.complete(null);
        future1.thenCombine(future2, (v1, v2) -> 1L);
        assertThat(recordContext.getReferenceCount()).isGreaterThan(1);
        future2.complete(null);
        assertThat(runner.runThrough()).isTrue();
        assertThat(recordContext.getReferenceCount()).isEqualTo(0);

        // validate future2 completion before callback
        future1 = new ContextStateFutureImpl<>(runner::submit, recordContext);
        future2 = new ContextStateFutureImpl<>(runner::submit, recordContext);
        assertThat(recordContext.getReferenceCount()).isEqualTo(2);
        future2.complete(null);
        future1.thenCombine(future2, (v1, v2) -> 1L);
        assertThat(recordContext.getReferenceCount()).isGreaterThan(1);
        future1.complete(null);
        assertThat(runner.runThrough()).isTrue();
        assertThat(recordContext.getReferenceCount()).isEqualTo(0);

        // validate both future1 and future2 completion before callback
        future1 = new ContextStateFutureImpl<>(runner::submit, recordContext);
        future2 = new ContextStateFutureImpl<>(runner::submit, recordContext);
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
        RecordContext<String, String> recordContext = new RecordContext<>("a", "b", (e) -> {});

        for (int i = 0; i < 32; i++) { // 2^5 for completion status combination
            // Each bit of 'i' represents the complete status when the user code executes.
            // 1 stands for the completion of corresponding future before the user code execution.
            // While 0, on the contrary, represents the scenario where the user code is executed
            // first and then the future completes.
            ArrayList<ContextStateFutureImpl<Void>> futures = new ArrayList<>(6);
            for (int j = 0; j < 5; j++) {
                ContextStateFutureImpl<Void> future =
                        new ContextStateFutureImpl<>(runner::submit, recordContext);
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

    /** A runner that performs single-step debugging. */
    public static class SingleStepRunner {
        private final LinkedList<Runnable> runnables = new LinkedList<>();

        public void submit(Runnable runnable) {
            runnables.add(runnable);
        }

        public boolean runThrough() {
            boolean run = false;
            while (!runnables.isEmpty()) {
                runnables.poll().run();
                run = true;
            }
            return run;
        }
    }
}
