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

package org.apache.flink.runtime.operators.coordination.util;

import org.junit.jupiter.api.Test;

import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;

import static org.apache.flink.core.testutils.FlinkAssertions.assertThatFuture;
import static org.assertj.core.api.Assertions.assertThat;

/** Unit tests for the {@link IncompleteFuturesTracker}. */
class IncompleteFuturesTrackerTest {

    @Test
    void testFutureTracked() {
        final IncompleteFuturesTracker tracker = new IncompleteFuturesTracker();
        final CompletableFuture<?> future = new CompletableFuture<>();

        tracker.trackFutureWhileIncomplete(future);

        assertThat(tracker.getCurrentIncompleteAndReset()).containsExactly(future);
    }

    @Test
    void testFutureRemovedAfterCompletion() {
        final IncompleteFuturesTracker tracker = new IncompleteFuturesTracker();
        final CompletableFuture<?> future = new CompletableFuture<>();

        tracker.trackFutureWhileIncomplete(future);
        future.complete(null);

        assertThat(tracker.getCurrentIncompleteAndReset()).doesNotContain(future);
    }

    @Test
    void testFutureNotAddedIfAlreadyCompleted() {
        final IncompleteFuturesTracker tracker = new IncompleteFuturesTracker();
        final CompletableFuture<?> future = new CompletableFuture<>();

        future.complete(null);
        tracker.trackFutureWhileIncomplete(future);

        assertThat(tracker.getCurrentIncompleteAndReset()).doesNotContain(future);
    }

    @Test
    void testFailFutures() {
        final IncompleteFuturesTracker tracker = new IncompleteFuturesTracker();
        final CompletableFuture<?> future = new CompletableFuture<>();

        tracker.trackFutureWhileIncomplete(future);

        final Exception expectedException = new Exception();
        tracker.failAllFutures(expectedException);

        assertThatFuture(future)
                .eventuallyFailsWith(ExecutionException.class)
                .withCause(expectedException);
    }

    @Test
    void testFailFuturesImmediately() {
        final IncompleteFuturesTracker tracker = new IncompleteFuturesTracker();
        final CompletableFuture<?> future = new CompletableFuture<>();

        final Exception expectedException = new Exception();
        tracker.failAllFutures(expectedException);

        tracker.trackFutureWhileIncomplete(future);

        assertThatFuture(future)
                .eventuallyFailsWith(ExecutionException.class)
                .withCause(expectedException);
    }

    @Test
    void testResetClearsTrackedFutures() {
        final IncompleteFuturesTracker tracker = new IncompleteFuturesTracker();

        final CompletableFuture<?> future = new CompletableFuture<>();
        tracker.trackFutureWhileIncomplete(future);
        tracker.getCurrentIncompleteAndReset();

        assertThat(tracker.getCurrentIncompleteAndReset()).isEmpty();
    }
}
