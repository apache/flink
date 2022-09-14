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

import org.junit.Test;

import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.fail;

/** Unit tests for the {@link IncompleteFuturesTracker}. */
public class IncompleteFuturesTrackerTest {

    @Test
    public void testFutureTracked() {
        final IncompleteFuturesTracker tracker = new IncompleteFuturesTracker();
        final CompletableFuture<?> future = new CompletableFuture<>();

        tracker.trackFutureWhileIncomplete(future);

        assertThat(tracker.getCurrentIncompleteAndReset()).containsExactly(future);
    }

    @Test
    public void testFutureRemovedAfterCompletion() {
        final IncompleteFuturesTracker tracker = new IncompleteFuturesTracker();
        final CompletableFuture<?> future = new CompletableFuture<>();

        tracker.trackFutureWhileIncomplete(future);
        future.complete(null);

        assertThat(tracker.getCurrentIncompleteAndReset()).doesNotContain(future);
    }

    @Test
    public void testFutureNotAddedIfAlreadyCompleted() {
        final IncompleteFuturesTracker tracker = new IncompleteFuturesTracker();
        final CompletableFuture<?> future = new CompletableFuture<>();

        future.complete(null);
        tracker.trackFutureWhileIncomplete(future);

        assertThat(tracker.getCurrentIncompleteAndReset()).doesNotContain(future);
    }

    @Test
    public void testFailFutures() throws Exception {
        final IncompleteFuturesTracker tracker = new IncompleteFuturesTracker();
        final CompletableFuture<?> future = new CompletableFuture<>();

        tracker.trackFutureWhileIncomplete(future);

        final Exception expectedException = new Exception();
        tracker.failAllFutures(expectedException);

        assertThat(future).isCompletedExceptionally();
        try {
            future.get();
            fail(null);
        } catch (ExecutionException e) {
            assertThat(e.getCause()).isEqualTo(expectedException);
        }
    }

    @Test
    public void testFailFuturesImmediately() throws Exception {
        final IncompleteFuturesTracker tracker = new IncompleteFuturesTracker();
        final CompletableFuture<?> future = new CompletableFuture<>();

        final Exception expectedException = new Exception();
        tracker.failAllFutures(expectedException);

        tracker.trackFutureWhileIncomplete(future);

        assertThat(future).isCompletedExceptionally();
        try {
            future.get();
            fail(null);
        } catch (ExecutionException e) {
            assertThat(e.getCause()).isEqualTo(expectedException);
        }
    }

    @Test
    public void testResetClearsTrackedFutures() {
        final IncompleteFuturesTracker tracker = new IncompleteFuturesTracker();

        final CompletableFuture<?> future = new CompletableFuture<>();
        tracker.trackFutureWhileIncomplete(future);
        tracker.getCurrentIncompleteAndReset();

        assertThat(tracker.getCurrentIncompleteAndReset()).isEmpty();
    }
}
