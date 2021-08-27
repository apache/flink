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

import static org.hamcrest.Matchers.contains;
import static org.hamcrest.Matchers.empty;
import static org.hamcrest.Matchers.not;
import static org.junit.Assert.assertSame;
import static org.junit.Assert.assertThat;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

/** Unit tests for the {@link IncompleteFuturesTracker}. */
public class IncompleteFuturesTrackerTest {

    @Test
    public void testFutureTracked() {
        final IncompleteFuturesTracker tracker = new IncompleteFuturesTracker();
        final CompletableFuture<?> future = new CompletableFuture<>();

        tracker.trackFutureWhileIncomplete(future);

        assertThat(tracker.getCurrentIncompleteAndReset(), contains(future));
    }

    @Test
    public void testFutureRemovedAfterCompletion() {
        final IncompleteFuturesTracker tracker = new IncompleteFuturesTracker();
        final CompletableFuture<?> future = new CompletableFuture<>();

        tracker.trackFutureWhileIncomplete(future);
        future.complete(null);

        assertThat(tracker.getCurrentIncompleteAndReset(), not(contains(future)));
    }

    @Test
    public void testFutureNotAddedIfAlreadyCompleted() {
        final IncompleteFuturesTracker tracker = new IncompleteFuturesTracker();
        final CompletableFuture<?> future = new CompletableFuture<>();

        future.complete(null);
        tracker.trackFutureWhileIncomplete(future);

        assertThat(tracker.getCurrentIncompleteAndReset(), not(contains(future)));
    }

    @Test
    public void testFailFutures() throws Exception {
        final IncompleteFuturesTracker tracker = new IncompleteFuturesTracker();
        final CompletableFuture<?> future = new CompletableFuture<>();

        tracker.trackFutureWhileIncomplete(future);

        final Exception expectedException = new Exception();
        tracker.failAllFutures(expectedException);

        assertTrue(future.isCompletedExceptionally());
        try {
            future.get();
            fail();
        } catch (ExecutionException e) {
            assertSame(expectedException, e.getCause());
        }
    }

    @Test
    public void testFailFuturesImmediately() throws Exception {
        final IncompleteFuturesTracker tracker = new IncompleteFuturesTracker();
        final CompletableFuture<?> future = new CompletableFuture<>();

        final Exception expectedException = new Exception();
        tracker.failAllFutures(expectedException);

        tracker.trackFutureWhileIncomplete(future);

        assertTrue(future.isCompletedExceptionally());
        try {
            future.get();
            fail();
        } catch (ExecutionException e) {
            assertSame(expectedException, e.getCause());
        }
    }

    @Test
    public void testResetClearsTrackedFutures() {
        final IncompleteFuturesTracker tracker = new IncompleteFuturesTracker();

        final CompletableFuture<?> future = new CompletableFuture<>();
        tracker.trackFutureWhileIncomplete(future);
        tracker.getCurrentIncompleteAndReset();

        assertThat(tracker.getCurrentIncompleteAndReset(), empty());
    }
}
