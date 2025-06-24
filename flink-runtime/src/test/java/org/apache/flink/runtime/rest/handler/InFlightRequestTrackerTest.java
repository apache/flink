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

package org.apache.flink.runtime.rest.handler;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.util.concurrent.CompletableFuture;

import static org.assertj.core.api.Assertions.assertThat;

/** Tests for {@link InFlightRequestTracker}. */
class InFlightRequestTrackerTest {

    private InFlightRequestTracker inFlightRequestTracker;

    @BeforeEach
    void setUp() {
        inFlightRequestTracker = new InFlightRequestTracker();
    }

    @Test
    void testShouldFinishAwaitAsyncImmediatelyIfNoRequests() {
        assertThat(inFlightRequestTracker.awaitAsync()).isDone();
    }

    @Test
    void testShouldFinishAwaitAsyncIffAllRequestsDeregistered() {
        inFlightRequestTracker.registerRequest();

        final CompletableFuture<Void> awaitFuture = inFlightRequestTracker.awaitAsync();
        assertThat(awaitFuture).isNotDone();

        inFlightRequestTracker.deregisterRequest();
        assertThat(awaitFuture).isDone();
    }

    @Test
    void testAwaitAsyncIsIdempotent() {
        final CompletableFuture<Void> awaitFuture = inFlightRequestTracker.awaitAsync();

        assertThat(awaitFuture).isDone();
        assertThat(awaitFuture)
                .as("The reference to the future must not change")
                .isSameAs(inFlightRequestTracker.awaitAsync());
    }

    @Test
    void testShouldTolerateRegisterAfterAwaitAsync() {
        final CompletableFuture<Void> awaitFuture = inFlightRequestTracker.awaitAsync();
        assertThat(awaitFuture).isDone();

        inFlightRequestTracker.registerRequest();

        assertThat(awaitFuture)
                .as("The reference to the future must not change")
                .isSameAs(inFlightRequestTracker.awaitAsync());
    }

    @Test
    void testShouldNotRegisterNewRequestsAfterTermination() {
        final CompletableFuture<Void> terminationFuture = inFlightRequestTracker.awaitAsync();

        assertThat(terminationFuture).isDone();
        assertThat(inFlightRequestTracker.registerRequest()).isFalse();
    }
}
