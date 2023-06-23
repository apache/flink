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

package org.apache.flink.runtime.highavailability.nonha.embedded;

import org.apache.flink.runtime.concurrent.ManuallyTriggeredScheduledExecutorService;
import org.apache.flink.runtime.leaderelection.LeaderElection;
import org.apache.flink.runtime.leaderelection.LeaderElectionEvent;
import org.apache.flink.runtime.leaderelection.TestingGenericLeaderContender;
import org.apache.flink.runtime.util.TestingFatalErrorHandlerExtension;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.RegisterExtension;

import java.util.LinkedList;
import java.util.Queue;
import java.util.concurrent.CompletableFuture;

import static org.assertj.core.api.Assertions.assertThat;

/** Tests for the {@link EmbeddedLeaderService}. */
class EmbeddedLeaderServiceTest {

    @RegisterExtension
    public final TestingFatalErrorHandlerExtension errorHandlerExtension =
            new TestingFatalErrorHandlerExtension();

    /**
     * Tests that the {@link EmbeddedLeaderService} can handle a concurrent grant leadership call
     * and a shutdown.
     */
    @Test
    public void testConcurrentGrantLeadershipAndShutdown() throws Exception {
        final ManuallyTriggeredScheduledExecutorService executorService =
                new ManuallyTriggeredScheduledExecutorService();
        final EmbeddedLeaderService embeddedLeaderService =
                new EmbeddedLeaderService(executorService);

        final Queue<LeaderElectionEvent> eventQueue = new LinkedList<>();
        try {
            final LeaderElection leaderElection =
                    embeddedLeaderService.createLeaderElectionService("contender_id");

            leaderElection.startLeaderElection(
                    TestingGenericLeaderContender.newBuilder(
                                    eventQueue,
                                    leaderElection,
                                    "unused-address",
                                    errorHandlerExtension.getTestingFatalErrorHandler()
                                            ::onFatalError)
                            .build());
            leaderElection.close();

            assertThat(eventQueue.remove().isNotLeaderEvent())
                    .as("The close call should have triggered the leadership revocation.")
                    .isTrue();

            // the election service should still be running
            assertThat(embeddedLeaderService.isShutdown()).isFalse();
        } finally {
            embeddedLeaderService.shutdown();

            // triggers the grant event processing after shutdown
            executorService.triggerAll();
            assertThat(eventQueue.remove().isIsLeaderEvent()).isTrue();
            assertThat(eventQueue).isEmpty();
        }
    }

    /**
     * Tests that the {@link EmbeddedLeaderService} can handle a concurrent revoke leadership call
     * and a shutdown.
     */
    @Test
    public void testConcurrentRevokeLeadershipAndShutdown() throws Exception {
        final ManuallyTriggeredScheduledExecutorService executorService =
                new ManuallyTriggeredScheduledExecutorService();
        final EmbeddedLeaderService embeddedLeaderService =
                new EmbeddedLeaderService(executorService);

        final Queue<LeaderElectionEvent> eventQueue = new LinkedList<>();
        try {
            final LeaderElection leaderElection =
                    embeddedLeaderService.createLeaderElectionService("contender_id");
            leaderElection.startLeaderElection(
                    TestingGenericLeaderContender.newBuilder(
                                    eventQueue,
                                    leaderElection,
                                    "unused-address",
                                    errorHandlerExtension.getTestingFatalErrorHandler()
                                            ::onFatalError)
                            .build());

            // wait for the leadership
            executorService.trigger();
            assertThat(eventQueue.remove().isIsLeaderEvent()).isTrue();

            final CompletableFuture<Void> revokeLeadershipFuture =
                    embeddedLeaderService.revokeLeadership();
            leaderElection.close();

            assertThat(revokeLeadershipFuture)
                    .as(
                            "The future shouldn't have completed because the revoke event wasn't processed, yet.")
                    .isNotDone();
            assertThat(eventQueue).as("No revoke event should have been sent, yet.").isEmpty();

            // the election service should still be running
            assertThat(embeddedLeaderService.isShutdown()).isFalse();
        } finally {
            embeddedLeaderService.shutdown();

            // triggers the revoke event processing after shutdown
            executorService.triggerAll();
            assertThat(eventQueue.remove().isNotLeaderEvent())
                    .as("The event is handled and the contender should deal with it properly.")
                    .isTrue();
        }
    }
}
