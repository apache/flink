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
import org.apache.flink.util.TestLogger;

import org.junit.Assert;
import org.junit.Test;

import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

import static org.hamcrest.Matchers.is;
import static org.junit.Assert.fail;

/** Tests for the {@link EmbeddedLeaderService}. */
public class EmbeddedLeaderServiceTest extends TestLogger {

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

        try {
            final TestingLeaderContender contender = new TestingLeaderContender();

            final LeaderElection leaderElection =
                    embeddedLeaderService.createLeaderElectionService("contender_id");
            leaderElection.startLeaderElection(contender);
            leaderElection.close();

            try {
                // check that no exception occurred
                contender.getLeaderSessionFuture().get(10L, TimeUnit.MILLISECONDS);
                fail("The future shouldn't have completed.");
            } catch (TimeoutException ignored) {
                // we haven't participated in the leader election
            }

            // the election service should still be running
            Assert.assertThat(embeddedLeaderService.isShutdown(), is(false));
        } finally {
            embeddedLeaderService.shutdown();

            // triggers the grant event processing after shutdown
            executorService.triggerAll();
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

        try {
            final TestingLeaderContender contender = new TestingLeaderContender();

            final LeaderElection leaderElection =
                    embeddedLeaderService.createLeaderElectionService("contender_id");
            leaderElection.startLeaderElection(contender);

            // wait for the leadership
            executorService.trigger();
            contender.getLeaderSessionFuture().get();

            final CompletableFuture<Void> revokeLeadershipFuture =
                    embeddedLeaderService.revokeLeadership();
            leaderElection.close();

            try {
                // check that no exception occurred
                revokeLeadershipFuture.get(10L, TimeUnit.MILLISECONDS);
                fail("The future shouldn't have completed.");
            } catch (TimeoutException ignored) {
                // the leader election service has been stopped before revoking could be executed
            }

            // the election service should still be running
            Assert.assertThat(embeddedLeaderService.isShutdown(), is(false));
        } finally {
            embeddedLeaderService.shutdown();

            // triggers the revoke event processing after shutdown
            executorService.triggerAll();
        }
    }
}
