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

import org.apache.flink.runtime.leaderelection.LeaderElectionService;
import org.apache.flink.runtime.testutils.TestingUtils;
import org.apache.flink.util.TestLogger;

import org.junit.Assert;
import org.junit.Test;

import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

import static org.hamcrest.Matchers.is;

/** Tests for the {@link EmbeddedLeaderService}. */
public class EmbeddedLeaderServiceTest extends TestLogger {

    /**
     * Tests that the {@link EmbeddedLeaderService} can handle a concurrent grant leadership call
     * and a shutdown.
     */
    @Test
    public void testConcurrentGrantLeadershipAndShutdown() throws Exception {
        final EmbeddedLeaderService embeddedLeaderService =
                new EmbeddedLeaderService(TestingUtils.defaultExecutor());

        try {
            final LeaderElectionService leaderElectionService =
                    embeddedLeaderService.createLeaderElectionService();

            final TestingLeaderContender contender = new TestingLeaderContender();

            leaderElectionService.start(contender);
            leaderElectionService.stop();

            try {
                // check that no exception occurred
                contender.getLeaderSessionFuture().get(10L, TimeUnit.MILLISECONDS);
            } catch (TimeoutException ignored) {
                // we haven't participated in the leader election
            }

            // the election service should still be running
            Assert.assertThat(embeddedLeaderService.isShutdown(), is(false));
        } finally {
            embeddedLeaderService.shutdown();
        }
    }

    /**
     * Tests that the {@link EmbeddedLeaderService} can handle a concurrent revoke leadership call
     * and a shutdown.
     */
    @Test
    public void testConcurrentRevokeLeadershipAndShutdown() throws Exception {
        final EmbeddedLeaderService embeddedLeaderService =
                new EmbeddedLeaderService(TestingUtils.defaultExecutor());

        try {
            final LeaderElectionService leaderElectionService =
                    embeddedLeaderService.createLeaderElectionService();

            final TestingLeaderContender contender = new TestingLeaderContender();

            leaderElectionService.start(contender);

            // wait for the leadership
            contender.getLeaderSessionFuture().get();

            final CompletableFuture<Void> revokeLeadershipFuture =
                    embeddedLeaderService.revokeLeadership();
            leaderElectionService.stop();

            try {
                // check that no exception occurred
                revokeLeadershipFuture.get(10L, TimeUnit.MILLISECONDS);
            } catch (TimeoutException ignored) {
                // the leader election service has been stopped before revoking could be executed
            }

            // the election service should still be running
            Assert.assertThat(embeddedLeaderService.isShutdown(), is(false));
        } finally {
            embeddedLeaderService.shutdown();
        }
    }
}
