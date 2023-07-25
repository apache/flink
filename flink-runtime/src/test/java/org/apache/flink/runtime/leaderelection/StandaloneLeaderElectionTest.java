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

package org.apache.flink.runtime.leaderelection;

import org.apache.flink.runtime.leaderretrieval.StandaloneLeaderRetrievalService;

import org.junit.jupiter.api.Test;

import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.atomic.AtomicBoolean;

import static org.assertj.core.api.Assertions.assertThat;

class StandaloneLeaderElectionTest {

    private static final UUID SESSION_ID = UUID.randomUUID();

    private static final String TEST_URL = "pekko://users/jobmanager";

    /**
     * Tests that the standalone leader election and retrieval service return the same leader URL.
     */
    @Test
    void testStandaloneLeaderElectionRetrieval() throws Exception {
        final UUID expectedSessionID = UUID.randomUUID();
        StandaloneLeaderRetrievalService leaderRetrievalService =
                new StandaloneLeaderRetrievalService(TEST_URL, expectedSessionID);
        TestingListener testingListener = new TestingListener();

        try (LeaderElection leaderElection = new StandaloneLeaderElection(expectedSessionID)) {
            TestingContender contender = new TestingContender(TEST_URL, leaderElection);
            contender.startLeaderElection();

            leaderRetrievalService.start(testingListener);

            contender.waitForLeader();

            assertThat(contender.isLeader()).isTrue();
            assertThat(contender.getLeaderSessionID()).isEqualTo(expectedSessionID);

            testingListener.waitForNewLeader();

            assertThat(testingListener.getAddress()).isEqualTo(TEST_URL);
            assertThat(testingListener.getLeaderSessionID()).isEqualTo(expectedSessionID);
        } finally {
            leaderRetrievalService.stop();
        }
    }

    @Test
    void testStartLeaderElection() throws Exception {
        final CompletableFuture<UUID> grantLeadershipResult = new CompletableFuture<>();
        final TestingGenericLeaderContender contender =
                TestingGenericLeaderContender.newBuilder()
                        .setGrantLeadershipConsumer(grantLeadershipResult::complete)
                        .build();
        try (final LeaderElection testInstance = new StandaloneLeaderElection(SESSION_ID)) {
            testInstance.startLeaderElection(contender);

            assertThat(grantLeadershipResult).isCompletedWithValue(SESSION_ID);
        }
    }

    @Test
    void testHasLeadershipWithContender() throws Exception {
        final TestingGenericLeaderContender contender =
                TestingGenericLeaderContender.newBuilder().build();
        try (final LeaderElection testInstance = new StandaloneLeaderElection(SESSION_ID)) {
            testInstance.startLeaderElection(contender);

            assertThat(testInstance.hasLeadership(SESSION_ID)).isTrue();

            final UUID differentSessionID = UUID.randomUUID();
            assertThat(testInstance.hasLeadership(differentSessionID)).isFalse();
        }
    }

    @Test
    void testHasLeadershipWithoutContender() throws Exception {
        try (final LeaderElection testInstance = new StandaloneLeaderElection(SESSION_ID)) {
            assertThat(testInstance.hasLeadership(SESSION_ID)).isFalse();

            final UUID differentSessionID = UUID.randomUUID();
            assertThat(testInstance.hasLeadership(differentSessionID)).isFalse();
        }
    }

    @Test
    void testRevokeCallOnClose() throws Exception {
        final AtomicBoolean revokeLeadershipCalled = new AtomicBoolean(false);
        final TestingGenericLeaderContender contender =
                TestingGenericLeaderContender.newBuilder()
                        .setRevokeLeadershipRunnable(() -> revokeLeadershipCalled.set(true))
                        .build();
        try (final LeaderElection testInstance = new StandaloneLeaderElection(SESSION_ID)) {
            testInstance.startLeaderElection(contender);
        }

        assertThat(revokeLeadershipCalled).isTrue();
    }
}
