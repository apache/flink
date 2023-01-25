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

import org.apache.flink.runtime.highavailability.HighAvailabilityServices;
import org.apache.flink.runtime.leaderretrieval.StandaloneLeaderRetrievalService;

import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;

class StandaloneLeaderElectionTest {

    private static final String TEST_URL = "akka://users/jobmanager";

    /**
     * Tests that the standalone leader election and retrieval service return the same leader URL.
     */
    @Test
    void testStandaloneLeaderElectionRetrieval() throws Exception {
        StandaloneLeaderElectionService leaderElectionService =
                new StandaloneLeaderElectionService();
        StandaloneLeaderRetrievalService leaderRetrievalService =
                new StandaloneLeaderRetrievalService(
                        TEST_URL, HighAvailabilityServices.DEFAULT_LEADER_ID);
        TestingContender contender = new TestingContender(TEST_URL, leaderElectionService);
        TestingListener testingListener = new TestingListener();

        try (LeaderElection leaderElection = contender.startLeaderElection()) {
            leaderRetrievalService.start(testingListener);

            contender.waitForLeader();

            assertThat(contender.isLeader()).isTrue();
            assertThat(contender.getLeaderSessionID())
                    .isEqualTo(HighAvailabilityServices.DEFAULT_LEADER_ID);

            testingListener.waitForNewLeader();

            assertThat(testingListener.getAddress()).isEqualTo(TEST_URL);
            assertThat(testingListener.getLeaderSessionID())
                    .isEqualTo(HighAvailabilityServices.DEFAULT_LEADER_ID);
        } finally {
            leaderRetrievalService.stop();
        }
    }
}
