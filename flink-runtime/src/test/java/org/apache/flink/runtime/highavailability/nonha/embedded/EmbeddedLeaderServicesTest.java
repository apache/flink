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

import org.apache.flink.api.common.JobID;
import org.apache.flink.runtime.leaderelection.LeaderElection;
import org.apache.flink.runtime.leaderelection.LeaderInformation;
import org.apache.flink.runtime.leaderretrieval.LeaderRetrievalService;
import org.apache.flink.runtime.util.LeaderRetrievalUtils;
import org.apache.flink.util.TestLogger;
import org.apache.flink.util.concurrent.Executors;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.util.UUID;

import static org.assertj.core.api.Assertions.assertThat;

/** Tests for the {@link EmbeddedLeaderServices}. */
class EmbeddedLeaderServicesTest extends TestLogger {

    private static final String ADDRESS = "foobar";

    private EmbeddedLeaderServices embeddedHaServices;

    @BeforeEach
    public void setupTest() {
        embeddedHaServices = new EmbeddedLeaderServices(Executors.directExecutor());
    }

    @AfterEach
    public void teardownTest() throws Exception {
        if (embeddedHaServices != null) {
            embeddedHaServices.close();
            embeddedHaServices = null;
        }
    }

    /** Tests that exactly one JobManager is elected as the leader for a given job id. */
    @Test
    public void testJobManagerLeaderElection() throws Exception {
        JobID jobId1 = new JobID();
        JobID jobId2 = new JobID();

        LeaderElection leaderElection1 = embeddedHaServices.getJobMasterLeaderElection(jobId1);
        TestingLeaderContender leaderContender1 = new TestingLeaderContender();
        leaderElection1.startLeaderElection(leaderContender1);

        LeaderElection leaderElection2 = embeddedHaServices.getJobMasterLeaderElection(jobId1);
        TestingLeaderContender leaderContender2 = new TestingLeaderContender();
        leaderElection2.startLeaderElection(leaderContender2);

        LeaderElection leaderElectionDifferentJobId =
                embeddedHaServices.getJobMasterLeaderElection(jobId2);
        TestingLeaderContender leaderContenderDifferentJobId = new TestingLeaderContender();
        leaderElectionDifferentJobId.startLeaderElection(leaderContenderDifferentJobId);

        assertThat(leaderContender1.getLeaderSessionFuture().isDone())
                .isNotEqualTo(leaderContender2.getLeaderSessionFuture().isDone());
        assertThat(leaderContenderDifferentJobId.getLeaderSessionFuture().isDone()).isTrue();
    }

    /** Tests that exactly one ResourceManager is elected as the leader. */
    @Test
    public void testResourceManagerLeaderElection() throws Exception {
        LeaderElection leaderElection1 = embeddedHaServices.getResourceManagerLeaderElection();
        TestingLeaderContender leaderContender1 = new TestingLeaderContender();
        leaderElection1.startLeaderElection(leaderContender1);

        LeaderElection leaderElection2 = embeddedHaServices.getResourceManagerLeaderElection();
        TestingLeaderContender leaderContender2 = new TestingLeaderContender();
        leaderElection2.startLeaderElection(leaderContender2);

        assertThat(leaderContender1.getLeaderSessionFuture().isDone())
                .isNotEqualTo(leaderContender2.getLeaderSessionFuture().isDone());
    }

    /** Tests the JobManager leader retrieval for a given job. */
    @Test
    public void testJobManagerLeaderRetrieval() throws Exception {
        JobID jobId = new JobID();

        LeaderElection leaderElection = embeddedHaServices.getJobMasterLeaderElection(jobId);
        LeaderRetrievalService leaderRetrievalService =
                embeddedHaServices.getJobMasterLeaderRetriever(jobId, "UNKNOWN");

        runLeaderRetrievalTest(leaderElection, leaderRetrievalService);
    }

    private void runLeaderRetrievalTest(
            LeaderElection leaderElection, LeaderRetrievalService leaderRetrievalService)
            throws Exception {
        LeaderRetrievalUtils.LeaderInformationListener leaderRetrievalListener =
                new LeaderRetrievalUtils.LeaderInformationListener();
        TestingLeaderContender leaderContender = new TestingLeaderContender();

        leaderRetrievalService.start(leaderRetrievalListener);
        leaderElection.startLeaderElection(leaderContender);

        final UUID leaderId = leaderContender.getLeaderSessionFuture().get();

        leaderElection.confirmLeadership(leaderId, ADDRESS);

        final LeaderInformation leaderInformation =
                leaderRetrievalListener.getLeaderInformationFuture().get();

        assertThat(leaderInformation.getLeaderAddress()).isEqualTo(ADDRESS);
        assertThat(leaderInformation.getLeaderSessionID()).isEqualTo(leaderId);
    }

    /** Tests the ResourceManager leader retrieval for a given job. */
    @Test
    public void testResourceManagerLeaderRetrieval() throws Exception {
        LeaderElection leaderElection = embeddedHaServices.getResourceManagerLeaderElection();
        LeaderRetrievalService leaderRetrievalService =
                embeddedHaServices.getResourceManagerLeaderRetriever();

        runLeaderRetrievalTest(leaderElection, leaderRetrievalService);
    }

    /**
     * Tests that concurrent leadership operations (granting and revoking) leadership leave the
     * system in a sane state.
     */
    @Test
    public void testConcurrentLeadershipOperations() throws Exception {
        final LeaderElection leaderElection = embeddedHaServices.getDispatcherLeaderElection();
        final TestingLeaderContender leaderContender = new TestingLeaderContender();

        leaderElection.startLeaderElection(leaderContender);

        final UUID oldLeaderSessionId = leaderContender.getLeaderSessionFuture().get();

        assertThat(leaderElection.hasLeadership(oldLeaderSessionId)).isEqualTo(true);

        embeddedHaServices.getDispatcherLeaderService().revokeLeadership().get();
        assertThat(leaderElection.hasLeadership(oldLeaderSessionId)).isEqualTo(false);

        embeddedHaServices.getDispatcherLeaderService().grantLeadership();
        final UUID newLeaderSessionId = leaderContender.getLeaderSessionFuture().get();

        assertThat(leaderElection.hasLeadership(newLeaderSessionId)).isEqualTo(true);

        leaderElection.confirmLeadership(oldLeaderSessionId, ADDRESS);
        leaderElection.confirmLeadership(newLeaderSessionId, ADDRESS);

        assertThat(leaderElection.hasLeadership(newLeaderSessionId)).isEqualTo(true);

        leaderContender.tryRethrowException();
    }
}
