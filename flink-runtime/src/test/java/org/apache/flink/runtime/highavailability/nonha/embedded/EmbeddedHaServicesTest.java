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
import org.apache.flink.runtime.leaderelection.LeaderContender;
import org.apache.flink.runtime.leaderelection.LeaderElection;
import org.apache.flink.runtime.leaderelection.LeaderInformation;
import org.apache.flink.runtime.leaderelection.TestingGenericLeaderContender;
import org.apache.flink.runtime.leaderretrieval.LeaderRetrievalService;
import org.apache.flink.runtime.util.LeaderRetrievalUtils;
import org.apache.flink.runtime.util.TestingFatalErrorHandlerExtension;
import org.apache.flink.util.concurrent.Executors;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.RegisterExtension;

import java.util.ArrayList;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.atomic.AtomicReference;

import static org.assertj.core.api.Assertions.assertThat;

class EmbeddedHaServicesTest {

    @RegisterExtension
    final TestingFatalErrorHandlerExtension testingFatalErrorHandlerExtension =
            new TestingFatalErrorHandlerExtension();

    private static final String ADDRESS = "foobar";

    private EmbeddedHaServices embeddedHaServices;

    @BeforeEach
    void setupTest() {
        embeddedHaServices = new EmbeddedHaServices(Executors.directExecutor());
    }

    @AfterEach
    void teardownTest() throws Exception {
        if (embeddedHaServices != null) {
            embeddedHaServices.closeWithOptionalClean(true);
            embeddedHaServices = null;
        }
    }

    /** Tests that exactly one JobManager is elected as the leader for a given job id. */
    @Test
    void testJobManagerLeaderElection() throws Exception {
        final JobID jobId1 = new JobID();
        final JobID jobId2 = new JobID();

        final List<UUID> sessionIDsForJob1 = new ArrayList<>();
        final TestingGenericLeaderContender.Builder contenderBuilder =
                TestingGenericLeaderContender.newBuilder()
                        .setGrantLeadershipConsumer(sessionIDsForJob1::add);
        final LeaderContender leaderContender1 = contenderBuilder.build();
        final LeaderContender leaderContender2 = contenderBuilder.build();

        final List<UUID> sessionIDsForJob2 = new ArrayList<>();
        final LeaderContender leaderContenderDifferentJobId =
                contenderBuilder.setGrantLeadershipConsumer(sessionIDsForJob2::add).build();

        final LeaderElection leaderElection1 =
                embeddedHaServices.getJobManagerLeaderElection(jobId1);
        leaderElection1.startLeaderElection(leaderContender1);

        final LeaderElection leaderElection2 =
                embeddedHaServices.getJobManagerLeaderElection(jobId1);
        leaderElection2.startLeaderElection(leaderContender2);

        final LeaderElection leaderElectionDifferentJobId =
                embeddedHaServices.getJobManagerLeaderElection(jobId2);
        leaderElectionDifferentJobId.startLeaderElection(leaderContenderDifferentJobId);

        assertThat(sessionIDsForJob1).hasSize(1);
        assertThat(sessionIDsForJob2).hasSize(1);

        assertThat(sessionIDsForJob1).isNotEqualTo(sessionIDsForJob2);
    }

    /** Tests that exactly one ResourceManager is elected as the leader. */
    @Test
    void testResourceManagerLeaderElection() throws Exception {
        final List<UUID> grantedSessionIDs = new ArrayList<>();
        final LeaderContender leaderContender1 =
                TestingGenericLeaderContender.newBuilder()
                        .setGrantLeadershipConsumer(grantedSessionIDs::add)
                        .build();
        final LeaderContender leaderContender2 =
                TestingGenericLeaderContender.newBuilder()
                        .setGrantLeadershipConsumer(grantedSessionIDs::add)
                        .build();

        final LeaderElection leaderElection1 =
                embeddedHaServices.getResourceManagerLeaderElection();
        leaderElection1.startLeaderElection(leaderContender1);

        final LeaderElection leaderElection2 =
                embeddedHaServices.getResourceManagerLeaderElection();
        leaderElection2.startLeaderElection(leaderContender2);

        assertThat(grantedSessionIDs).hasSize(1);
    }

    /** Tests the JobManager leader retrieval for a given job. */
    @Test
    void testJobManagerLeaderRetrieval() throws Exception {
        JobID jobId = new JobID();

        LeaderElection leaderElection = embeddedHaServices.getJobManagerLeaderElection(jobId);
        LeaderRetrievalService leaderRetrievalService =
                embeddedHaServices.getJobManagerLeaderRetriever(jobId);

        runLeaderRetrievalTest(leaderElection, leaderRetrievalService);
    }

    private void runLeaderRetrievalTest(
            LeaderElection leaderElection, LeaderRetrievalService leaderRetrievalService)
            throws Exception {
        final CompletableFuture<UUID> sessionGrantedFuture = new CompletableFuture<>();
        final LeaderRetrievalUtils.LeaderInformationListener leaderRetrievalListener =
                new LeaderRetrievalUtils.LeaderInformationListener();
        final LeaderContender leaderContender =
                TestingGenericLeaderContender.newBuilder()
                        .setGrantLeadershipConsumer(sessionGrantedFuture::complete)
                        .build();

        leaderRetrievalService.start(leaderRetrievalListener);
        leaderElection.startLeaderElection(leaderContender);

        final UUID leaderId = sessionGrantedFuture.get();

        leaderElection.confirmLeadership(leaderId, ADDRESS);

        final LeaderInformation leaderInformation =
                leaderRetrievalListener.getLeaderInformationFuture().get();

        assertThat(leaderInformation.getLeaderAddress()).isEqualTo(ADDRESS);
        assertThat(leaderInformation.getLeaderSessionID()).isEqualTo(leaderId);
    }

    /** Tests the ResourceManager leader retrieval for a given job. */
    @Test
    void testResourceManagerLeaderRetrieval() throws Exception {
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
    void testConcurrentLeadershipOperations() throws Exception {
        final AtomicReference<UUID> contenderSessionID = new AtomicReference<>();
        final LeaderContender leaderContender =
                TestingGenericLeaderContender.newBuilder()
                        .setGrantLeadershipConsumer(contenderSessionID::set)
                        .setRevokeLeadershipRunnable(() -> contenderSessionID.set(null))
                        .setHandleErrorConsumer(testingFatalErrorHandlerExtension)
                        .build();
        final LeaderElection leaderElection = embeddedHaServices.getDispatcherLeaderElection();
        final EmbeddedLeaderService dispatcherLeaderService =
                embeddedHaServices.getDispatcherLeaderService();

        assertThat(dispatcherLeaderService.getCurrentLeaderSessionID()).isNull();
        assertThat(dispatcherLeaderService.getCurrentLeaderAddress()).isNull();
        assertThat(contenderSessionID.get())
                .as("The contender shouldn't have leadership initially.")
                .isEqualTo(null);
        leaderElection.startLeaderElection(leaderContender);

        final UUID initialLeaderSessionId = dispatcherLeaderService.getCurrentLeaderSessionID();
        assertThat(initialLeaderSessionId)
                .as("The contender should have acquired leadership right away.")
                .isNotNull();
        assertThat(contenderSessionID.get()).isEqualTo(initialLeaderSessionId);
        assertThat(dispatcherLeaderService.getCurrentLeaderAddress())
                .as("But the leadership shouldn't have been confirmed, yet.")
                .isNull();

        dispatcherLeaderService.revokeLeadership().get();
        assertThat(dispatcherLeaderService.getCurrentLeaderSessionID()).isNull();
        assertThat(dispatcherLeaderService.getCurrentLeaderAddress()).isNull();
        assertThat(contenderSessionID.get()).isNull();

        dispatcherLeaderService.grantLeadership();
        final UUID newLeaderSessionId = dispatcherLeaderService.getCurrentLeaderSessionID();
        assertThat(newLeaderSessionId)
                .as("The contender should have acquired leadership again.")
                .isNotNull();
        assertThat(contenderSessionID.get()).isEqualTo(newLeaderSessionId);
        assertThat(dispatcherLeaderService.getCurrentLeaderAddress())
                .as("But the leadership shouldn't have been confirmed, yet.")
                .isNull();

        leaderElection.confirmLeadership(initialLeaderSessionId, ADDRESS);
        leaderElection.confirmLeadership(newLeaderSessionId, ADDRESS);

        assertThat(dispatcherLeaderService.getCurrentLeaderSessionID())
                .isEqualTo(newLeaderSessionId);
    }
}
