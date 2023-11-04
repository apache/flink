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
import org.apache.flink.runtime.leaderretrieval.LeaderRetrievalService;
import org.apache.flink.runtime.util.LeaderRetrievalUtils;
import org.apache.flink.util.TestLogger;
import org.apache.flink.util.concurrent.Executors;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.mockito.ArgumentCaptor;

import java.util.UUID;

import static junit.framework.TestCase.assertTrue;
import static org.hamcrest.Matchers.is;
import static org.junit.Assert.assertThat;
import static org.mockito.Matchers.any;
import static org.mockito.Mockito.atLeast;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;

/** Tests for the {@link EmbeddedHaServices}. */
public class EmbeddedHaServicesTest extends TestLogger {

    private static final String ADDRESS = "foobar";

    private EmbeddedHaServices embeddedHaServices;

    @Before
    public void setupTest() {
        embeddedHaServices = new EmbeddedHaServices(Executors.directExecutor());
    }

    @After
    public void teardownTest() throws Exception {
        if (embeddedHaServices != null) {
            embeddedHaServices.closeWithOptionalClean(true);
            embeddedHaServices = null;
        }
    }

    /** Tests that exactly one JobManager is elected as the leader for a given job id. */
    @Test
    public void testJobManagerLeaderElection() throws Exception {
        JobID jobId1 = new JobID();
        JobID jobId2 = new JobID();

        LeaderContender leaderContender1 = mock(LeaderContender.class);
        LeaderContender leaderContender2 = mock(LeaderContender.class);
        LeaderContender leaderContenderDifferentJobId = mock(LeaderContender.class);

        LeaderElection leaderElection1 = embeddedHaServices.getJobManagerLeaderElection(jobId1);
        leaderElection1.startLeaderElection(leaderContender1);

        LeaderElection leaderElection2 = embeddedHaServices.getJobManagerLeaderElection(jobId1);
        leaderElection2.startLeaderElection(leaderContender2);

        LeaderElection leaderElectionDifferentJobId =
                embeddedHaServices.getJobManagerLeaderElection(jobId2);
        leaderElectionDifferentJobId.startLeaderElection(leaderContenderDifferentJobId);

        ArgumentCaptor<UUID> leaderIdArgumentCaptor1 = ArgumentCaptor.forClass(UUID.class);
        ArgumentCaptor<UUID> leaderIdArgumentCaptor2 = ArgumentCaptor.forClass(UUID.class);
        verify(leaderContender1, atLeast(0)).grantLeadership(leaderIdArgumentCaptor1.capture());
        verify(leaderContender2, atLeast(0)).grantLeadership(leaderIdArgumentCaptor2.capture());

        assertTrue(
                leaderIdArgumentCaptor1.getAllValues().isEmpty()
                        ^ leaderIdArgumentCaptor2.getAllValues().isEmpty());

        verify(leaderContenderDifferentJobId).grantLeadership(any(UUID.class));
    }

    /** Tests that exactly one ResourceManager is elected as the leader. */
    @Test
    public void testResourceManagerLeaderElection() throws Exception {
        LeaderContender leaderContender1 = mock(LeaderContender.class);
        LeaderContender leaderContender2 = mock(LeaderContender.class);

        LeaderElection leaderElection1 = embeddedHaServices.getResourceManagerLeaderElection();
        leaderElection1.startLeaderElection(leaderContender1);

        LeaderElection leaderElection2 = embeddedHaServices.getResourceManagerLeaderElection();
        leaderElection2.startLeaderElection(leaderContender2);

        ArgumentCaptor<UUID> leaderIdArgumentCaptor1 = ArgumentCaptor.forClass(UUID.class);
        ArgumentCaptor<UUID> leaderIdArgumentCaptor2 = ArgumentCaptor.forClass(UUID.class);
        verify(leaderContender1, atLeast(0)).grantLeadership(leaderIdArgumentCaptor1.capture());
        verify(leaderContender2, atLeast(0)).grantLeadership(leaderIdArgumentCaptor2.capture());

        assertTrue(
                leaderIdArgumentCaptor1.getAllValues().isEmpty()
                        ^ leaderIdArgumentCaptor2.getAllValues().isEmpty());
    }

    /** Tests the JobManager leader retrieval for a given job. */
    @Test
    public void testJobManagerLeaderRetrieval() throws Exception {
        JobID jobId = new JobID();

        LeaderElection leaderElection = embeddedHaServices.getJobManagerLeaderElection(jobId);
        LeaderRetrievalService leaderRetrievalService =
                embeddedHaServices.getJobManagerLeaderRetriever(jobId);

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

        assertThat(leaderInformation.getLeaderAddress(), is(ADDRESS));
        assertThat(leaderInformation.getLeaderSessionID(), is(leaderId));
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

        assertThat(leaderElection.hasLeadership(oldLeaderSessionId), is(true));

        embeddedHaServices.getDispatcherLeaderService().revokeLeadership().get();
        assertThat(leaderElection.hasLeadership(oldLeaderSessionId), is(false));

        embeddedHaServices.getDispatcherLeaderService().grantLeadership();
        final UUID newLeaderSessionId = leaderContender.getLeaderSessionFuture().get();

        assertThat(leaderElection.hasLeadership(newLeaderSessionId), is(true));

        leaderElection.confirmLeadership(oldLeaderSessionId, ADDRESS);
        leaderElection.confirmLeadership(newLeaderSessionId, ADDRESS);

        assertThat(leaderElection.hasLeadership(newLeaderSessionId), is(true));

        leaderContender.tryRethrowException();
    }
}
