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
import org.apache.flink.runtime.concurrent.Executors;
import org.apache.flink.runtime.leaderelection.LeaderContender;
import org.apache.flink.runtime.leaderelection.LeaderElectionService;
import org.apache.flink.runtime.leaderretrieval.LeaderRetrievalService;
import org.apache.flink.runtime.util.LeaderConnectionInfo;
import org.apache.flink.runtime.util.LeaderRetrievalUtils;
import org.apache.flink.util.TestLogger;

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
            embeddedHaServices.closeAndCleanupAllData();
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

        LeaderElectionService leaderElectionService1 =
                embeddedHaServices.getJobManagerLeaderElectionService(jobId1);
        LeaderElectionService leaderElectionService2 =
                embeddedHaServices.getJobManagerLeaderElectionService(jobId1);
        LeaderElectionService leaderElectionServiceDifferentJobId =
                embeddedHaServices.getJobManagerLeaderElectionService(jobId2);

        leaderElectionService1.start(leaderContender1);
        leaderElectionService2.start(leaderContender2);
        leaderElectionServiceDifferentJobId.start(leaderContenderDifferentJobId);

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

        LeaderElectionService leaderElectionService1 =
                embeddedHaServices.getResourceManagerLeaderElectionService();
        LeaderElectionService leaderElectionService2 =
                embeddedHaServices.getResourceManagerLeaderElectionService();

        leaderElectionService1.start(leaderContender1);
        leaderElectionService2.start(leaderContender2);

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

        LeaderElectionService leaderElectionService =
                embeddedHaServices.getJobManagerLeaderElectionService(jobId);
        LeaderRetrievalService leaderRetrievalService =
                embeddedHaServices.getJobManagerLeaderRetriever(jobId);

        runLeaderRetrievalTest(leaderElectionService, leaderRetrievalService);
    }

    private void runLeaderRetrievalTest(
            LeaderElectionService leaderElectionService,
            LeaderRetrievalService leaderRetrievalService)
            throws Exception {
        LeaderRetrievalUtils.LeaderConnectionInfoListener leaderRetrievalListener =
                new LeaderRetrievalUtils.LeaderConnectionInfoListener();
        TestingLeaderContender leaderContender = new TestingLeaderContender();

        leaderRetrievalService.start(leaderRetrievalListener);
        leaderElectionService.start(leaderContender);

        final UUID leaderId = leaderContender.getLeaderSessionFuture().get();

        leaderElectionService.confirmLeadership(leaderId, ADDRESS);

        final LeaderConnectionInfo leaderConnectionInfo =
                leaderRetrievalListener.getLeaderConnectionInfoFuture().get();

        assertThat(leaderConnectionInfo.getAddress(), is(ADDRESS));
        assertThat(leaderConnectionInfo.getLeaderSessionId(), is(leaderId));
    }

    /** Tests the ResourceManager leader retrieval for a given job. */
    @Test
    public void testResourceManagerLeaderRetrieval() throws Exception {
        LeaderElectionService leaderElectionService =
                embeddedHaServices.getResourceManagerLeaderElectionService();
        LeaderRetrievalService leaderRetrievalService =
                embeddedHaServices.getResourceManagerLeaderRetriever();

        runLeaderRetrievalTest(leaderElectionService, leaderRetrievalService);
    }

    /**
     * Tests that concurrent leadership operations (granting and revoking) leadership leave the
     * system in a sane state.
     */
    @Test
    public void testConcurrentLeadershipOperations() throws Exception {
        final LeaderElectionService dispatcherLeaderElectionService =
                embeddedHaServices.getDispatcherLeaderElectionService();
        final TestingLeaderContender leaderContender = new TestingLeaderContender();

        dispatcherLeaderElectionService.start(leaderContender);

        final UUID oldLeaderSessionId = leaderContender.getLeaderSessionFuture().get();

        assertThat(dispatcherLeaderElectionService.hasLeadership(oldLeaderSessionId), is(true));

        embeddedHaServices.getDispatcherLeaderService().revokeLeadership().get();
        assertThat(dispatcherLeaderElectionService.hasLeadership(oldLeaderSessionId), is(false));

        embeddedHaServices.getDispatcherLeaderService().grantLeadership();
        final UUID newLeaderSessionId = leaderContender.getLeaderSessionFuture().get();

        assertThat(dispatcherLeaderElectionService.hasLeadership(newLeaderSessionId), is(true));

        dispatcherLeaderElectionService.confirmLeadership(oldLeaderSessionId, ADDRESS);
        dispatcherLeaderElectionService.confirmLeadership(newLeaderSessionId, ADDRESS);

        assertThat(dispatcherLeaderElectionService.hasLeadership(newLeaderSessionId), is(true));

        leaderContender.tryRethrowException();
    }
}
