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

package org.apache.flink.runtime.highavailability.nonha.standalone;

import org.apache.flink.api.common.JobID;
import org.apache.flink.runtime.highavailability.HighAvailabilityServices;
import org.apache.flink.runtime.leaderelection.LeaderContender;
import org.apache.flink.runtime.leaderelection.LeaderElection;
import org.apache.flink.runtime.leaderretrieval.LeaderRetrievalListener;
import org.apache.flink.runtime.leaderretrieval.LeaderRetrievalService;
import org.apache.flink.util.TestLogger;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import static org.mockito.Matchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;

/** Tests for the {@link StandaloneHaServices}. */
public class StandaloneHaServicesTest extends TestLogger {

    private final String dispatcherAddress = "dispatcher";
    private final String resourceManagerAddress = "resourceManager";
    private final String webMonitorAddress = "webMonitor";

    private StandaloneHaServices standaloneHaServices;

    @Before
    public void setupTest() {

        standaloneHaServices =
                new StandaloneHaServices(
                        resourceManagerAddress, dispatcherAddress, webMonitorAddress);
    }

    @After
    public void teardownTest() throws Exception {
        if (standaloneHaServices != null) {
            standaloneHaServices.closeWithOptionalClean(true);
            standaloneHaServices = null;
        }
    }

    /**
     * Tests that the standalone leader election services return a fixed address and leader session
     * id.
     */
    @Test
    public void testLeaderElection() throws Exception {
        JobID jobId = new JobID();
        LeaderContender jmLeaderContender = mock(LeaderContender.class);
        LeaderContender rmLeaderContender = mock(LeaderContender.class);

        final LeaderElection jmLeaderElection =
                standaloneHaServices.getJobManagerLeaderElection(jobId);
        jmLeaderElection.startLeaderElection(jmLeaderContender);

        final LeaderElection rmLeaderElection =
                standaloneHaServices.getResourceManagerLeaderElection();
        rmLeaderElection.startLeaderElection(rmLeaderContender);

        verify(jmLeaderContender).grantLeadership(eq(HighAvailabilityServices.DEFAULT_LEADER_ID));
        verify(rmLeaderContender).grantLeadership(eq(HighAvailabilityServices.DEFAULT_LEADER_ID));
    }

    /**
     * Tests that the standalone leader retrieval services return the specified address and the
     * fixed leader session id.
     */
    @Test
    public void testResourceManagerLeaderRetrieval() throws Exception {
        LeaderRetrievalListener rmListener1 = mock(LeaderRetrievalListener.class);
        LeaderRetrievalListener rmListener2 = mock(LeaderRetrievalListener.class);

        LeaderRetrievalService rmLeaderRetrievalService1 =
                standaloneHaServices.getResourceManagerLeaderRetriever();
        LeaderRetrievalService rmLeaderRetrievalService2 =
                standaloneHaServices.getResourceManagerLeaderRetriever();

        rmLeaderRetrievalService1.start(rmListener1);
        rmLeaderRetrievalService2.start(rmListener2);

        verify(rmListener1)
                .notifyLeaderAddress(
                        eq(resourceManagerAddress), eq(HighAvailabilityServices.DEFAULT_LEADER_ID));
        verify(rmListener2)
                .notifyLeaderAddress(
                        eq(resourceManagerAddress), eq(HighAvailabilityServices.DEFAULT_LEADER_ID));
    }
}
