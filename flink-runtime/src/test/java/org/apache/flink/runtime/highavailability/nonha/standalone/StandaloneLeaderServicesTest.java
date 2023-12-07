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
import org.apache.flink.runtime.leaderelection.LeaderElection;
import org.apache.flink.runtime.leaderelection.TestingContender;
import org.apache.flink.runtime.leaderelection.TestingListener;
import org.apache.flink.runtime.leaderretrieval.LeaderRetrievalService;
import org.apache.flink.util.TestLogger;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;

/** Tests for the {@link StandaloneLeaderServices}. */
class StandaloneLeaderServicesTest extends TestLogger {

    private final String dispatcherAddress = "dispatcher";
    private final String resourceManagerAddress = "resourceManager";
    private final String webMonitorAddress = "webMonitor";

    private StandaloneLeaderServices standaloneLeaderServices;

    @BeforeEach
    public void setupTest() {
        standaloneLeaderServices =
                new StandaloneLeaderServices(
                        resourceManagerAddress, dispatcherAddress, webMonitorAddress);
    }

    @AfterEach
    public void teardownTest() throws Exception {
        if (standaloneLeaderServices != null) {
            standaloneLeaderServices.close();
            standaloneLeaderServices = null;
        }
    }

    /**
     * Tests that the standalone leader election services return a fixed address and leader session
     * id.
     */
    @Test
    public void testLeaderElection() throws Exception {
        JobID jobId = new JobID();

        final LeaderElection jmLeaderElection =
                standaloneLeaderServices.getJobMasterLeaderElection(jobId);
        TestingContender jmLeaderContender = new TestingContender("UNKNOWN", jmLeaderElection);
        jmLeaderElection.startLeaderElection(jmLeaderContender);

        final LeaderElection rmLeaderElection =
                standaloneLeaderServices.getResourceManagerLeaderElection();
        TestingContender rmLeaderContender = new TestingContender("UNKNOWN", rmLeaderElection);
        rmLeaderElection.startLeaderElection(rmLeaderContender);

        assertThat(jmLeaderContender.getLeaderSessionID())
                .isEqualTo(HighAvailabilityServices.DEFAULT_LEADER_ID);
        assertThat(rmLeaderContender.getLeaderSessionID())
                .isEqualTo(HighAvailabilityServices.DEFAULT_LEADER_ID);
    }

    /**
     * Tests that the standalone leader retrieval services return the specified address and the
     * fixed leader session id.
     */
    @Test
    public void testJobManagerLeaderRetrieval() throws Exception {
        JobID jobId1 = new JobID();
        JobID jobId2 = new JobID();
        TestingListener jmListener1 = new TestingListener();
        TestingListener jmListener2 = new TestingListener();
        TestingListener rmListener = new TestingListener();

        LeaderRetrievalService jmLeaderRetrievalService1 =
                standaloneLeaderServices.getJobMasterLeaderRetriever(jobId1, "UNKNOWN");
        LeaderRetrievalService jmLeaderRetrievalService2 =
                standaloneLeaderServices.getJobMasterLeaderRetriever(jobId2, "UNKNOWN");
        LeaderRetrievalService rmLeaderRetrievalService =
                standaloneLeaderServices.getResourceManagerLeaderRetriever();

        jmLeaderRetrievalService1.start(jmListener1);
        jmLeaderRetrievalService2.start(jmListener2);
        rmLeaderRetrievalService.start(rmListener);

        assertThat(jmListener1.waitForNewLeader()).isEqualTo("UNKNOWN");
        assertThat(jmListener2.waitForNewLeader()).isEqualTo("UNKNOWN");
        assertThat(rmListener.waitForNewLeader()).isEqualTo(resourceManagerAddress);

        assertThat(jmListener1.getLeaderSessionID())
                .isEqualTo(HighAvailabilityServices.DEFAULT_LEADER_ID);
        assertThat(jmListener2.getLeaderSessionID())
                .isEqualTo(HighAvailabilityServices.DEFAULT_LEADER_ID);
        assertThat(rmListener.getLeaderSessionID())
                .isEqualTo(HighAvailabilityServices.DEFAULT_LEADER_ID);
    }

    /**
     * Tests that the standalone leader retrieval services return the given address and the fixed
     * leader session id.
     */
    @Test
    public void testJobMasterLeaderRetrieval() throws Exception {
        JobID jobId1 = new JobID();
        JobID jobId2 = new JobID();
        final String jobManagerAddress1 = "foobar";
        final String jobManagerAddress2 = "barfoo";
        TestingListener jmListener1 = new TestingListener();
        TestingListener jmListener2 = new TestingListener();

        LeaderRetrievalService jmLeaderRetrievalService1 =
                standaloneLeaderServices.getJobMasterLeaderRetriever(jobId1, jobManagerAddress1);
        LeaderRetrievalService jmLeaderRetrievalService2 =
                standaloneLeaderServices.getJobMasterLeaderRetriever(jobId2, jobManagerAddress2);

        jmLeaderRetrievalService1.start(jmListener1);
        jmLeaderRetrievalService2.start(jmListener2);

        assertThat(jmListener1.waitForNewLeader()).isEqualTo(jobManagerAddress1);
        assertThat(jmListener2.waitForNewLeader()).isEqualTo(jobManagerAddress2);
        assertThat(jmListener1.getLeaderSessionID())
                .isEqualTo(HighAvailabilityServices.DEFAULT_LEADER_ID);
        assertThat(jmListener2.getLeaderSessionID())
                .isEqualTo(HighAvailabilityServices.DEFAULT_LEADER_ID);
    }
}
