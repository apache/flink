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

package org.apache.flink.connectors.test.common.environment;

import org.apache.flink.annotation.Experimental;
import org.apache.flink.api.common.time.Deadline;
import org.apache.flink.core.execution.JobClient;
import org.apache.flink.runtime.highavailability.nonha.embedded.HaLeadershipControl;
import org.apache.flink.runtime.minicluster.RpcServiceSharing;
import org.apache.flink.runtime.testutils.CommonTestUtils;
import org.apache.flink.runtime.testutils.MiniClusterResourceConfiguration;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.test.util.MiniClusterWithClientResource;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Duration;
import java.util.Optional;
import java.util.concurrent.ExecutionException;

/** Test environment for running jobs on Flink mini-cluster. */
@Experimental
public class MiniClusterTestEnvironment implements TestEnvironment, ClusterControllable {

    private static final Logger LOG = LoggerFactory.getLogger(MiniClusterTestEnvironment.class);

    // MiniCluster
    private final MiniClusterWithClientResource miniCluster;

    // The index of current running TaskManager
    private int latestTMIndex = 0;

    // Whether MiniCluster is running
    private boolean isStarted = false;

    public MiniClusterTestEnvironment() {
        this.miniCluster =
                new MiniClusterWithClientResource(
                        new MiniClusterResourceConfiguration.Builder()
                                .setNumberTaskManagers(1)
                                .setNumberSlotsPerTaskManager(6)
                                .setRpcServiceSharing(RpcServiceSharing.DEDICATED)
                                .withHaLeadershipControl()
                                .build());
    }

    @Override
    public StreamExecutionEnvironment createExecutionEnvironment() {
        return StreamExecutionEnvironment.getExecutionEnvironment();
    }

    @Override
    public void triggerJobManagerFailover(JobClient jobClient, Runnable afterFailAction)
            throws ExecutionException, InterruptedException {
        final Optional<HaLeadershipControl> controlOptional =
                miniCluster.getMiniCluster().getHaLeadershipControl();
        if (!controlOptional.isPresent()) {
            throw new UnsupportedOperationException(
                    "This MiniCluster does not support JobManager HA");
        }
        final HaLeadershipControl haLeadershipControl = controlOptional.get();
        haLeadershipControl.revokeJobMasterLeadership(jobClient.getJobID()).get();
        afterFailAction.run();
        haLeadershipControl.grantJobMasterLeadership(jobClient.getJobID()).get();
    }

    @Override
    public void triggerTaskManagerFailover(JobClient jobClient, Runnable afterFailAction)
            throws Exception {
        terminateTaskManager();
        CommonTestUtils.waitForNoTaskRunning(
                () -> miniCluster.getRestClusterClient().getJobDetails(jobClient.getJobID()).get(),
                Deadline.fromNow(Duration.ofMinutes(5)));
        afterFailAction.run();
        startTaskManager();
    }

    @Override
    public void isolateNetwork(JobClient jobClient, Runnable afterFailAction) {
        throw new UnsupportedOperationException("Cannot isolate network in a MiniCluster");
    }

    @Override
    public void startUp() throws Exception {
        if (isStarted) {
            return;
        }
        this.miniCluster.before();
        LOG.debug("MiniCluster is running");
        isStarted = true;
    }

    @Override
    public void tearDown() {
        if (!isStarted) {
            return;
        }
        isStarted = false;
        this.miniCluster.after();
        LOG.debug("MiniCluster has been tear down");
    }

    private void terminateTaskManager() throws Exception {
        miniCluster.getMiniCluster().terminateTaskManager(latestTMIndex).get();
        LOG.debug("TaskManager {} has been terminated.", latestTMIndex);
    }

    private void startTaskManager() throws Exception {
        miniCluster.getMiniCluster().startTaskManager();
        latestTMIndex++;
        LOG.debug("New TaskManager {} has been launched.", latestTMIndex);
    }

    @Override
    public String toString() {
        return "MiniCluster";
    }
}
