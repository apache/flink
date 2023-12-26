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

package org.apache.flink.connector.testframe.environment;

import org.apache.flink.annotation.Experimental;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.core.execution.JobClient;
import org.apache.flink.runtime.highavailability.nonha.embedded.HaLeadershipControl;
import org.apache.flink.runtime.minicluster.RpcServiceSharing;
import org.apache.flink.runtime.testutils.CommonTestUtils;
import org.apache.flink.runtime.testutils.MiniClusterResourceConfiguration;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.util.TestStreamEnvironment;
import org.apache.flink.test.util.MiniClusterWithClientResource;

import org.apache.commons.io.FileUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.net.URI;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Collections;
import java.util.Optional;
import java.util.concurrent.ExecutionException;
import java.util.stream.Collectors;

import static org.apache.flink.configuration.MetricOptions.METRIC_FETCHER_UPDATE_INTERVAL;
import static org.apache.flink.connector.testframe.utils.ConnectorTestConstants.METRIC_FETCHER_UPDATE_INTERVAL_MS;
import static org.apache.flink.runtime.jobgraph.SavepointConfigOptions.SAVEPOINT_PATH;

/** Test environment for running jobs on Flink mini-cluster. */
@Experimental
public class MiniClusterTestEnvironment implements TestEnvironment, ClusterControllable {

    private static final Logger LOG = LoggerFactory.getLogger(MiniClusterTestEnvironment.class);

    private final MiniClusterWithClientResource miniCluster;
    private final Path checkpointPath;

    // The index of current running TaskManager
    private int latestTMIndex = 0;
    private boolean isStarted = false;

    public MiniClusterTestEnvironment() {
        this(defaultMiniClusterResourceConfiguration());
    }

    public MiniClusterTestEnvironment(MiniClusterResourceConfiguration conf) {
        this.miniCluster = new MiniClusterWithClientResource(conf);
        try {
            this.checkpointPath = Files.createTempDirectory("minicluster-environment-checkpoint-");
        } catch (IOException e) {
            throw new RuntimeException("Failed to create temporary checkpoint directory", e);
        }
    }

    private static MiniClusterResourceConfiguration defaultMiniClusterResourceConfiguration() {
        Configuration conf = new Configuration();
        conf.set(METRIC_FETCHER_UPDATE_INTERVAL, METRIC_FETCHER_UPDATE_INTERVAL_MS);
        return new MiniClusterResourceConfiguration.Builder()
                .setConfiguration(conf)
                .setNumberTaskManagers(1)
                .setNumberSlotsPerTaskManager(6)
                .setRpcServiceSharing(RpcServiceSharing.DEDICATED)
                .withHaLeadershipControl()
                .build();
    }

    @Override
    public StreamExecutionEnvironment createExecutionEnvironment(
            TestEnvironmentSettings envOptions) {
        Configuration configuration = new Configuration();
        if (envOptions.getSavepointRestorePath() != null) {
            configuration.setString(SAVEPOINT_PATH, envOptions.getSavepointRestorePath());
        }
        return new TestStreamEnvironment(
                this.miniCluster.getMiniCluster(),
                configuration,
                this.miniCluster.getNumberSlots(),
                envOptions.getConnectorJarPaths().stream()
                        .map(url -> new org.apache.flink.core.fs.Path(url.getPath()))
                        .collect(Collectors.toList()),
                Collections.emptyList());
    }

    @Override
    public Endpoint getRestEndpoint() {
        try {
            final URI restAddress = this.miniCluster.getMiniCluster().getRestAddress().get();
            return new Endpoint(restAddress.getHost(), restAddress.getPort());
        } catch (Exception e) {
            throw new RuntimeException("Failed to get REST endpoint of MiniCluster", e);
        }
    }

    @Override
    public String getCheckpointUri() {
        return checkpointPath.toUri().toString();
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
                () -> miniCluster.getRestClusterClient().getJobDetails(jobClient.getJobID()).get());
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
    public void tearDown() throws Exception {
        if (!isStarted) {
            return;
        }
        isStarted = false;
        this.miniCluster.after();
        FileUtils.deleteDirectory(checkpointPath.toFile());
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
