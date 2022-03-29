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

package org.apache.flink.tests.util.flink;

import org.apache.flink.configuration.Configuration;
import org.apache.flink.connector.testframe.environment.ClusterControllable;
import org.apache.flink.connector.testframe.environment.TestEnvironment;
import org.apache.flink.connector.testframe.environment.TestEnvironmentSettings;
import org.apache.flink.core.execution.JobClient;
import org.apache.flink.runtime.jobgraph.SavepointRestoreSettings;
import org.apache.flink.runtime.testutils.CommonTestUtils;
import org.apache.flink.streaming.api.environment.RemoteStreamEnvironment;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.tests.util.flink.container.FlinkContainers;
import org.apache.flink.tests.util.flink.container.FlinkContainersBuilder;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.net.URL;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.stream.Collectors;

import static org.apache.flink.configuration.HeartbeatManagerOptions.HEARTBEAT_INTERVAL;
import static org.apache.flink.configuration.HeartbeatManagerOptions.HEARTBEAT_TIMEOUT;
import static org.apache.flink.configuration.JobManagerOptions.SLOT_REQUEST_TIMEOUT;
import static org.apache.flink.configuration.MetricOptions.METRIC_FETCHER_UPDATE_INTERVAL;
import static org.apache.flink.configuration.TaskManagerOptions.NUM_TASK_SLOTS;
import static org.apache.flink.connector.testframe.utils.ConnectorTestConstants.HEARTBEAT_INTERVAL_MS;
import static org.apache.flink.connector.testframe.utils.ConnectorTestConstants.HEARTBEAT_TIMEOUT_MS;
import static org.apache.flink.connector.testframe.utils.ConnectorTestConstants.METRIC_FETCHER_UPDATE_INTERVAL_MS;
import static org.apache.flink.connector.testframe.utils.ConnectorTestConstants.SLOT_REQUEST_TIMEOUT_MS;

/** Test environment running job on {@link FlinkContainers}. */
public class FlinkContainerTestEnvironment implements TestEnvironment, ClusterControllable {

    private static final Logger LOG = LoggerFactory.getLogger(FlinkContainerTestEnvironment.class);

    private final FlinkContainers flinkContainers;
    private final List<String> jarPaths = new ArrayList<>();

    public FlinkContainerTestEnvironment(
            int numTaskManagers, int numSlotsPerTaskManager, String... jarPaths) {
        this(new Configuration(), numTaskManagers, numSlotsPerTaskManager, jarPaths);
    }

    public FlinkContainerTestEnvironment(
            Configuration clusterConfiguration,
            int numTaskManagers,
            int numSlotsPerTaskManager,
            String... jarPaths) {
        Configuration config = new Configuration();
        config.addAll(clusterConfiguration);
        config.set(NUM_TASK_SLOTS, numSlotsPerTaskManager);
        config.set(HEARTBEAT_INTERVAL, HEARTBEAT_INTERVAL_MS);
        config.set(HEARTBEAT_TIMEOUT, HEARTBEAT_TIMEOUT_MS);
        config.set(SLOT_REQUEST_TIMEOUT, SLOT_REQUEST_TIMEOUT_MS);
        config.set(METRIC_FETCHER_UPDATE_INTERVAL, METRIC_FETCHER_UPDATE_INTERVAL_MS);
        flinkContainers =
                FlinkContainers.builder()
                        .setNumTaskManagers(numTaskManagers)
                        .setConfiguration(config)
                        .setLogger(LOG)
                        .enableZookeeperHA()
                        .build();
        this.jarPaths.addAll(Arrays.asList(jarPaths));
    }

    @Override
    public void startUp() throws Exception {
        if (!flinkContainers.isStarted()) {
            flinkContainers.start();
        }
    }

    @Override
    public void tearDown() {
        if (flinkContainers.isStarted()) {
            flinkContainers.stop();
        }
    }

    @Override
    public StreamExecutionEnvironment createExecutionEnvironment(
            TestEnvironmentSettings envOptions) {
        jarPaths.addAll(
                envOptions.getConnectorJarPaths().stream()
                        .map(URL::getPath)
                        .collect(Collectors.toList()));
        if (envOptions.getSavepointRestorePath() != null) {
            return new RemoteStreamEnvironment(
                    flinkContainers.getJobManagerHost(),
                    flinkContainers.getJobManagerPort(),
                    null,
                    jarPaths.toArray(new String[0]),
                    null,
                    SavepointRestoreSettings.forPath(envOptions.getSavepointRestorePath()));
        }
        return StreamExecutionEnvironment.createRemoteEnvironment(
                flinkContainers.getJobManagerHost(),
                flinkContainers.getJobManagerPort(),
                jarPaths.toArray(new String[0]));
    }

    @Override
    public Endpoint getRestEndpoint() {
        return new Endpoint(
                flinkContainers.getJobManagerHost(), flinkContainers.getJobManagerPort());
    }

    @Override
    public String getCheckpointUri() {
        return FlinkContainersBuilder.CHECKPOINT_PATH.toString();
    }

    @Override
    public void triggerJobManagerFailover(JobClient jobClient, Runnable afterFailAction)
            throws Exception {
        flinkContainers.restartJobManager(afterFailAction::run);
    }

    @Override
    public void triggerTaskManagerFailover(JobClient jobClient, Runnable afterFailAction)
            throws Exception {
        flinkContainers.restartTaskManager(
                () -> {
                    CommonTestUtils.waitForNoTaskRunning(
                            () ->
                                    flinkContainers
                                            .getRestClusterClient()
                                            .getJobDetails(jobClient.getJobID())
                                            .get());
                    afterFailAction.run();
                });
    }

    @Override
    public void isolateNetwork(JobClient jobClient, Runnable afterFailAction) {}

    @Override
    public String toString() {
        return "FlinkContainers";
    }

    /**
     * Get instance of Flink containers for cluster controlling.
     *
     * @return Flink cluster on Testcontainers
     */
    public FlinkContainers getFlinkContainers() {
        return this.flinkContainers;
    }
}
