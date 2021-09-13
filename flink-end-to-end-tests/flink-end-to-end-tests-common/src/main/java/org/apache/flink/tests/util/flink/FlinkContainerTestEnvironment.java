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

import org.apache.flink.api.common.time.Deadline;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.connectors.test.common.environment.ClusterControllable;
import org.apache.flink.connectors.test.common.environment.TestEnvironment;
import org.apache.flink.core.execution.JobClient;
import org.apache.flink.runtime.testutils.CommonTestUtils;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

import java.time.Duration;

import static org.apache.flink.configuration.HeartbeatManagerOptions.HEARTBEAT_INTERVAL;
import static org.apache.flink.configuration.HeartbeatManagerOptions.HEARTBEAT_TIMEOUT;
import static org.apache.flink.configuration.JobManagerOptions.SLOT_REQUEST_TIMEOUT;
import static org.apache.flink.configuration.TaskManagerOptions.NUM_TASK_SLOTS;

/** Test environment running job on {@link FlinkContainer}. */
public class FlinkContainerTestEnvironment implements TestEnvironment, ClusterControllable {

    private final FlinkContainer flinkContainer;
    private final String[] jarPath;

    public FlinkContainerTestEnvironment(
            int numTaskManagers, int numSlotsPerTaskManager, String... jarPath) {

        Configuration flinkConfiguration = flinkConfiguration();
        flinkConfiguration.set(NUM_TASK_SLOTS, numSlotsPerTaskManager);

        this.flinkContainer =
                FlinkContainer.builder()
                        .numTaskManagers(numTaskManagers)
                        .withFlinkConfiguration(flinkConfiguration)
                        .build();
        this.jarPath = jarPath;
    }

    @Override
    public void startUp() {
        if (!flinkContainer.isRunning()) {
            this.flinkContainer.start();
        }
    }

    @Override
    public void tearDown() {
        if (flinkContainer.isRunning()) {
            this.flinkContainer.stop();
        }
    }

    @Override
    public StreamExecutionEnvironment createExecutionEnvironment() {
        return StreamExecutionEnvironment.createRemoteEnvironment(
                this.flinkContainer.getHost(),
                this.flinkContainer.getMappedPort(FlinkContainer.JOB_MANAGER_REST_PORT),
                this.jarPath);
    }

    @Override
    public void triggerJobManagerFailover(JobClient jobClient, Runnable afterFailAction) {}

    @Override
    public void triggerTaskManagerFailover(JobClient jobClient, Runnable afterFailAction)
            throws Exception {
        flinkContainer.restartTaskManager(
                () -> {
                    CommonTestUtils.waitForNoTaskRunning(
                            () ->
                                    flinkContainer
                                            .getRestClusterClient()
                                            .getJobDetails(jobClient.getJobID())
                                            .get(),
                            Deadline.fromNow(Duration.ofMinutes(5)));
                    afterFailAction.run();
                });
    }

    @Override
    public void isolateNetwork(JobClient jobClient, Runnable afterFailAction) {}

    @Override
    public String toString() {
        return "FlinkContainer";
    }

    /**
     * Get instance of Flink containers for cluster controlling.
     *
     * @return Flink cluster on Testcontainers
     */
    public FlinkContainer getFlinkContainer() {
        return this.flinkContainer;
    }

    protected Configuration flinkConfiguration() {
        Configuration flinkConfiguration = new Configuration();
        flinkConfiguration.set(HEARTBEAT_INTERVAL, 1000L);
        flinkConfiguration.set(HEARTBEAT_TIMEOUT, 5000L);
        flinkConfiguration.set(SLOT_REQUEST_TIMEOUT, 10000L);

        return flinkConfiguration;
    }
}
