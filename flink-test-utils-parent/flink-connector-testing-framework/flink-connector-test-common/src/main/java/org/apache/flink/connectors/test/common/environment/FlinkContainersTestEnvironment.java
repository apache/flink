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

import org.apache.flink.api.common.JobStatus;
import org.apache.flink.connectors.test.common.utils.FlinkContainers;
import org.apache.flink.connectors.test.common.utils.FlinkJobStatusHelper;
import org.apache.flink.core.execution.JobClient;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

import java.time.Duration;
import java.util.Arrays;

/** Test environment running job on {@link FlinkContainers}. */
public class FlinkContainersTestEnvironment implements TestEnvironment, ClusterControllable {

    private final FlinkContainers flinkContainers;

    private final String[] jarPath;

    @Override
    public void startUp() throws Exception {
        flinkContainers.startUp();
    }

    @Override
    public void tearDown() {
        flinkContainers.tearDown();
    }

    public FlinkContainersTestEnvironment(int numTaskManagers, String... jarPath) {
        this.jarPath = jarPath;
        this.flinkContainers = FlinkContainers.builder("test", numTaskManagers).build();
    }

    /**
     * Get instance of Flink containers for cluster controlling.
     *
     * @return Flink cluster on Testcontainers
     */
    public FlinkContainers getFlinkContainers() {
        return flinkContainers;
    }

    @Override
    public StreamExecutionEnvironment createExecutionEnvironment() {
        return StreamExecutionEnvironment.createRemoteEnvironment(
                flinkContainers.getJobManagerHost(),
                flinkContainers.getJobManagerRESTPort(),
                jarPath);
    }

    @Override
    public void triggerJobManagerFailover(JobClient jobClient, Runnable afterFailAction) {}

    @Override
    public void triggerTaskManagerFailover(JobClient jobClient, Runnable afterFailAction) {
        flinkContainers.restartTaskManagers(
                0,
                () -> {
                    try {
                        FlinkJobStatusHelper.waitForJobStatus(
                                jobClient,
                                Arrays.asList(
                                        JobStatus.FAILING, JobStatus.FAILED, JobStatus.RESTARTING),
                                Duration.ofSeconds(300));
                        afterFailAction.run();
                    } catch (Exception e) {
                        throw new RuntimeException(
                                "Error when waiting for job entering FAILING status", e);
                    }
                });
    }

    @Override
    public void isolateNetwork(JobClient jobClient, Runnable afterFailAction) {}
}
