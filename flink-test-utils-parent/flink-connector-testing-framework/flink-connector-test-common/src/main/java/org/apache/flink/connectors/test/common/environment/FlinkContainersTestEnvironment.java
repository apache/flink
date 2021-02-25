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

import org.apache.flink.api.common.JobID;
import org.apache.flink.connectors.test.common.utils.FlinkContainers;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

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
    public void triggerJobManagerFailover(JobID jobID, Runnable afterFailAction) {}

    @Override
    public void triggerTaskManagerFailover(JobID jobID, Runnable afterFailAction) {
        flinkContainers.restartTaskManagers(0);
    }

    @Override
    public void isolateNetwork(Runnable afterFailAction) {}
}
