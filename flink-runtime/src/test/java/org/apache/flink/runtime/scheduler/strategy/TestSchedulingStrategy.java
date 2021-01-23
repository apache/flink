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

package org.apache.flink.runtime.scheduler.strategy;

import org.apache.flink.runtime.execution.ExecutionState;
import org.apache.flink.runtime.jobgraph.IntermediateResultPartitionID;
import org.apache.flink.runtime.scheduler.DeploymentOption;
import org.apache.flink.runtime.scheduler.ExecutionVertexDeploymentOption;
import org.apache.flink.runtime.scheduler.SchedulerOperations;

import java.util.ArrayList;
import java.util.List;
import java.util.Set;

import static org.apache.flink.util.Preconditions.checkNotNull;

/** {@link SchedulingStrategy} instance for tests. */
public class TestSchedulingStrategy implements SchedulingStrategy {

    private final SchedulerOperations schedulerOperations;

    private final SchedulingTopology schedulingTopology;

    private final DeploymentOption deploymentOption = new DeploymentOption(false);

    private Set<ExecutionVertexID> receivedVerticesToRestart;

    public TestSchedulingStrategy(
            final SchedulerOperations schedulerOperations,
            final SchedulingTopology schedulingTopology) {

        this.schedulerOperations = checkNotNull(schedulerOperations);
        this.schedulingTopology = checkNotNull(schedulingTopology);
    }

    @Override
    public void startScheduling() {}

    @Override
    public void restartTasks(final Set<ExecutionVertexID> verticesToRestart) {
        this.receivedVerticesToRestart = verticesToRestart;
    }

    @Override
    public void onExecutionStateChange(
            final ExecutionVertexID executionVertexId, final ExecutionState executionState) {}

    @Override
    public void onPartitionConsumable(final IntermediateResultPartitionID resultPartitionId) {}

    public void schedule(final List<ExecutionVertexID> verticesToSchedule) {
        allocateSlotsAndDeploy(verticesToSchedule);
    }

    public SchedulingTopology getSchedulingTopology() {
        return schedulingTopology;
    }

    public Set<ExecutionVertexID> getReceivedVerticesToRestart() {
        return receivedVerticesToRestart;
    }

    private void allocateSlotsAndDeploy(final List<ExecutionVertexID> verticesToSchedule) {
        final List<ExecutionVertexDeploymentOption> executionVertexDeploymentOptions =
                createExecutionVertexDeploymentOptions(verticesToSchedule);
        schedulerOperations.allocateSlotsAndDeploy(executionVertexDeploymentOptions);
    }

    private List<ExecutionVertexDeploymentOption> createExecutionVertexDeploymentOptions(
            final List<ExecutionVertexID> vertices) {

        final List<ExecutionVertexDeploymentOption> executionVertexDeploymentOptions =
                new ArrayList<>(vertices.size());
        for (ExecutionVertexID executionVertexID : vertices) {
            executionVertexDeploymentOptions.add(
                    new ExecutionVertexDeploymentOption(executionVertexID, deploymentOption));
        }
        return executionVertexDeploymentOptions;
    }

    /** The factory for creating {@link TestSchedulingStrategy}. */
    public static class Factory implements SchedulingStrategyFactory {

        private TestSchedulingStrategy lastInstance;

        @Override
        public SchedulingStrategy createInstance(
                final SchedulerOperations schedulerOperations,
                final SchedulingTopology schedulingTopology) {

            lastInstance = new TestSchedulingStrategy(schedulerOperations, schedulingTopology);
            return lastInstance;
        }

        public TestSchedulingStrategy getLastCreatedSchedulingStrategy() {
            return lastInstance;
        }
    }
}
