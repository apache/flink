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

package org.apache.flink.runtime.jobmaster;

import org.apache.flink.runtime.clusterframework.types.ResourceID;
import org.apache.flink.runtime.executiongraph.ExecutionAttemptID;

import java.util.Collections;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.CompletableFuture;

/** Testing implementation of the {@link ExecutionDeploymentTracker}. */
public class TestingExecutionDeploymentTrackerWrapper implements ExecutionDeploymentTracker {
    private final ExecutionDeploymentTracker originalTracker;
    private final CompletableFuture<ExecutionAttemptID> taskDeploymentFuture;
    private final CompletableFuture<ExecutionAttemptID> stopFuture;
    private final Set<ExecutionAttemptID> deployedExecutions = new HashSet<>();

    public TestingExecutionDeploymentTrackerWrapper() {
        this(new DefaultExecutionDeploymentTracker());
    }

    public TestingExecutionDeploymentTrackerWrapper(ExecutionDeploymentTracker originalTracker) {
        this.originalTracker = originalTracker;
        this.taskDeploymentFuture = new CompletableFuture<>();
        this.stopFuture = new CompletableFuture<>();
    }

    @Override
    public void startTrackingPendingDeploymentOf(
            ExecutionAttemptID executionAttemptId, ResourceID host) {
        originalTracker.startTrackingPendingDeploymentOf(executionAttemptId, host);
    }

    @Override
    public void completeDeploymentOf(ExecutionAttemptID executionAttemptId) {
        originalTracker.completeDeploymentOf(executionAttemptId);
        taskDeploymentFuture.complete(executionAttemptId);
        deployedExecutions.add(executionAttemptId);
    }

    @Override
    public void stopTrackingDeploymentOf(ExecutionAttemptID executionAttemptId) {
        originalTracker.stopTrackingDeploymentOf(executionAttemptId);
        stopFuture.complete(executionAttemptId);
    }

    @Override
    public Map<ExecutionAttemptID, ExecutionDeploymentState> getExecutionsOn(ResourceID host) {
        return originalTracker.getExecutionsOn(host);
    }

    public CompletableFuture<ExecutionAttemptID> getTaskDeploymentFuture() {
        return taskDeploymentFuture;
    }

    public CompletableFuture<ExecutionAttemptID> getStopFuture() {
        return stopFuture;
    }

    public Set<ExecutionAttemptID> getDeployedExecutions() {
        return Collections.unmodifiableSet(deployedExecutions);
    }
}
