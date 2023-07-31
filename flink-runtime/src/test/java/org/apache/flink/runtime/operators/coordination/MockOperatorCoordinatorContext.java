/*
Licensed to the Apache Software Foundation (ASF) under one
or more contributor license agreements.  See the NOTICE file
distributed with this work for additional information
regarding copyright ownership.  The ASF licenses this file
to you under the Apache License, Version 2.0 (the
"License"); you may not use this file except in compliance
with the License.  You may obtain a copy of the License at

      http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
*/

package org.apache.flink.runtime.operators.coordination;

import org.apache.flink.metrics.groups.OperatorCoordinatorMetricGroup;
import org.apache.flink.runtime.checkpoint.CheckpointCoordinator;
import org.apache.flink.runtime.jobgraph.OperatorID;
import org.apache.flink.runtime.metrics.groups.InternalOperatorCoordinatorMetricGroup;
import org.apache.flink.runtime.metrics.groups.UnregisteredMetricGroups;

import java.util.concurrent.CompletableFuture;

/** A simple implementation of {@link OperatorCoordinator.Context} for testing purposes. */
public class MockOperatorCoordinatorContext implements OperatorCoordinator.Context {

    private final OperatorID operatorID;
    private final ClassLoader userCodeClassLoader;
    private final int numSubtasks;
    private final CoordinatorStore coordinatorStore = new CoordinatorStoreImpl();

    private boolean jobFailed;
    private Throwable jobFailureReason;
    private final CompletableFuture<Void> jobFailedFuture = new CompletableFuture<>();

    public MockOperatorCoordinatorContext(OperatorID operatorID, int numSubtasks) {
        this(operatorID, numSubtasks, MockOperatorCoordinatorContext.class.getClassLoader());
    }

    public MockOperatorCoordinatorContext(OperatorID operatorID, ClassLoader userCodeClassLoader) {
        this(operatorID, 1, userCodeClassLoader);
    }

    public MockOperatorCoordinatorContext(
            OperatorID operatorID, int numSubtasks, ClassLoader userCodeClassLoader) {
        this.operatorID = operatorID;
        this.numSubtasks = numSubtasks;
        this.jobFailed = false;
        this.jobFailureReason = null;
        this.userCodeClassLoader = userCodeClassLoader;
    }

    @Override
    public OperatorID getOperatorId() {
        return operatorID;
    }

    @Override
    public OperatorCoordinatorMetricGroup metricGroup() {
        return new InternalOperatorCoordinatorMetricGroup(
                UnregisteredMetricGroups.createUnregisteredJobManagerOperatorMetricGroup());
    }

    @Override
    public void failJob(Throwable cause) {
        jobFailed = true;
        jobFailureReason = cause;
        jobFailedFuture.complete(null);
    }

    @Override
    public int currentParallelism() {
        return numSubtasks;
    }

    @Override
    public ClassLoader getUserCodeClassloader() {
        return userCodeClassLoader;
    }

    @Override
    public CoordinatorStore getCoordinatorStore() {
        return coordinatorStore;
    }

    @Override
    public boolean isConcurrentExecutionAttemptsSupported() {
        return false;
    }

    @Override
    public CheckpointCoordinator getCheckpointCoordinator() {
        throw new UnsupportedOperationException();
    }

    // -------------------------------

    public boolean isJobFailed() {
        return jobFailed;
    }

    public Throwable getJobFailureReason() {
        return jobFailureReason;
    }

    public CompletableFuture<Void> getJobFailedFuture() {
        return jobFailedFuture;
    }
}
