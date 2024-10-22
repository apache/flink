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

package org.apache.flink.runtime.jobmanager;

import org.apache.flink.api.common.JobID;

import static org.apache.flink.util.Preconditions.checkNotNull;

/** {@link ExecutionPlanStoreWatcher} implementation for testing purposes. */
public class TestingExecutionPlanStoreWatcher implements ExecutionPlanStoreWatcher {

    private ExecutionPlanStore.ExecutionPlanListener executionPlanListener;

    @Override
    public void start(ExecutionPlanStore.ExecutionPlanListener executionPlanListener) {
        this.executionPlanListener = executionPlanListener;
    }

    @Override
    public void stop() {
        // noop
    }

    public void addExecutionPlan(JobID jobID) {
        checkNotNull(executionPlanListener, "TestingExecutionPlanStoreWatcher is not started.");
        executionPlanListener.onAddedExecutionPlan(jobID);
    }

    public void removeExecutionPlan(JobID jobID) {
        checkNotNull(executionPlanListener, "TestingExecutionPlanStoreWatcher is not started.");
        executionPlanListener.onRemovedExecutionPlan(jobID);
    }
}
