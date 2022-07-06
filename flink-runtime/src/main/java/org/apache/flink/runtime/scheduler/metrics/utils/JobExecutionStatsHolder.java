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

package org.apache.flink.runtime.scheduler.metrics.utils;

import org.apache.flink.runtime.execution.ExecutionState;

/** Temporary holder for job execution stats. */
public class JobExecutionStatsHolder {
    private final int pendingDeployments;
    private final int initializingDeployments;
    private final int completedDeployments;

    public JobExecutionStatsHolder(
            int pendingDeployments, int initializingDeployments, int completedDeployments) {
        this.pendingDeployments = pendingDeployments;
        this.initializingDeployments = initializingDeployments;
        this.completedDeployments = completedDeployments;
    }

    public int getPendingDeployments() {
        return pendingDeployments;
    }

    public int getInitializingDeployments() {
        return initializingDeployments;
    }

    public int getCompletedDeployments() {
        return completedDeployments;
    }

    public int getStateMetric(ExecutionState executionState) {
        switch (executionState) {
            case DEPLOYING:
                return pendingDeployments;
            case INITIALIZING:
                return initializingDeployments;
            case RUNNING:
                return completedDeployments;
            default:
                throw new IllegalArgumentException("Unsupported state passed in");
        }
    }
}
