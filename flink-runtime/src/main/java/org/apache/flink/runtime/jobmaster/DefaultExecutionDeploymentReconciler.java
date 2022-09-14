/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
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
import org.apache.flink.runtime.taskexecutor.ExecutionDeploymentReport;

import java.util.HashSet;
import java.util.Map;
import java.util.Set;

/**
 * Default {@link ExecutionDeploymentReconciler} implementation. Detects missing/unknown
 * deployments, and defers to a provided {@link ExecutionDeploymentReconciliationHandler} to resolve
 * them.
 */
public class DefaultExecutionDeploymentReconciler implements ExecutionDeploymentReconciler {

    private final ExecutionDeploymentReconciliationHandler handler;

    public DefaultExecutionDeploymentReconciler(ExecutionDeploymentReconciliationHandler handler) {
        this.handler = handler;
    }

    @Override
    public void reconcileExecutionDeployments(
            ResourceID taskExecutorHost,
            ExecutionDeploymentReport executionDeploymentReport,
            Map<ExecutionAttemptID, ExecutionDeploymentState> expectedDeployedExecutions) {
        final Set<ExecutionAttemptID> unknownExecutions =
                new HashSet<>(executionDeploymentReport.getExecutions());
        final Set<ExecutionAttemptID> missingExecutions = new HashSet<>();

        for (Map.Entry<ExecutionAttemptID, ExecutionDeploymentState> execution :
                expectedDeployedExecutions.entrySet()) {
            boolean deployed = unknownExecutions.remove(execution.getKey());
            if (!deployed && execution.getValue() != ExecutionDeploymentState.PENDING) {
                missingExecutions.add(execution.getKey());
            }
        }
        if (!unknownExecutions.isEmpty()) {
            handler.onUnknownDeploymentsOf(unknownExecutions, taskExecutorHost);
        }
        if (!missingExecutions.isEmpty()) {
            handler.onMissingDeploymentsOf(missingExecutions, taskExecutorHost);
        }
    }
}
