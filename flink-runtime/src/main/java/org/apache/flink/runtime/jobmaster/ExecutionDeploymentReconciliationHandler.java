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

import java.util.Collection;

/** Interface for triggering actions in case of state mismatches. */
public interface ExecutionDeploymentReconciliationHandler {
    /**
     * Called if some executions are expected to be hosted on a task executor, but aren't.
     *
     * @param executionAttemptIds ids of the missing deployments
     * @param hostingTaskExecutor expected hosting task executor
     */
    void onMissingDeploymentsOf(
            Collection<ExecutionAttemptID> executionAttemptIds, ResourceID hostingTaskExecutor);

    /**
     * Called if some executions are hosted on a task executor, but we don't expect them.
     *
     * @param executionAttemptIds ids of the unknown executions
     * @param hostingTaskExecutor hosting task executor
     */
    void onUnknownDeploymentsOf(
            Collection<ExecutionAttemptID> executionAttemptIds, ResourceID hostingTaskExecutor);
}
