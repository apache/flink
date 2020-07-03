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

import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

/**
 * Default {@link ExecutionDeploymentTracker} implementation.
 */
public class DefaultExecutionDeploymentTracker implements ExecutionDeploymentTracker {

	private final Map<ResourceID, Set<ExecutionAttemptID>> executionsByHost = new HashMap<>();
	private final Map<ExecutionAttemptID, ResourceID> hostByExecution = new HashMap<>();

	@Override
	public void startTrackingDeploymentOf(ExecutionAttemptID executionAttemptId, ResourceID host) {
		hostByExecution.put(executionAttemptId, host);
		executionsByHost.computeIfAbsent(host, ignored -> new HashSet<>()).add(executionAttemptId);
	}

	@Override
	public void stopTrackingDeploymentOf(ExecutionAttemptID executionAttemptId) {
		ResourceID host = hostByExecution.remove(executionAttemptId);
		if (host != null) {
			executionsByHost.computeIfPresent(host, (resourceID, executionAttemptIds) -> {
				executionAttemptIds.remove(executionAttemptId);

				return executionAttemptIds.isEmpty()
					? null
					: executionAttemptIds;
			});
		}
	}

	@Override
	public Set<ExecutionAttemptID> getExecutionsOn(ResourceID host) {
		return executionsByHost.getOrDefault(host, Collections.emptySet());
	}
}
