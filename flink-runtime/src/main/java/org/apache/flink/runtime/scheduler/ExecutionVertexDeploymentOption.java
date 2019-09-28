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

package org.apache.flink.runtime.scheduler;

import org.apache.flink.runtime.scheduler.strategy.ExecutionVertexID;

import static org.apache.flink.util.Preconditions.checkNotNull;

/**
 * Component that stores the task need to be scheduled and the option for deployment.
 */
public class ExecutionVertexDeploymentOption {

	private final ExecutionVertexID executionVertexId;

	private final DeploymentOption deploymentOption;

	public ExecutionVertexDeploymentOption(ExecutionVertexID executionVertexId, DeploymentOption deploymentOption) {
		this.executionVertexId = checkNotNull(executionVertexId);
		this.deploymentOption = checkNotNull(deploymentOption);
	}

	public ExecutionVertexID getExecutionVertexId() {
		return executionVertexId;
	}

	public DeploymentOption getDeploymentOption() {
		return deploymentOption;
	}
}
