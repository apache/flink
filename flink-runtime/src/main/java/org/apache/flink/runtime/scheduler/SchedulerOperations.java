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

import org.apache.flink.runtime.scheduler.strategy.SchedulingStrategy;

import java.util.List;

/**
 * Component which is used by {@link SchedulingStrategy} to commit scheduling decisions.
 */
public interface SchedulerOperations {

	/**
	 * Allocate slots and deploy the vertex when slots are returned.
	 * The given order will be respected, i.e. tasks with smaller indices will be deployed earlier.
	 * Only vertices in CREATED state will be accepted. Errors will happen if scheduling Non-CREATED vertices.
	 *
	 * @param executionVertexDeploymentOptions The deployment options of tasks to be deployed
	 */
	void allocateSlotsAndDeploy(List<ExecutionVertexDeploymentOption> executionVertexDeploymentOptions);
}
