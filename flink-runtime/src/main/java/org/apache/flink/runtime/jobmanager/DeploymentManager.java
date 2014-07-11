/**
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

import java.util.List;

import org.apache.flink.runtime.executiongraph.ExecutionVertex;
import org.apache.flink.runtime.instance.Instance;
import org.apache.flink.runtime.jobgraph.JobID;

/**
 * A deployment manager is responsible for deploying a list of {@link ExecutionVertex} objects the given
 * {@link org.apache.flink.runtime.instance.Instance}. It is called by a {@link org.apache.flink.runtime.jobmanager.scheduler.DefaultScheduler} implementation whenever at least one
 * {@link ExecutionVertex} has become ready to be executed.
 * 
 */
public interface DeploymentManager {

	/**
	 * Deploys the list of vertices on the given {@link org.apache.flink.runtime.instance.Instance}.
	 * 
	 * @param jobID
	 *        the ID of the job the vertices to be deployed belong to
	 * @param instance
	 *        the instance on which the vertices shall be deployed
	 * @param verticesToBeDeployed
	 *        the list of vertices to be deployed
	 */
	void deploy(JobID jobID, Instance instance, List<ExecutionVertex> verticesToBeDeployed);
}
