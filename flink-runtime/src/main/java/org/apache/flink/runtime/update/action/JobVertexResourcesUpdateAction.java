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

package org.apache.flink.runtime.update.action;

import org.apache.flink.api.common.operators.ResourceSpec;
import org.apache.flink.runtime.jobgraph.JobVertex;
import org.apache.flink.runtime.jobgraph.JobVertexID;

/**
 * Action to update the resources of a JobVertex.
 */
public class JobVertexResourcesUpdateAction extends JobVertexUpdateAction {

	/**
	 * The new min resource spec for the job vertex.
	 */
	private final ResourceSpec newMinResourceSpec;

	/**
	 * The new max resource spec for the job vertex.
	 */
	private final ResourceSpec newMaxResourceSpec;

	public JobVertexResourcesUpdateAction(JobVertexID jobVertexID, ResourceSpec newResourceSpec) {
		this(jobVertexID, newResourceSpec, newResourceSpec);
	}

	public JobVertexResourcesUpdateAction(JobVertexID jobVertexID, ResourceSpec newMinResourceSpec, ResourceSpec newMaxResourceSpec) {
		super(jobVertexID);

		this.newMinResourceSpec = newMinResourceSpec;
		this.newMaxResourceSpec = newMaxResourceSpec;
	}

	@Override
	public void updateVertex(JobVertex jobVertex) {
		jobVertex.setResources(newMinResourceSpec, newMaxResourceSpec);
	}
}
