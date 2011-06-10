/***********************************************************************************************************************
 *
 * Copyright (C) 2010 by the Stratosphere project (http://stratosphere.eu)
 *
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software distributed under the License is distributed on
 * an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the License for the
 * specific language governing permissions and limitations under the License.
 *
 **********************************************************************************************************************/

package eu.stratosphere.nephele.jobmanager;

import java.util.List;

import eu.stratosphere.nephele.executiongraph.ExecutionVertex;
import eu.stratosphere.nephele.instance.AllocatedResource;
import eu.stratosphere.nephele.jobgraph.JobID;

public interface DeploymentManager {

	/**
	 * Deploys the list of vertices on the given {@link AllocatedResource}.
	 * 
	 * @param jobID
	 *        the ID of the job the vertices to be deployed belong to
	 * @param allocatedResource
	 *        the resource on which the vertices shall be deployed
	 * @param verticesToBeDeployed
	 *        the list of vertices to be deployed
	 */
	void deploy(JobID jobID, AllocatedResource allocatedResource, List<ExecutionVertex> verticesToBeDeployed);
}
