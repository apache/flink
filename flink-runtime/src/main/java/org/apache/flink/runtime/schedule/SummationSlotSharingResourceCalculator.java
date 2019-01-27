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

package org.apache.flink.runtime.schedule;

import org.apache.flink.runtime.clusterframework.types.ResourceProfile;
import org.apache.flink.runtime.executiongraph.ExecutionGraph;
import org.apache.flink.runtime.executiongraph.ExecutionVertex;
import org.apache.flink.runtime.jobgraph.JobVertexID;
import org.apache.flink.runtime.jobmanager.scheduler.SlotSharingGroup;

import java.util.Set;

/**
 * A sharing group resource calculator who simply sums up the resources of all the vertices
 * inside the sharing group. It may not be optimal for non-eager scheduling policies.
 */
public class SummationSlotSharingResourceCalculator implements SlotSharingResourceCalculator {

	@Override
	public ResourceProfile calculateSharedGroupResource(SlotSharingGroup slotSharingGroup, ExecutionGraph executionGraph) {
		ResourceProfile shareGroupResource = null;

		Set<JobVertexID> jobVertexIDS = slotSharingGroup.getJobVertexIds();
		for (JobVertexID id : jobVertexIDS) {
			ExecutionVertex[] tasks = executionGraph.getJobVertex(id).getTaskVertices();

			// Compute the maximum resource of all the tasks. The tasks may have different network memory
			// when using RescalePartitioner.
			ResourceProfile vertexMaxResource = null;
			for (ExecutionVertex task : tasks) {
				ResourceProfile taskResource = task.calculateResourceProfile();

				// If the resource profile of one task is UNKNOWN, then the resource matching are not used
				// and the overall resource should also be UNKNOWN.
				if (taskResource == ResourceProfile.UNKNOWN) {
					return ResourceProfile.UNKNOWN;
				}

				if (vertexMaxResource == null) {
					vertexMaxResource = taskResource;
				} else {
					// The extended resources of tasks of the same vertex should be always the same，therefore
					// isMatching can compare the resource correctly.
					if (taskResource.isMatching(vertexMaxResource)) {
						vertexMaxResource = taskResource;
					}
				}
			}

			if (shareGroupResource == null) {
				shareGroupResource = vertexMaxResource;
			} else {
				shareGroupResource = shareGroupResource.merge(vertexMaxResource);
			}
		}

		return shareGroupResource;
	}
}
