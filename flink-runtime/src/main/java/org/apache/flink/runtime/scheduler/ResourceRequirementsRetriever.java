/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.apache.flink.runtime.scheduler;

import org.apache.flink.configuration.MemorySize;
import org.apache.flink.runtime.clusterframework.types.ResourceProfile;
import org.apache.flink.runtime.executiongraph.ExecutionJobVertex;
import org.apache.flink.runtime.instance.SlotSharingGroupId;
import org.apache.flink.runtime.jobgraph.JobVertexID;
import org.apache.flink.runtime.jobmanager.scheduler.SlotSharingGroup;
import org.apache.flink.runtime.shuffle.ShuffleMaster;

import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

import static org.apache.flink.util.Preconditions.checkNotNull;
import static org.apache.flink.util.Preconditions.checkState;

/**
 * Class that builds and host resource requirements for {@link ExecutionJobVertex} and {@link SlotSharingGroup}.
 */
public final class ResourceRequirementsRetriever {

	private final Map<JobVertexID, ResourceProfile> vertexResourceProfiles = new HashMap<>();

	private final Map<SlotSharingGroupId, ResourceProfile> groupResourceProfiles = new HashMap<>();

	public ResourceRequirementsRetriever(
			final Map<JobVertexID, ExecutionJobVertex> vertices,
			final ShuffleMaster<?> shuffleMaster) {

		checkNotNull(vertices);
		checkNotNull(shuffleMaster);

		buildResourceRequirements(vertices, shuffleMaster);
	}

	private void buildResourceRequirements(
			final Map<JobVertexID, ExecutionJobVertex> vertices,
			final ShuffleMaster<?> shuffleMaster) {

		final Set<SlotSharingGroup> slotSharingGroups = new HashSet<>();

		for (ExecutionJobVertex vertex : vertices.values()) {
			final SlotSharingGroup group = vertex.getSlotSharingGroup();
			if (group != null) {
				slotSharingGroups.add(group);
			}

			final ResourceProfile enrichedResourceProfile = getEnrichedResourceProfile(vertex, vertices, shuffleMaster);
			vertexResourceProfiles.put(vertex.getJobVertexId(), enrichedResourceProfile);
		}

		for (SlotSharingGroup group : slotSharingGroups) {
			checkState(group.getJobVertexIds().size() > 0);

			ResourceProfile totalResources = null;
			for (JobVertexID jobVertexID : group.getJobVertexIds()) {
				final ResourceProfile vertexResources = getJobVertexResourceRequirement(jobVertexID);
				totalResources = totalResources == null
					? vertexResources
					: totalResources.merge(vertexResources);
			}
			groupResourceProfiles.put(group.getSlotSharingGroupId(), totalResources);
		}
	}

	/**
	 * Enrich the original resource profile with required shuffle memory.
	 */
	private ResourceProfile getEnrichedResourceProfile(
			final ExecutionJobVertex vertex,
			final Map<JobVertexID, ExecutionJobVertex> vertices,
			final ShuffleMaster<?> shuffleMaster) {

		final MemorySize requiredShuffleMemory = shuffleMaster.getShuffleMemoryForTask(
			ExecutionJobVertexTaskInputsOutputsDescriptorBuilder.buildTaskInputsOutputsDescriptor(vertex, vertices));

		final ResourceProfile original = vertex.getResourceProfile();
		final ResourceProfile enriched;
		if (original.equals(ResourceProfile.UNKNOWN)) {
			enriched = ResourceProfile.UNKNOWN;
		} else {
			enriched = ResourceProfile.newBuilder()
				.setCpuCores(original.getCpuCores())
				.setTaskHeapMemory(original.getTaskHeapMemory())
				.setTaskOffHeapMemory(original.getTaskOffHeapMemory())
				.setManagedMemory(original.getManagedMemory())
				.setShuffleMemory(requiredShuffleMemory)
				.addExtendedResources(original.getExtendedResources())
				.build();
		}

		return enriched;
	}

	public ResourceProfile getJobVertexResourceRequirement(final JobVertexID jobVertexID) {
		checkNotNull(jobVertexID);

		return vertexResourceProfiles.get(jobVertexID);
	}

	public ResourceProfile getSlotSharingGroupResourceRequirement(final SlotSharingGroupId slotSharingGroupId) {
		checkNotNull(slotSharingGroupId);

		return groupResourceProfiles.get(slotSharingGroupId);
	}
}
