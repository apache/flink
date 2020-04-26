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

package org.apache.flink.yarn;

import org.apache.flink.annotation.VisibleForTesting;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.runtime.clusterframework.TaskExecutorProcessSpec;
import org.apache.flink.runtime.clusterframework.TaskExecutorProcessUtils;
import org.apache.flink.runtime.resourcemanager.WorkerResourceSpec;
import org.apache.flink.util.MathUtils;
import org.apache.flink.util.Preconditions;

import org.apache.hadoop.yarn.api.records.Resource;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Nullable;

import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.stream.Collectors;

/**
 * Utility class for converting between Flink {@link WorkerResourceSpec} and Yarn {@link Resource}.
 */
public class WorkerSpecContainerResourceAdapter {
	private static final Logger LOG = LoggerFactory.getLogger(WorkerSpecContainerResourceAdapter.class);

	private final Configuration flinkConfig;
	private final int minMemMB;
	private final int maxMemMB;
	private final int minVcore;
	private final int maxVcore;
	private final Map<WorkerResourceSpec, Resource> workerSpecToContainerResource;
	private final Map<Resource, Set<WorkerResourceSpec>> containerResourceToWorkerSpecs;
	private final Map<Integer, Set<Resource>> containerMemoryToContainerResource;

	WorkerSpecContainerResourceAdapter(
		final Configuration flinkConfig,
		final int minMemMB,
		final int minVcore,
		final int maxMemMB,
		final int maxVcore) {
		this.flinkConfig = Preconditions.checkNotNull(flinkConfig);
		this.minMemMB = minMemMB;
		this.minVcore = minVcore;
		this.maxMemMB = maxMemMB;
		this.maxVcore = maxVcore;
		workerSpecToContainerResource = new HashMap<>();
		containerResourceToWorkerSpecs = new HashMap<>();
		containerMemoryToContainerResource = new HashMap<>();
	}

	@VisibleForTesting
	Optional<Resource> tryComputeContainerResource(final WorkerResourceSpec workerResourceSpec) {
		return Optional.ofNullable(workerSpecToContainerResource.computeIfAbsent(
			Preconditions.checkNotNull(workerResourceSpec),
			this::createAndMapContainerResource));
	}

	@VisibleForTesting
	Set<WorkerResourceSpec> getWorkerSpecs(final Resource containerResource, final MatchingStrategy matchingStrategy) {
		return getEquivalentContainerResource(containerResource, matchingStrategy).stream()
			.flatMap(resource -> containerResourceToWorkerSpecs.getOrDefault(resource, Collections.emptySet()).stream())
			.collect(Collectors.toSet());
	}

	@VisibleForTesting
	Set<Resource> getEquivalentContainerResource(final Resource containerResource, final MatchingStrategy matchingStrategy) {
		// Yarn might ignore the requested vcores, depending on its configurations.
		// In such cases, we should also not matching vcores.
		final Set<Resource> equivalentContainerResources;
		switch (matchingStrategy) {
			case MATCH_VCORE:
				equivalentContainerResources = Collections.singleton(containerResource);
				break;
			case IGNORE_VCORE:
			default:
				equivalentContainerResources = containerMemoryToContainerResource
					.getOrDefault(containerResource.getMemory(), Collections.emptySet());
				break;
		}
		return equivalentContainerResources;
	}

	@Nullable
	private Resource createAndMapContainerResource(final WorkerResourceSpec workerResourceSpec) {
		final TaskExecutorProcessSpec taskExecutorProcessSpec =
			TaskExecutorProcessUtils.processSpecFromWorkerResourceSpec(flinkConfig, workerResourceSpec);
		final Resource containerResource = Resource.newInstance(
			normalize(taskExecutorProcessSpec.getTotalProcessMemorySize().getMebiBytes(), minMemMB),
			normalize(taskExecutorProcessSpec.getCpuCores().getValue().intValue(), minVcore));

		if (resourceWithinMaxAllocation(containerResource)) {
			containerResourceToWorkerSpecs.computeIfAbsent(containerResource, ignored -> new HashSet<>())
				.add(workerResourceSpec);
			containerMemoryToContainerResource.computeIfAbsent(containerResource.getMemory(), ignored -> new HashSet<>())
				.add(containerResource);
			return containerResource;
		} else {
			LOG.warn("Requested container resource {} exceeds yarn max allocation {}. Will not allocate resource.",
				containerResource,
				Resource.newInstance(maxMemMB, maxVcore));
			return null;
		}
	}

	/**
	 * Normalize to the minimum integer that is greater or equal to 'value' and is integer multiple of 'unitValue'.
	 */
	private int normalize(final int value, final int unitValue) {
		return MathUtils.divideRoundUp(value, unitValue) * unitValue;
	}

	boolean resourceWithinMaxAllocation(final Resource resource) {
		return resource.getMemory() <= maxMemMB && resource.getVirtualCores() <= maxVcore;
	}

	enum MatchingStrategy {
		MATCH_VCORE,
		IGNORE_VCORE
	}
}
