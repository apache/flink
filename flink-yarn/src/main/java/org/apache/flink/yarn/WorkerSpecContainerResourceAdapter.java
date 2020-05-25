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
import java.util.TreeSet;
import java.util.stream.Collectors;

/**
 * Utility class for converting between Flink {@link WorkerResourceSpec} and Yarn {@link Resource}.
 */
class WorkerSpecContainerResourceAdapter {
	private static final Logger LOG = LoggerFactory.getLogger(WorkerSpecContainerResourceAdapter.class);

	private final Configuration flinkConfig;
	private final int minMemMB;
	private final int maxMemMB;
	private final int minVcore;
	private final int maxVcore;
	private final Map<String, Long> externalResourceConfigs;
	private final Map<WorkerResourceSpec, InternalContainerResource> workerSpecToContainerResource;
	private final Map<InternalContainerResource, Set<WorkerResourceSpec>> containerResourceToWorkerSpecs;
	private final Map<Integer, Set<InternalContainerResource>> containerMemoryToContainerResource;

	WorkerSpecContainerResourceAdapter(
		final Configuration flinkConfig,
		final int minMemMB,
		final int minVcore,
		final int maxMemMB,
		final int maxVcore,
		final Map<String, Long> externalResourceConfigs) {
		this.flinkConfig = Preconditions.checkNotNull(flinkConfig);
		this.minMemMB = minMemMB;
		this.minVcore = minVcore;
		this.maxMemMB = maxMemMB;
		this.maxVcore = maxVcore;
		this.externalResourceConfigs = Preconditions.checkNotNull(externalResourceConfigs);
		workerSpecToContainerResource = new HashMap<>();
		containerResourceToWorkerSpecs = new HashMap<>();
		containerMemoryToContainerResource = new HashMap<>();
	}

	Optional<Resource> tryComputeContainerResource(final WorkerResourceSpec workerResourceSpec) {
		final InternalContainerResource internalContainerResource = workerSpecToContainerResource.computeIfAbsent(
			Preconditions.checkNotNull(workerResourceSpec),
			this::createAndMapContainerResource);
		if (internalContainerResource != null) {
			return Optional.of(internalContainerResource.toResource());
		} else {
			return Optional.empty();
		}
	}

	Set<WorkerResourceSpec> getWorkerSpecs(final Resource containerResource, final MatchingStrategy matchingStrategy) {
		final InternalContainerResource internalContainerResource = new InternalContainerResource(containerResource);
		return getEquivalentInternalContainerResource(internalContainerResource, matchingStrategy).stream()
			.flatMap(resource -> containerResourceToWorkerSpecs.getOrDefault(resource, Collections.emptySet()).stream())
			.collect(Collectors.toSet());
	}

	Set<Resource> getEquivalentContainerResource(final Resource containerResource, final MatchingStrategy matchingStrategy) {
		final InternalContainerResource internalContainerResource = new InternalContainerResource(containerResource);
		return getEquivalentInternalContainerResource(internalContainerResource, matchingStrategy).stream()
			.map(InternalContainerResource::toResource)
			.collect(Collectors.toSet());
	}

	private Set<InternalContainerResource> getEquivalentInternalContainerResource(final InternalContainerResource internalContainerResource, final MatchingStrategy matchingStrategy) {
		// Yarn might ignore the requested vcores, depending on its configurations.
		// In such cases, we should also not matching vcores.
		final Set<InternalContainerResource> equivalentInternalContainerResources;
		switch (matchingStrategy) {
			case MATCH_VCORE:
				equivalentInternalContainerResources = Collections.singleton(internalContainerResource);
				break;
			case IGNORE_VCORE:
			default:
				equivalentInternalContainerResources = containerMemoryToContainerResource
					.getOrDefault(internalContainerResource.memory, Collections.emptySet());
				break;
		}
		return equivalentInternalContainerResources;
	}

	@Nullable
	private InternalContainerResource createAndMapContainerResource(final WorkerResourceSpec workerResourceSpec) {
		final TaskExecutorProcessSpec taskExecutorProcessSpec =
			TaskExecutorProcessUtils.processSpecFromWorkerResourceSpec(flinkConfig, workerResourceSpec);
		final InternalContainerResource internalContainerResource = new InternalContainerResource(
			normalize(taskExecutorProcessSpec.getTotalProcessMemorySize().getMebiBytes(), minMemMB),
			normalize(taskExecutorProcessSpec.getCpuCores().getValue().intValue(), minVcore),
			externalResourceConfigs);

		if (resourceWithinMaxAllocation(internalContainerResource)) {
			containerResourceToWorkerSpecs.computeIfAbsent(internalContainerResource, ignored -> new HashSet<>())
				.add(workerResourceSpec);
			containerMemoryToContainerResource.computeIfAbsent(internalContainerResource.memory, ignored -> new HashSet<>())
				.add(internalContainerResource);
			return internalContainerResource;
		} else {
			LOG.warn("Requested container resource {} exceeds yarn max allocation {}. Will not allocate resource.",
				internalContainerResource,
				new InternalContainerResource(maxMemMB, maxVcore, Collections.emptyMap()));
			return null;
		}
	}

	/**
	 * Normalize to the minimum integer that is greater or equal to 'value' and is integer multiple of 'unitValue'.
	 */
	private int normalize(final int value, final int unitValue) {
		return MathUtils.divideRoundUp(value, unitValue) * unitValue;
	}

	private boolean resourceWithinMaxAllocation(final InternalContainerResource resource) {
		return resource.memory <= maxMemMB && resource.vcores <= maxVcore;
	}

	private static void trySetExternalResources(Map<String, Long> externalResources, Resource resource) {
		for (Map.Entry<String, Long> externalResource: externalResources.entrySet()) {
			ResourceInformationReflector.INSTANCE.setResourceInformation(resource, externalResource.getKey(), externalResource.getValue());
		}
	}

	enum MatchingStrategy {
		MATCH_VCORE,
		IGNORE_VCORE
	}

	/**
	 * An {@link InternalContainerResource} corresponds to a {@link Resource}.
	 * This class is for {@link WorkerSpecContainerResourceAdapter} internal usages only, to overcome the problem that
	 * hash codes are calculated inconsistently across different {@link Resource} implementations.
	 */
	@VisibleForTesting
	static final class InternalContainerResource {
		private final int memory;
		private final int vcores;
		private final Map<String, Long> externalResources;

		@VisibleForTesting
		InternalContainerResource(final int memory, final int vcores, final Map<String, Long> externalResources) {
			this.memory = memory;
			this.vcores = vcores;
			this.externalResources = externalResources.entrySet()
						.stream()
						.filter(entry -> !entry.getValue().equals(0L))
						.collect(Collectors.toMap(Map.Entry::getKey, Map.Entry::getValue));
		}

		private InternalContainerResource(final Resource resource) {
			this(
				Preconditions.checkNotNull(resource).getMemory(),
				Preconditions.checkNotNull(resource).getVirtualCores(),
				ResourceInformationReflector.INSTANCE.getExternalResources(resource));
		}

		private Resource toResource() {
			final Resource resource = Resource.newInstance(memory, vcores);
			trySetExternalResources(externalResources, resource);
			return resource;
		}

		@Override
		public boolean equals(Object obj) {
			if (obj == this) {
				return true;
			} else if (obj instanceof InternalContainerResource) {
				final InternalContainerResource other = (InternalContainerResource) obj;
				return this.memory == other.memory && this.vcores == other.vcores && this.externalResources.equals(other.externalResources);
			}
			return false;
		}

		@Override
		public int hashCode() {
			final int prime = 31;
			int result = Integer.hashCode(memory);
			result = prime * result + Integer.hashCode(vcores);
			result = prime * result + externalResources.hashCode();
			return result;
		}

		@Override
		public String toString() {
			final StringBuilder sb = new StringBuilder();

			sb.append("<memory:")
				.append(memory)
				.append(", vCores:")
				.append(vcores);

			final Set<String> externalResourceNames = new TreeSet<>(externalResources.keySet());
			for (String externalResourceName : externalResourceNames) {
				sb.append(", ")
					.append(externalResourceName).append(": ")
					.append(externalResources.get(externalResourceName));
			}

			sb.append(">");
			return sb.toString();
		}
	}
}
