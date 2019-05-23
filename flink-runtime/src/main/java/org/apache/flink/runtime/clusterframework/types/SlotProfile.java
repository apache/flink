/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.flink.runtime.clusterframework.types;

import org.apache.flink.runtime.jobmaster.SlotContext;
import org.apache.flink.runtime.taskmanager.TaskManagerLocation;

import javax.annotation.Nonnull;

import java.util.Collection;
import java.util.Collections;
import java.util.Set;

/**
 * A slot profile describes the profile of a slot into which a task wants to be scheduled. The profile contains
 * attributes such as resource or locality constraints, some of which may be hard or soft. A matcher can be generated
 * to filter out candidate slots by matching their {@link SlotContext} against the slot profile and, potentially,
 * further requirements.
 */
public class SlotProfile {

	/** Singleton object for a slot profile without any requirements. */
	private static final SlotProfile NO_REQUIREMENTS = noLocality(ResourceProfile.UNKNOWN);

	/** This specifies the desired resource profile for the slot. */
	@Nonnull
	private final ResourceProfile resourceProfile;

	/** This specifies the preferred locations for the slot. */
	@Nonnull
	private final Collection<TaskManagerLocation> preferredLocations;

	/** This contains desired allocation ids of the slot. */
	@Nonnull
	private final Collection<AllocationID> preferredAllocations;

	/** This contains all prior allocation ids from the whole execution graph. */
	@Nonnull
	private final Set<AllocationID> previousExecutionGraphAllocations;

	public SlotProfile(
		@Nonnull ResourceProfile resourceProfile,
		@Nonnull Collection<TaskManagerLocation> preferredLocations,
		@Nonnull Collection<AllocationID> preferredAllocations) {

		this(resourceProfile, preferredLocations, preferredAllocations, Collections.emptySet());
	}

	public SlotProfile(
		@Nonnull ResourceProfile resourceProfile,
		@Nonnull Collection<TaskManagerLocation> preferredLocations,
		@Nonnull Collection<AllocationID> preferredAllocations,
		@Nonnull Set<AllocationID> previousExecutionGraphAllocations) {

		this.resourceProfile = resourceProfile;
		this.preferredLocations = preferredLocations;
		this.preferredAllocations = preferredAllocations;
		this.previousExecutionGraphAllocations = previousExecutionGraphAllocations;
	}

	/**
	 * Returns the desired resource profile for the slot.
	 */
	@Nonnull
	public ResourceProfile getResourceProfile() {
		return resourceProfile;
	}

	/**
	 * Returns the preferred locations for the slot.
	 */
	@Nonnull
	public Collection<TaskManagerLocation> getPreferredLocations() {
		return preferredLocations;
	}

	/**
	 * Returns the desired allocation ids for the slot.
	 */
	@Nonnull
	public Collection<AllocationID> getPreferredAllocations() {
		return preferredAllocations;
	}

	/**
	 * Returns a set of all previous allocation ids from the execution graph.
	 *
	 * This is optional and can be empty if unused.
	 */
	@Nonnull
	public Set<AllocationID> getPreviousExecutionGraphAllocations() {
		return previousExecutionGraphAllocations;
	}

	/**
	 * Returns a slot profile that has no requirements.
	 */
	public static SlotProfile noRequirements() {
		return NO_REQUIREMENTS;
	}

	/**
	 * Returns a slot profile for the given resource profile, without any locality requirements.
	 */
	public static SlotProfile noLocality(ResourceProfile resourceProfile) {
		return new SlotProfile(resourceProfile, Collections.emptyList(), Collections.emptyList());
	}

	/**
	 * Returns a slot profile for the given resource profile and the preferred locations.
	 *
	 * @param resourceProfile specifying the slot requirements
	 * @param preferredLocations specifying the preferred locations
	 * @return Slot profile with the given resource profile and preferred locations
	 */
	public static SlotProfile preferredLocality(ResourceProfile resourceProfile, Collection<TaskManagerLocation> preferredLocations) {
		return new SlotProfile(resourceProfile, preferredLocations, Collections.emptyList());
	}

	/**
	 * Returns a slot profile for the given resource profile and the prior allocations.
	 *
	 * @param resourceProfile specifying the slot requirements
	 * @param priorAllocations specifying the prior allocations
	 * @return Slot profile with the given resource profile and prior allocations
	 */
	public static SlotProfile priorAllocation(ResourceProfile resourceProfile, Collection<AllocationID> priorAllocations) {
		return new SlotProfile(resourceProfile, Collections.emptyList(), priorAllocations);
	}
}
