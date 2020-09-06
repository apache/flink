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

import org.apache.flink.annotation.VisibleForTesting;
import org.apache.flink.runtime.jobmaster.SlotContext;
import org.apache.flink.runtime.taskmanager.TaskManagerLocation;

import java.util.Collection;
import java.util.Collections;
import java.util.Set;

import static org.apache.flink.util.Preconditions.checkNotNull;

/**
 * A slot profile describes the profile of a slot into which a task wants to be scheduled. The profile contains
 * attributes such as resource or locality constraints, some of which may be hard or soft. It also contains the
 * information of resources for the physical slot to host this task slot, which can be used to allocate a physical
 * slot when no physical slot is available for this task slot. A matcher can be generated to filter out candidate
 * slots by matching their {@link SlotContext} against the slot profile and, potentially, further requirements.
 */
public class SlotProfile {

	/** Singleton object for a slot profile without any requirements. */
	private static final SlotProfile NO_REQUIREMENTS = noLocality(ResourceProfile.UNKNOWN);

	/** This specifies the desired resource profile for the task slot. */
	private final ResourceProfile taskResourceProfile;

	/** This specifies the desired resource profile for the physical slot to host this task slot. */
	private final ResourceProfile physicalSlotResourceProfile;

	/** This specifies the preferred locations for the slot. */
	private final Collection<TaskManagerLocation> preferredLocations;

	/** This contains desired allocation ids of the slot. */
	private final Collection<AllocationID> preferredAllocations;

	/** This contains all prior allocation ids from the whole execution graph. */
	private final Set<AllocationID> previousExecutionGraphAllocations;

	private SlotProfile(
			final ResourceProfile taskResourceProfile,
			final ResourceProfile physicalSlotResourceProfile,
			final Collection<TaskManagerLocation> preferredLocations,
			final Collection<AllocationID> preferredAllocations,
			final Set<AllocationID> previousExecutionGraphAllocations) {

		this.taskResourceProfile = checkNotNull(taskResourceProfile);
		this.physicalSlotResourceProfile = checkNotNull(physicalSlotResourceProfile);
		this.preferredLocations = checkNotNull(preferredLocations);
		this.preferredAllocations = checkNotNull(preferredAllocations);
		this.previousExecutionGraphAllocations = checkNotNull(previousExecutionGraphAllocations);
	}

	/**
	 * Returns the desired resource profile for the task slot.
	 */
	public ResourceProfile getTaskResourceProfile() {
		return taskResourceProfile;
	}

	/**
	 * Returns the desired resource profile for the physical slot to host this task slot.
	 */
	public ResourceProfile getPhysicalSlotResourceProfile() {
		return physicalSlotResourceProfile;
	}

	/**
	 * Returns the preferred locations for the slot.
	 */
	public Collection<TaskManagerLocation> getPreferredLocations() {
		return preferredLocations;
	}

	/**
	 * Returns the desired allocation ids for the slot.
	 */
	public Collection<AllocationID> getPreferredAllocations() {
		return preferredAllocations;
	}

	/**
	 * Returns a set of all previous allocation ids from the execution graph.
	 *
	 * <p>This is optional and can be empty if unused.
	 */
	public Set<AllocationID> getPreviousExecutionGraphAllocations() {
		return previousExecutionGraphAllocations;
	}

	/**
	 * Returns a slot profile that has no requirements.
	 */
	@VisibleForTesting
	public static SlotProfile noRequirements() {
		return NO_REQUIREMENTS;
	}

	/**
	 * Returns a slot profile for the given resource profile, without any locality requirements.
	 */
	@VisibleForTesting
	public static SlotProfile noLocality(ResourceProfile resourceProfile) {
		return preferredLocality(resourceProfile, Collections.emptyList());
	}

	/**
	 * Returns a slot profile for the given resource profile and the preferred locations.
	 *
	 * @param resourceProfile specifying the slot requirements
	 * @param preferredLocations specifying the preferred locations
	 * @return Slot profile with the given resource profile and preferred locations
	 */
	@VisibleForTesting
	public static SlotProfile preferredLocality(
			final ResourceProfile resourceProfile,
			final Collection<TaskManagerLocation> preferredLocations) {

		return priorAllocation(
			resourceProfile,
			resourceProfile,
			preferredLocations,
			Collections.emptyList(),
			Collections.emptySet());
	}

	/**
	 * Returns a slot profile for the given resource profile, prior allocations and
	 * all prior allocation ids from the whole execution graph.
	 *
	 * @param taskResourceProfile specifying the required resources for the task slot
	 * @param physicalSlotResourceProfile specifying the required resources for the physical slot to host this task slot
	 * @param preferredLocations specifying the preferred locations
	 * @param priorAllocations specifying the prior allocations
	 * @param previousExecutionGraphAllocations specifying all prior allocation ids from the whole execution graph
	 * @return Slot profile with all the given information
	 */
	public static SlotProfile priorAllocation(
			final ResourceProfile taskResourceProfile,
			final ResourceProfile physicalSlotResourceProfile,
			final Collection<TaskManagerLocation> preferredLocations,
			final Collection<AllocationID> priorAllocations,
			final Set<AllocationID> previousExecutionGraphAllocations) {

		return new SlotProfile(
			taskResourceProfile,
			physicalSlotResourceProfile,
			preferredLocations,
			priorAllocations,
			previousExecutionGraphAllocations);
	}
}
