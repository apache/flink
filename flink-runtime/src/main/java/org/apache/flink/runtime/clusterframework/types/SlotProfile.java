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
import org.apache.flink.runtime.jobmanager.scheduler.Locality;
import org.apache.flink.runtime.jobmaster.SlotContext;
import org.apache.flink.runtime.taskmanager.TaskManagerLocation;
import org.apache.flink.util.Preconditions;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashSet;
import java.util.Iterator;
import java.util.function.BiFunction;
import java.util.function.Function;
import java.util.function.Predicate;
import java.util.stream.Stream;

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
	private final Collection<AllocationID> priorAllocations;

	public SlotProfile(
		@Nonnull ResourceProfile resourceProfile,
		@Nonnull Collection<TaskManagerLocation> preferredLocations,
		@Nonnull Collection<AllocationID> priorAllocations) {

		this.resourceProfile = resourceProfile;
		this.preferredLocations = preferredLocations;
		this.priorAllocations = priorAllocations;
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
	public Collection<AllocationID> getPriorAllocations() {
		return priorAllocations;
	}

	/**
	 * Returns the matcher for this profile that helps to find slots that fit the profile.
	 */
	public ProfileToSlotContextMatcher matcher() {
		if (priorAllocations.isEmpty()) {
			return new LocalityAwareRequirementsToSlotMatcher(preferredLocations);
		} else {
			return new PreviousAllocationProfileToSlotContextMatcher(priorAllocations);
		}
	}

	/**
	 * Classes that implement this interface provide a method to match objects to somehow represent slot candidates
	 * against the {@link SlotProfile} that produced the matcher object. A matching candidate is transformed into a
	 * desired result. If the matcher does not find a matching candidate, it returns null.
	 */
	public interface ProfileToSlotContextMatcher {

		/**
		 * This method takes the candidate slots, extracts slot contexts from them, filters them by the profile
		 * requirements and potentially by additional requirements, and produces a result from a match.
		 *
		 * @param candidates                   stream of candidates to match against.
		 * @param contextExtractor             function to extract the {@link SlotContext} from the candidates.
		 * @param additionalRequirementsFilter predicate to specify additional requirements for each candidate.
		 * @param resultProducer               function to produce a result from a matching candidate input.
		 * @param <IN>                         type of the objects against we match the profile.
		 * @param <OUT>                        type of the produced output from a matching object.
		 * @return the result produced by resultProducer if a matching candidate was found or null otherwise.
		 */
		@Nullable
		<IN, OUT> OUT findMatchWithLocality(
			@Nonnull Stream<IN> candidates,
			@Nonnull Function<IN, SlotContext> contextExtractor,
			@Nonnull Predicate<IN> additionalRequirementsFilter,
			@Nonnull BiFunction<IN, Locality, OUT> resultProducer);
	}

	/**
	 * This matcher implementation is the presence of prior allocations. Prior allocations are supposed to overrule
	 * other locality requirements, such as preferred locations. Prior allocations also require strict matching and
	 * this matcher returns null if it cannot find a candidate for the same prior allocation. The background is that
	 * this will force the scheduler tor request a new slot that is guaranteed to be not the prior location of any
	 * other subtask, so that subtasks do not steal another subtasks prior allocation in case that the own prior
	 * allocation is no longer available (e.g. machine failure). This is important to enable local recovery for all
	 * tasks that can still return to their prior allocation.
	 */
	@VisibleForTesting
	public static class PreviousAllocationProfileToSlotContextMatcher implements ProfileToSlotContextMatcher {

		/** Set of prior allocations. */
		private final HashSet<AllocationID> priorAllocations;

		@VisibleForTesting
		PreviousAllocationProfileToSlotContextMatcher(@Nonnull Collection<AllocationID> priorAllocations) {
			this.priorAllocations = new HashSet<>(priorAllocations);
			Preconditions.checkState(
				this.priorAllocations.size() > 0,
				"This matcher should only be used if there are prior allocations!");
		}

		public <I, O> O findMatchWithLocality(
			@Nonnull Stream<I> candidates,
			@Nonnull Function<I, SlotContext> contextExtractor,
			@Nonnull Predicate<I> additionalRequirementsFilter,
			@Nonnull BiFunction<I, Locality, O> resultProducer) {

			Predicate<I> filterByAllocation =
				(candidate) -> priorAllocations.contains(contextExtractor.apply(candidate).getAllocationId());

			return candidates
				.filter(filterByAllocation.and(additionalRequirementsFilter))
				.findFirst()
				.map((result) -> resultProducer.apply(result, Locality.LOCAL)) // TODO introduce special locality?
				.orElse(null);
		}
	}

	/**
	 * This matcher is used whenever no prior allocation was specified in the {@link SlotProfile}. This implementation
	 * tries to achieve best possible locality if a preferred location is specified in the profile.
	 */
	@VisibleForTesting
	public static class LocalityAwareRequirementsToSlotMatcher implements ProfileToSlotContextMatcher {

		private final Collection<TaskManagerLocation> locationPreferences;

		@VisibleForTesting
		public LocalityAwareRequirementsToSlotMatcher(@Nonnull Collection<TaskManagerLocation> locationPreferences) {
			this.locationPreferences = new ArrayList<>(locationPreferences);
		}

		@Override
		public <IN, OUT> OUT findMatchWithLocality(
			@Nonnull Stream<IN> candidates,
			@Nonnull Function<IN, SlotContext> contextExtractor,
			@Nonnull Predicate<IN> additionalRequirementsFilter,
			@Nonnull BiFunction<IN, Locality, OUT> resultProducer) {

			// if we have no location preferences, we can only filter by the additional requirements.
			if (locationPreferences.isEmpty()) {
				return candidates
					.filter(additionalRequirementsFilter)
					.findFirst()
					.map((result) -> resultProducer.apply(result, Locality.UNCONSTRAINED))
					.orElse(null);
			}

			// we build up two indexes, one for resource id and one for host names of the preferred locations.
			HashSet<ResourceID> preferredResourceIDs = new HashSet<>(locationPreferences.size());
			HashSet<String> preferredFQHostNames = new HashSet<>(locationPreferences.size());

			for (TaskManagerLocation locationPreference : locationPreferences) {
				preferredResourceIDs.add(locationPreference.getResourceID());
				preferredFQHostNames.add(locationPreference.getFQDNHostname());
			}

			Iterator<IN> iterator = candidates.iterator();

			IN matchByHostName = null;
			IN matchByAdditionalRequirements = null;

			while (iterator.hasNext()) {

				IN candidate = iterator.next();
				SlotContext slotContext = contextExtractor.apply(candidate);

				// this if checks if the candidate has is a local slot
				if (preferredResourceIDs.contains(slotContext.getTaskManagerLocation().getResourceID())) {
					if (additionalRequirementsFilter.test(candidate)) {
						// we can stop, because we found a match with best possible locality.
						return resultProducer.apply(candidate, Locality.LOCAL);
					} else {
						// next candidate because this failed on the additional requirements.
						continue;
					}
				}

				// this if checks if the candidate is at least host-local, if we did not find another host-local
				// candidate before.
				if (matchByHostName == null) {
					if (preferredFQHostNames.contains(slotContext.getTaskManagerLocation().getFQDNHostname())) {
						if (additionalRequirementsFilter.test(candidate)) {
							// We remember the candidate, but still continue because there might still be a candidate
							// that is local to the desired task manager.
							matchByHostName = candidate;
						} else {
							// next candidate because this failed on the additional requirements.
							continue;
						}
					}

					// this if checks if the candidate at least fulfils the resource requirements, and is only required
					// if we did not yet find a valid candidate with better locality.
					if (matchByAdditionalRequirements == null
						&& additionalRequirementsFilter.test(candidate)) {
						// Again, we remember but continue in hope for a candidate with better locality.
						matchByAdditionalRequirements = candidate;
					}
				}
			}

			// at the end of the iteration, we return the candidate with best possible locality or null.
			if (matchByHostName != null) {
				return resultProducer.apply(matchByHostName, Locality.HOST_LOCAL);
			} else if (matchByAdditionalRequirements != null) {
				return resultProducer.apply(matchByAdditionalRequirements, Locality.NON_LOCAL);
			} else {
				return null;
			}
		}
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
}
