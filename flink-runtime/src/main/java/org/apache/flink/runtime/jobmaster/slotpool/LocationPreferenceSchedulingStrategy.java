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

package org.apache.flink.runtime.jobmaster.slotpool;

import org.apache.flink.runtime.clusterframework.types.ResourceID;
import org.apache.flink.runtime.clusterframework.types.SlotProfile;
import org.apache.flink.runtime.jobmanager.scheduler.Locality;
import org.apache.flink.runtime.jobmaster.SlotContext;
import org.apache.flink.runtime.taskmanager.TaskManagerLocation;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;

import java.util.Collection;
import java.util.HashSet;
import java.util.Iterator;
import java.util.function.BiFunction;
import java.util.function.Function;
import java.util.function.Predicate;
import java.util.stream.Stream;

/**
 * Default {@link SchedulingStrategy} which tries to match a slot with its location preferences.
 */
public class LocationPreferenceSchedulingStrategy implements SchedulingStrategy {

	private static final LocationPreferenceSchedulingStrategy INSTANCE = new LocationPreferenceSchedulingStrategy();

	LocationPreferenceSchedulingStrategy() {}

	@Nullable
	@Override
	public <IN, OUT> OUT findMatchWithLocality(
		@Nonnull SlotProfile slotProfile,
		@Nonnull Stream<IN> candidates,
		@Nonnull Function<IN, SlotContext> contextExtractor,
		@Nonnull Predicate<IN> additionalRequirementsFilter,
		@Nonnull BiFunction<IN, Locality, OUT> resultProducer) {

		final Collection<TaskManagerLocation> locationPreferences = slotProfile.getPreferredLocations();

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

	public static LocationPreferenceSchedulingStrategy getInstance() {
		return INSTANCE;
	}
}
