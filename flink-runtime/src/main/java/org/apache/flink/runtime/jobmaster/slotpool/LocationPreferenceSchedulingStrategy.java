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
import org.apache.flink.runtime.jobmaster.SlotInfo;
import org.apache.flink.runtime.taskmanager.TaskManagerLocation;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;

import java.util.Collection;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;
import java.util.function.BiFunction;
import java.util.function.Function;
import java.util.function.Predicate;
import java.util.function.Supplier;
import java.util.stream.Stream;

/**
 * Default {@link SchedulingStrategy} which tries to match a slot with its location preferences.
 */
public class LocationPreferenceSchedulingStrategy implements SchedulingStrategy {

	private static final LocationPreferenceSchedulingStrategy INSTANCE = new LocationPreferenceSchedulingStrategy();

	/**
	 * Calculates the candidate's locality score.
	 */
	private static final BiFunction<Integer, Integer, Integer> LOCALITY_EVALUATION_FUNCTION = (localWeigh, hostLocalWeigh) -> localWeigh * 10 + hostLocalWeigh;

	LocationPreferenceSchedulingStrategy() {}

	@Nullable
	@Override
	public <IN, OUT> OUT findMatchWithLocality(
			@Nonnull SlotProfile slotProfile,
			@Nonnull Supplier<Stream<IN>> candidates,
			@Nonnull Function<IN, SlotInfo> contextExtractor,
			@Nonnull Predicate<IN> additionalRequirementsFilter,
			@Nonnull BiFunction<IN, Locality, OUT> resultProducer) {

		return doFindMatchWithLocality(
			slotProfile,
			candidates.get(),
			contextExtractor,
			additionalRequirementsFilter,
			resultProducer);
	}

	@Nullable
	protected  <IN, OUT> OUT doFindMatchWithLocality(
		@Nonnull SlotProfile slotProfile,
		@Nonnull Stream<IN> candidates,
		@Nonnull Function<IN, SlotInfo> contextExtractor,
		@Nonnull Predicate<IN> additionalRequirementsFilter,
		@Nonnull BiFunction<IN, Locality, OUT> resultProducer) {
		Collection<TaskManagerLocation> locationPreferences = slotProfile.getPreferredLocations();

		// if we have no location preferences, we can only filter by the additional requirements.
		if (locationPreferences.isEmpty()) {
			return candidates
				.filter(additionalRequirementsFilter)
				.findFirst()
				.map((result) -> resultProducer.apply(result, Locality.UNCONSTRAINED))
				.orElse(null);
		}

		// we build up two indexes, one for resource id and one for host names of the preferred locations.
		final Map<ResourceID, Integer> preferredResourceIDs = new HashMap<>(locationPreferences.size());
		final Map<String, Integer> preferredFQHostNames = new HashMap<>(locationPreferences.size());

		for (TaskManagerLocation locationPreference : locationPreferences) {
			preferredResourceIDs.merge(locationPreference.getResourceID(), 1, Integer::sum);
			preferredFQHostNames.merge(locationPreference.getFQDNHostname(), 1, Integer::sum);
		}

		Iterator<IN> iterator = candidates.iterator();

		IN bestCandidate = null;
		int bestCandidateScore = Integer.MIN_VALUE;
		Locality bestCandidateLocality = null;

		while (iterator.hasNext()) {
			IN candidate = iterator.next();
			if (additionalRequirementsFilter.test(candidate)) {
				SlotInfo slotContext = contextExtractor.apply(candidate);

				// this gets candidate is local-weigh
				Integer localWeigh = preferredResourceIDs.getOrDefault(slotContext.getTaskManagerLocation().getResourceID(), 0);

				// this gets candidate is host-local-weigh
				Integer hostLocalWeigh = preferredFQHostNames.getOrDefault(slotContext.getTaskManagerLocation().getFQDNHostname(), 0);

				int candidateScore = LOCALITY_EVALUATION_FUNCTION.apply(localWeigh, hostLocalWeigh);
				if (candidateScore > bestCandidateScore) {
					bestCandidateScore = candidateScore;
					bestCandidate = candidate;
					bestCandidateLocality = localWeigh > 0 ? Locality.LOCAL : hostLocalWeigh > 0 ? Locality.HOST_LOCAL : Locality.NON_LOCAL;
				}
			}
		}

		// at the end of the iteration, we return the candidate with best possible locality or null.
		if (bestCandidate != null) {
			return resultProducer.apply(bestCandidate, bestCandidateLocality);
		} else {
			return null;
		}
	}

	public static LocationPreferenceSchedulingStrategy getInstance() {
		return INSTANCE;
	}
}
