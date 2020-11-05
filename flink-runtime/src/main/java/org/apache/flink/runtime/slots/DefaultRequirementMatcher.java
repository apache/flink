/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.flink.runtime.slots;

import org.apache.flink.runtime.clusterframework.types.ResourceProfile;

import java.util.Collection;
import java.util.Map;
import java.util.Optional;
import java.util.function.Function;

/**
 * Default implementation of {@link RequirementMatcher}. This matcher finds the first requirement that a) is not unfulfilled and B) matches the resource profile.
 */
public class DefaultRequirementMatcher implements RequirementMatcher {
	@Override
	public Optional<ResourceProfile> match(ResourceProfile resourceProfile, Collection<Map.Entry<ResourceProfile, Integer>> totalRequirements, Function<ResourceProfile, Integer> numAssignedResourcesLookup) {
		for (Map.Entry<ResourceProfile, Integer> requirementCandidate : totalRequirements) {
			ResourceProfile requirementProfile = requirementCandidate.getKey();

			// beware the order when matching resources to requirements, because ResourceProfile.UNKNOWN (which only
			// occurs as a requirement) does not match any resource!
			if (resourceProfile.isMatching(requirementProfile) && requirementCandidate.getValue() > numAssignedResourcesLookup.apply(requirementProfile)) {
				return Optional.of(requirementProfile);
			}
		}
		return Optional.empty();

	}
}
