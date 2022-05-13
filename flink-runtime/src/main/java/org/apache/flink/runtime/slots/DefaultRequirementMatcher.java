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
import org.apache.flink.runtime.util.ResourceCounter;

import java.util.Map;
import java.util.Optional;
import java.util.function.Function;

/**
 * Default implementation of {@link RequirementMatcher}. This matcher finds the first requirement
 * that a) is not unfulfilled and B) matches the resource profile.
 */
public class DefaultRequirementMatcher implements RequirementMatcher {
    @Override
    public Optional<ResourceProfile> match(
            ResourceProfile resourceProfile,
            ResourceCounter totalRequirements,
            Function<ResourceProfile, Integer> numAssignedResourcesLookup) {
        // Short-cut for fine-grained resource management. If there is already exactly equal
        // requirement, we can directly match with it.
        if (totalRequirements.getResourceCount(resourceProfile)
                > numAssignedResourcesLookup.apply(resourceProfile)) {
            return Optional.of(resourceProfile);
        }

        for (Map.Entry<ResourceProfile, Integer> requirementCandidate :
                totalRequirements.getResourcesWithCount()) {
            ResourceProfile requirementProfile = requirementCandidate.getKey();

            // beware the order when matching resources to requirements, because
            // ResourceProfile.UNKNOWN (which only
            // occurs as a requirement) does not match any resource!
            if (resourceProfile.isMatching(requirementProfile)
                    && requirementCandidate.getValue()
                            > numAssignedResourcesLookup.apply(requirementProfile)) {
                return Optional.of(requirementProfile);
            }
        }
        return Optional.empty();
    }
}
