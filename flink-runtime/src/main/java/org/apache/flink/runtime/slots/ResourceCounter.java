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
import org.apache.flink.util.Preconditions;

import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.Set;

/**
 * A counter for resources.
 *
 * <p>ResourceCounter contains a set of {@link ResourceProfile ResourceProfiles} and their
 * associated counts. The counts are always positive (> 0).
 */
public class ResourceCounter {
    public static final ResourceCounter EMPTY = new ResourceCounter(Collections.emptyMap());

    private final Map<ResourceProfile, Integer> resources;

    public ResourceCounter() {
        this(new HashMap<>());
    }

    public ResourceCounter(Map<ResourceProfile, Integer> resources) {
        this.resources = resources;
    }

    public void incrementCount(ResourceProfile profile, int increment) {
        Preconditions.checkArgument(increment > 0);
        resources.compute(
                profile,
                (ignored, currentCount) ->
                        currentCount == null ? increment : currentCount + increment);
    }

    public void decrementCount(ResourceProfile profile, int decrement) {
        Preconditions.checkArgument(decrement > 0);
        resources.compute(
                profile,
                (p, currentCount) -> {
                    Preconditions.checkState(
                            currentCount != null,
                            "Attempting to decrement count of profile %s, but no count was present.",
                            p);
                    return currentCount == decrement
                            ? null
                            : guardAgainstNegativeCount(currentCount - decrement);
                });
    }

    private static int guardAgainstNegativeCount(int count) {
        if (count < 0) {
            throw new IllegalStateException("Count is negative.");
        }
        return count;
    }

    public Map<ResourceProfile, Integer> getResourceProfilesWithCount() {
        return Collections.unmodifiableMap(resources);
    }

    public Set<ResourceProfile> getResourceProfiles() {
        return resources.keySet();
    }

    public int getResourceCount(ResourceProfile profile) {
        return resources.getOrDefault(profile, 0);
    }

    public int getResourceCount() {
        return resources.isEmpty() ? 0 : resources.values().stream().reduce(0, Integer::sum);
    }

    public ResourceCounter copy() {
        return new ResourceCounter(new HashMap<>(resources));
    }

    public boolean isEmpty() {
        return resources.isEmpty();
    }

    public void clear() {
        resources.clear();
    }
}
