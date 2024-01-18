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

import org.apache.flink.annotation.VisibleForTesting;
import org.apache.flink.runtime.clusterframework.types.ResourceProfile;
import org.apache.flink.runtime.scheduler.loading.LoadingWeight;
import org.apache.flink.util.Preconditions;

import java.io.Serializable;
import java.util.List;
import java.util.Objects;

/** Represents the number of required resources for a specific {@link ResourceProfile}. */
public class ResourceRequirement implements Serializable {

    private static final long serialVersionUID = 1L;

    private final ResourceProfile resourceProfile;

    private final int numberOfRequiredSlots;

    List<LoadingWeight> loadingWeights;

    private ResourceRequirement(
            ResourceProfile resourceProfile,
            int numberOfRequiredSlots,
            List<LoadingWeight> loadingWeights) {
        Preconditions.checkNotNull(resourceProfile);
        Preconditions.checkArgument(numberOfRequiredSlots >= 0);
        Preconditions.checkArgument(
                numberOfRequiredSlots == loadingWeights.size(),
                "The number of loading weight info must be equals to numberOfRequiredSlots.");

        this.resourceProfile = resourceProfile;
        this.numberOfRequiredSlots = numberOfRequiredSlots;
        this.loadingWeights = loadingWeights;
    }

    public ResourceProfile getResourceProfile() {
        return resourceProfile;
    }

    public int getNumberOfRequiredSlots() {
        return numberOfRequiredSlots;
    }

    @VisibleForTesting
    public static ResourceRequirement create(
            ResourceProfile resourceProfile, int numberOfRequiredSlots) {
        return new ResourceRequirement(
                resourceProfile,
                numberOfRequiredSlots,
                LoadingWeight.supplyEmptyLoadWeights(numberOfRequiredSlots));
    }

    public static ResourceRequirement create(
            ResourceProfile resourceProfile,
            int numberOfRequiredSlots,
            List<LoadingWeight> loadingWeights) {
        return new ResourceRequirement(resourceProfile, numberOfRequiredSlots, loadingWeights);
    }

    public List<LoadingWeight> getLoadingWeights() {
        return loadingWeights;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }
        ResourceRequirement that = (ResourceRequirement) o;
        return numberOfRequiredSlots == that.numberOfRequiredSlots
                && Objects.equals(resourceProfile, that.resourceProfile)
                && Objects.equals(loadingWeights, that.loadingWeights);
    }

    @Override
    public int hashCode() {
        return Objects.hash(resourceProfile, numberOfRequiredSlots, loadingWeights);
    }

    @Override
    public String toString() {
        return "ResourceRequirement{"
                + "resourceProfile="
                + resourceProfile
                + ", numberOfRequiredSlots="
                + numberOfRequiredSlots
                + ", loadingWeights="
                + loadingWeights
                + '}';
    }
}
