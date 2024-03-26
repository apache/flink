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
import org.apache.flink.runtime.clusterframework.types.LoadableResourceProfile;
import org.apache.flink.runtime.clusterframework.types.ResourceProfile;
import org.apache.flink.runtime.scheduler.loading.LoadingWeight;
import org.apache.flink.util.Preconditions;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Nonnull;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.Objects;

/** Represents the number of required resources for a specific {@link ResourceProfile}. */
public class ResourceRequirement implements Serializable {

    public static final Logger LOG = LoggerFactory.getLogger(ResourceRequirement.class);

    private static final long serialVersionUID = 1L;

    private final @Nonnull LoadableResourceProfile loadableResourceProfile;

    private final int numberOfRequiredSlots;

    private ResourceRequirement(
            LoadableResourceProfile loadableResourceProfile, int numberOfRequiredSlots) {
        this.loadableResourceProfile = Preconditions.checkNotNull(loadableResourceProfile);
        Preconditions.checkArgument(numberOfRequiredSlots >= 0);
        if (numberOfRequiredSlots == 0) {
            LOG.warn("The numberOfRequiredSlots is 0.");
        }
        this.numberOfRequiredSlots = numberOfRequiredSlots;
    }

    @Nonnull
    public ResourceProfile getResourceProfile() {
        return loadableResourceProfile.getResourceProfile();
    }

    @Nonnull
    public LoadableResourceProfile getLoadableResourceProfile() {
        return loadableResourceProfile;
    }

    public int getNumberOfRequiredSlots() {
        return numberOfRequiredSlots;
    }

    @VisibleForTesting
    public static ResourceRequirement create(
            ResourceProfile resourceProfile, int numberOfRequiredSlots) {
        return new ResourceRequirement(
                resourceProfile.toEmptyLoadsResourceProfile(), numberOfRequiredSlots);
    }

    public static ResourceRequirement create(
            LoadableResourceProfile loadableResourceProfile, int numberOfRequiredSlots) {
        return new ResourceRequirement(loadableResourceProfile, numberOfRequiredSlots);
    }

    public static ResourceRequirement create(
            Map.Entry<LoadableResourceProfile, Integer> loadableResourceProfiles) {
        return new ResourceRequirement(
                loadableResourceProfiles.getKey(), loadableResourceProfiles.getValue());
    }

    public static Collection<ResourceRequirement> create(
            Map<LoadableResourceProfile, Integer> resources) {
        final List<ResourceRequirement> result = new ArrayList<>(resources.size());
        resources.forEach(
                (loadableResourceProfile, count) ->
                        result.add(ResourceRequirement.create(loadableResourceProfile, count)));
        return result;
    }

    @Nonnull
    public LoadingWeight getLoadingUnit() {
        return loadableResourceProfile.getLoading();
    }

    @Nonnull
    public LoadingWeight getTotalLoading() {
        return getLoadingUnit().multiply(numberOfRequiredSlots);
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
                && Objects.equals(loadableResourceProfile, that.loadableResourceProfile);
    }

    @Override
    public int hashCode() {
        return Objects.hash(loadableResourceProfile, numberOfRequiredSlots);
    }

    @Override
    public String toString() {
        return "ResourceRequirement{"
                + "loadableResourceProfile="
                + loadableResourceProfile
                + ", numberOfRequiredSlots="
                + numberOfRequiredSlots
                + '}';
    }
}
