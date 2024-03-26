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

package org.apache.flink.runtime.clusterframework.types;

import org.apache.flink.annotation.Internal;
import org.apache.flink.runtime.scheduler.loading.LoadingWeight;
import org.apache.flink.runtime.scheduler.loading.WeightLoadable;
import org.apache.flink.util.Preconditions;

import javax.annotation.Nonnull;

import java.io.Serializable;
import java.util.Objects;

/** Util class to represent {@link ResourceProfile} and expected {@link LoadingWeight}. */
@Internal
public final class LoadableResourceProfile implements WeightLoadable, Serializable {
    private @Nonnull final ResourceProfile resourceProfile;
    private @Nonnull final LoadingWeight weight;

    private LoadableResourceProfile(ResourceProfile resourceProfile, LoadingWeight weight) {
        this.resourceProfile = Preconditions.checkNotNull(resourceProfile);
        this.weight = Preconditions.checkNotNull(weight);
    }

    static LoadableResourceProfile of(
            @Nonnull ResourceProfile resourceProfile, @Nonnull LoadingWeight weight) {
        return new LoadableResourceProfile(resourceProfile, weight);
    }

    public @Nonnull ResourceProfile getResourceProfile() {
        return resourceProfile;
    }

    public @Nonnull LoadingWeight getLoading() {
        return weight;
    }

    public boolean isMatching(@Nonnull LoadableResourceProfile loadableResourceProfile) {
        return resourceProfile.isMatching(loadableResourceProfile.resourceProfile)
                && Objects.equals(weight, loadableResourceProfile.weight);
    }

    public boolean isMatchingResource(@Nonnull LoadableResourceProfile loadableResourceProfile) {
        return resourceProfile.isMatching(loadableResourceProfile.resourceProfile);
    }

    public boolean isMatchingWeight(@Nonnull LoadableResourceProfile loadableResourceProfile) {
        return weight.equals(loadableResourceProfile.getLoading());
    }

    public boolean isFromResourceProfile(ResourceProfile resourceProfile) {
        return this.resourceProfile.equals(resourceProfile);
    }

    public boolean isFromWeight(LoadingWeight loadingWeight) {
        return this.weight.equals(loadingWeight);
    }

    public boolean equalsResource(@Nonnull LoadableResourceProfile loadableResourceProfile) {
        return this.resourceProfile.equals(loadableResourceProfile.getResourceProfile());
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }
        LoadableResourceProfile that = (LoadableResourceProfile) o;
        return Objects.equals(resourceProfile, that.resourceProfile)
                && Objects.equals(weight, that.weight);
    }

    @Override
    public int hashCode() {
        return Objects.hash(resourceProfile, weight);
    }

    @Override
    public String toString() {
        return "LoadableResourceProfile{"
                + "resourceProfile="
                + resourceProfile
                + ", weight="
                + weight
                + '}';
    }
}
