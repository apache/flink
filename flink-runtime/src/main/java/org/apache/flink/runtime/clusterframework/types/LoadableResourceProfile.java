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

/** Util class to represent {@link ResourceProfile} and specified {@link LoadingWeight}. */
@Internal
public final class LoadableResourceProfile implements WeightLoadable, Serializable {
    @Nonnull private final ResourceProfile resourceProfile;
    @Nonnull private final LoadingWeight weight;

    private LoadableResourceProfile(ResourceProfile resourceProfile, LoadingWeight weight) {
        this.resourceProfile = Preconditions.checkNotNull(resourceProfile);
        this.weight = Preconditions.checkNotNull(weight);
    }

    static LoadableResourceProfile of(ResourceProfile resourceProfile, LoadingWeight weight) {
        return new LoadableResourceProfile(resourceProfile, weight);
    }

    @Nonnull
    public ResourceProfile getResourceProfile() {
        return resourceProfile;
    }

    @Nonnull
    public LoadingWeight getLoading() {
        return weight;
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
