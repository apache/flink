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

package org.apache.flink.runtime.scheduler.loading;

import org.apache.flink.runtime.clusterframework.types.LoadableResourceProfile;
import org.apache.flink.runtime.clusterframework.types.ResourceProfile;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;

import java.util.Objects;
import java.util.Optional;

/** The interface for allocated slots to represent the previous loading information. */
public interface PreviousWeightLoadable {

    /**
     * Returns the resource profile of the slot.
     *
     * @return the resource profile of the slot.
     */
    ResourceProfile getResourceProfile();

    /**
     * Get the resource profile with the previous laoding weight that was assigned in the last
     * profile matching. Note: only used in the {@link
     * org.apache.flink.runtime.jobmaster.slotpool.TasksBalancedLocationPreferenceSlotSelectionStrategy}
     * of {@link org.apache.flink.runtime.jobmaster.slotpool.PhysicalSlotProviderImpl}.
     *
     * @return the resource profile with the previous loading weight.
     */
    @Nonnull
    default Optional<LoadableResourceProfile> getPreviousLoadableResourceProfile() {
        return Objects.isNull(getPreviousLoadingWeight())
                ? Optional.empty()
                : Optional.of(getResourceProfile().toLoadable(getPreviousLoadingWeight()));
    }

    /**
     * Get the previous loading information in the last allocated with tasks.
     *
     * @return the resource profile with the previous loading weight.
     */
    @Nullable
    default LoadingWeight getPreviousLoadingWeight() {
        return null;
    }
}
