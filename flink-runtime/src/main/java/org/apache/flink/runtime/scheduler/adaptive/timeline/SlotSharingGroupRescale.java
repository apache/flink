/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.flink.runtime.scheduler.adaptive.timeline;

import org.apache.flink.runtime.clusterframework.types.ResourceProfile;
import org.apache.flink.runtime.instance.SlotSharingGroupId;
import org.apache.flink.runtime.jobmanager.scheduler.SlotSharingGroup;

import javax.annotation.Nullable;

import java.io.Serializable;
import java.util.Objects;

/**
 * The matching information of a requested {@link
 * org.apache.flink.runtime.jobmanager.scheduler.SlotSharingGroup}.
 */
public class SlotSharingGroupRescale implements Serializable {

    private static final long serialVersionUID = 1L;

    private final SlotSharingGroupId slotSharingGroupId;
    private final String slotSharingGroupName;
    private final ResourceProfile requiredResourceProfile;
    private Integer desiredSlots;
    private Integer minimalRequiredSlots;
    @Nullable private Integer preRescaleSlots;
    @Nullable private Integer postRescaleSlots;
    @Nullable private ResourceProfile acquiredResourceProfile;

    public SlotSharingGroupRescale(SlotSharingGroup slotSharingGroup) {
        this.slotSharingGroupId = slotSharingGroup.getSlotSharingGroupId();
        this.slotSharingGroupName = slotSharingGroup.getSlotSharingGroupName();
        this.requiredResourceProfile = slotSharingGroup.getResourceProfile();
    }

    public SlotSharingGroupId getSlotSharingGroupId() {
        return slotSharingGroupId;
    }

    public String getSlotSharingGroupName() {
        return slotSharingGroupName;
    }

    public Integer getDesiredSlots() {
        return desiredSlots;
    }

    public void setDesiredSlots(Integer desiredSlots) {
        this.desiredSlots = desiredSlots;
    }

    public Integer getMinimalRequiredSlots() {
        return minimalRequiredSlots;
    }

    public void setMinimalRequiredSlots(Integer minimalRequiredSlots) {
        this.minimalRequiredSlots = minimalRequiredSlots;
    }

    @Nullable
    public Integer getPreRescaleSlots() {
        return preRescaleSlots;
    }

    public void setPreRescaleSlots(Integer preRescaleSlots) {
        this.preRescaleSlots = preRescaleSlots;
    }

    @Nullable
    public Integer getPostRescaleSlots() {
        return postRescaleSlots;
    }

    public void setPostRescaleSlots(Integer postRescaleSlots) {
        this.postRescaleSlots = postRescaleSlots;
    }

    public ResourceProfile getRequiredResourceProfile() {
        return requiredResourceProfile;
    }

    @Nullable
    public ResourceProfile getAcquiredResourceProfile() {
        return acquiredResourceProfile;
    }

    public void setAcquiredResourceProfile(ResourceProfile acquiredResourceProfile) {
        this.acquiredResourceProfile = acquiredResourceProfile;
    }

    @Override
    public boolean equals(Object o) {
        if (o == null || getClass() != o.getClass()) {
            return false;
        }
        SlotSharingGroupRescale that = (SlotSharingGroupRescale) o;
        return Objects.equals(slotSharingGroupId, that.slotSharingGroupId)
                && Objects.equals(slotSharingGroupName, that.slotSharingGroupName)
                && Objects.equals(requiredResourceProfile, that.requiredResourceProfile)
                && Objects.equals(desiredSlots, that.desiredSlots)
                && Objects.equals(minimalRequiredSlots, that.minimalRequiredSlots)
                && Objects.equals(preRescaleSlots, that.preRescaleSlots)
                && Objects.equals(postRescaleSlots, that.postRescaleSlots)
                && Objects.equals(acquiredResourceProfile, that.acquiredResourceProfile);
    }

    @Override
    public int hashCode() {
        return Objects.hash(
                slotSharingGroupId,
                slotSharingGroupName,
                requiredResourceProfile,
                desiredSlots,
                minimalRequiredSlots,
                preRescaleSlots,
                postRescaleSlots,
                acquiredResourceProfile);
    }

    @Override
    public String toString() {
        return "SlotSharingGroupRescale{"
                + "slotSharingGroupId="
                + slotSharingGroupId
                + ", slotSharingGroupName='"
                + slotSharingGroupName
                + '\''
                + ", requiredResourceProfile="
                + requiredResourceProfile
                + ", desiredSlots="
                + desiredSlots
                + ", minimalRequiredSlots="
                + minimalRequiredSlots
                + ", preRescaleSlots="
                + preRescaleSlots
                + ", postRescaleSlots="
                + postRescaleSlots
                + ", acquiredResourceProfile="
                + acquiredResourceProfile
                + '}';
    }
}
