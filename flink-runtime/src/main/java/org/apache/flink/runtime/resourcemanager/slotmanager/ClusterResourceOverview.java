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

package org.apache.flink.runtime.resourcemanager.slotmanager;

import org.apache.flink.runtime.clusterframework.types.ResourceProfile;
import org.apache.flink.runtime.instance.InstanceID;
import org.apache.flink.util.Preconditions;

import java.util.Map;
import java.util.Optional;

/** An overview of the cluster resources. */
public class ClusterResourceOverview {
    private final Map<InstanceID, ? extends TaskManagerInfo> taskManagers;

    public ClusterResourceOverview(Map<InstanceID, ? extends TaskManagerInfo> taskManagers) {
        this.taskManagers = Preconditions.checkNotNull(taskManagers);
    }

    public int getNumberRegisteredSlots() {
        return taskManagers.values().stream().mapToInt(TaskManagerInfo::getDefaultNumSlots).sum();
    }

    public int getNumberRegisteredSlotsOf(InstanceID instanceId) {
        return Optional.ofNullable(taskManagers.get(instanceId))
                .map(TaskManagerInfo::getDefaultNumSlots)
                .orElse(0);
    }

    public int getNumberFreeSlots() {
        return taskManagers.keySet().stream().mapToInt(this::getNumberFreeSlotsOf).sum();
    }

    public int getNumberFreeSlotsOf(InstanceID instanceId) {
        return Optional.ofNullable(taskManagers.get(instanceId))
                .map(
                        taskManager ->
                                Math.max(
                                        taskManager.getDefaultNumSlots()
                                                - taskManager.getAllocatedSlots().size(),
                                        0))
                .orElse(0);
    }

    public ResourceProfile getRegisteredResource() {
        return taskManagers.values().stream()
                .map(TaskManagerInfo::getTotalResource)
                .reduce(ResourceProfile.ZERO, ResourceProfile::merge);
    }

    public ResourceProfile getRegisteredResourceOf(InstanceID instanceId) {
        return Optional.ofNullable(taskManagers.get(instanceId))
                .map(TaskManagerInfo::getTotalResource)
                .orElse(ResourceProfile.ZERO);
    }

    public ResourceProfile getFreeResource() {
        return taskManagers.values().stream()
                .map(TaskManagerInfo::getAvailableResource)
                .reduce(ResourceProfile.ZERO, ResourceProfile::merge);
    }

    public ResourceProfile getFreeResourceOf(InstanceID instanceId) {
        return Optional.ofNullable(taskManagers.get(instanceId))
                .map(TaskManagerInfo::getAvailableResource)
                .orElse(ResourceProfile.ZERO);
    }
}
