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
import org.apache.flink.runtime.resourcemanager.WorkerResourceSpec;

/** Utilities for {@link SlotManager} implementations. */
public class SlotManagerUtils {

    /**
     * This must be consist with {@link
     * org.apache.flink.runtime.taskexecutor.TaskExecutorResourceUtils#generateDefaultSlotResourceProfile}.
     */
    public static ResourceProfile generateDefaultSlotResourceProfile(
            WorkerResourceSpec workerResourceSpec, int numSlotsPerWorker) {
        final ResourceProfile.Builder resourceProfileBuilder =
                ResourceProfile.newBuilder()
                        .setCpuCores(workerResourceSpec.getCpuCores().divide(numSlotsPerWorker))
                        .setTaskHeapMemory(
                                workerResourceSpec.getTaskHeapSize().divide(numSlotsPerWorker))
                        .setTaskOffHeapMemory(
                                workerResourceSpec.getTaskOffHeapSize().divide(numSlotsPerWorker))
                        .setManagedMemory(
                                workerResourceSpec.getManagedMemSize().divide(numSlotsPerWorker))
                        .setNetworkMemory(
                                workerResourceSpec.getNetworkMemSize().divide(numSlotsPerWorker));
        workerResourceSpec
                .getExtendedResources()
                .forEach(
                        (name, resource) ->
                                resourceProfileBuilder.setExtendedResource(
                                        resource.divide(numSlotsPerWorker)));
        return resourceProfileBuilder.build();
    }

    /**
     * This must be consist with {@link
     * org.apache.flink.runtime.taskexecutor.TaskExecutorResourceUtils#generateDefaultSlotResourceProfile}.
     */
    public static ResourceProfile generateDefaultSlotResourceProfile(
            ResourceProfile resourceProfile, int numSlotsPerWorker) {
        final ResourceProfile.Builder resourceProfileBuilder =
                ResourceProfile.newBuilder()
                        .setCpuCores(resourceProfile.getCpuCores().divide(numSlotsPerWorker))
                        .setTaskHeapMemory(
                                resourceProfile.getTaskHeapMemory().divide(numSlotsPerWorker))
                        .setTaskOffHeapMemory(
                                resourceProfile.getTaskOffHeapMemory().divide(numSlotsPerWorker))
                        .setManagedMemory(
                                resourceProfile.getManagedMemory().divide(numSlotsPerWorker))
                        .setNetworkMemory(
                                resourceProfile.getNetworkMemory().divide(numSlotsPerWorker));
        resourceProfile
                .getExtendedResources()
                .forEach(
                        (name, resource) ->
                                resourceProfileBuilder.setExtendedResource(
                                        resource.divide(numSlotsPerWorker)));
        return resourceProfileBuilder.build();
    }

    public static int calculateDefaultNumSlots(
            ResourceProfile totalResourceProfile, ResourceProfile defaultSlotResourceProfile) {
        // For ResourceProfile.ANY in test case, return the maximum integer
        if (totalResourceProfile.equals(ResourceProfile.ANY)) {
            return Integer.MAX_VALUE;
        }

        int numSlots = 0;
        ResourceProfile remainResource = totalResourceProfile;
        while (remainResource.allFieldsNoLessThan(defaultSlotResourceProfile)) {
            remainResource = remainResource.subtract(defaultSlotResourceProfile);
            numSlots += 1;
        }
        return numSlots;
    }

    public static ResourceProfile getEffectiveResourceProfile(
            ResourceProfile requirement, ResourceProfile defaultResourceProfile) {
        return requirement.equals(ResourceProfile.UNKNOWN) ? defaultResourceProfile : requirement;
    }

    public static ResourceProfile generateTaskManagerTotalResourceProfile(
            WorkerResourceSpec workerResourceSpec) {
        return ResourceProfile.newBuilder()
                .setCpuCores(workerResourceSpec.getCpuCores())
                .setTaskHeapMemory(workerResourceSpec.getTaskHeapSize())
                .setTaskOffHeapMemory(workerResourceSpec.getTaskOffHeapSize())
                .setManagedMemory(workerResourceSpec.getManagedMemSize())
                .setNetworkMemory(workerResourceSpec.getNetworkMemSize())
                .setExtendedResources(workerResourceSpec.getExtendedResources().values())
                .build();
    }
}
