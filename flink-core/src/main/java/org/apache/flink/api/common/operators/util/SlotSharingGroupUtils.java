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

package org.apache.flink.api.common.operators.util;

import org.apache.flink.api.common.operators.ResourceSpec;
import org.apache.flink.api.common.operators.SlotSharingGroup;
import org.apache.flink.api.common.resources.ExternalResource;
import org.apache.flink.util.Preconditions;

import java.util.stream.Collectors;

/** Utils for {@link SlotSharingGroup}. */
public class SlotSharingGroupUtils {
    public static ResourceSpec extractResourceSpec(SlotSharingGroup slotSharingGroup) {
        if (!slotSharingGroup.getCpuCores().isPresent()) {
            return ResourceSpec.UNKNOWN;
        }

        Preconditions.checkState(slotSharingGroup.getCpuCores().isPresent());
        Preconditions.checkState(slotSharingGroup.getTaskHeapMemory().isPresent());
        Preconditions.checkState(slotSharingGroup.getTaskOffHeapMemory().isPresent());
        Preconditions.checkState(slotSharingGroup.getManagedMemory().isPresent());

        return ResourceSpec.newBuilder(
                        slotSharingGroup.getCpuCores().get(),
                        slotSharingGroup.getTaskHeapMemory().get())
                .setTaskOffHeapMemory(slotSharingGroup.getTaskOffHeapMemory().get())
                .setManagedMemory(slotSharingGroup.getManagedMemory().get())
                .setExtendedResources(
                        slotSharingGroup.getExternalResources().entrySet().stream()
                                .map(
                                        entry ->
                                                new ExternalResource(
                                                        entry.getKey(), entry.getValue()))
                                .collect(Collectors.toList()))
                .build();
    }
}
