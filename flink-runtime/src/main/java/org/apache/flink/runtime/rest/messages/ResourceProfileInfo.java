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

package org.apache.flink.runtime.rest.messages;

import org.apache.flink.api.common.resources.Resource;
import org.apache.flink.runtime.clusterframework.types.ResourceProfile;

import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.annotation.JsonCreator;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.annotation.JsonProperty;

import java.io.Serializable;
import java.util.Collections;
import java.util.Map;
import java.util.Objects;
import java.util.stream.Collectors;

/** Contains information of a {@link ResourceProfile}. */
public class ResourceProfileInfo implements ResponseBody, Serializable {

    private static final long serialVersionUID = 2286586486998901098L;

    public static final String FIELD_NAME_CPU = "cpuCores";

    public static final String FIELD_NAME_TASK_HEAP = "taskHeapMemory";

    public static final String FIELD_NAME_TASK_OFFHEAP = "taskOffHeapMemory";

    public static final String FIELD_NAME_MANAGED = "managedMemory";

    public static final String FIELD_NAME_NETWORK = "networkMemory";

    public static final String FIELD_NAME_EXTENDED = "extendedResources";

    @JsonProperty(FIELD_NAME_CPU)
    private final double cpu;

    @JsonProperty(FIELD_NAME_TASK_HEAP)
    private final int taskHeapMB;

    @JsonProperty(FIELD_NAME_TASK_OFFHEAP)
    private final int taskOffHeapMB;

    @JsonProperty(FIELD_NAME_MANAGED)
    private final int managedMB;

    @JsonProperty(FIELD_NAME_NETWORK)
    private final int networkMB;

    @JsonProperty(FIELD_NAME_EXTENDED)
    private final Map<String, Double> extendedResources;

    @JsonCreator
    public ResourceProfileInfo(
            @JsonProperty(FIELD_NAME_CPU) double cpu,
            @JsonProperty(FIELD_NAME_TASK_HEAP) int taskHeapMB,
            @JsonProperty(FIELD_NAME_TASK_OFFHEAP) int taskOffHeapMB,
            @JsonProperty(FIELD_NAME_MANAGED) int managedMB,
            @JsonProperty(FIELD_NAME_NETWORK) int networkMB,
            @JsonProperty(FIELD_NAME_EXTENDED) Map<String, Double> extendedResources) {
        this.cpu = cpu;
        this.taskHeapMB = taskHeapMB;
        this.taskOffHeapMB = taskOffHeapMB;
        this.managedMB = managedMB;
        this.networkMB = networkMB;
        this.extendedResources = extendedResources;
    }

    /** The ResourceProfile must not be UNKNOWN. */
    private ResourceProfileInfo(ResourceProfile resourceProfile) {
        this(
                resourceProfile.getCpuCores().getValue().doubleValue(),
                resourceProfile.getTaskHeapMemory().getMebiBytes(),
                resourceProfile.getTaskOffHeapMemory().getMebiBytes(),
                resourceProfile.getManagedMemory().getMebiBytes(),
                resourceProfile.getNetworkMemory().getMebiBytes(),
                getExetendedResources(resourceProfile.getExtendedResources()));
    }

    private ResourceProfileInfo() {
        this(-1.0, -1, -1, -1, -1, Collections.emptyMap());
    }

    public static ResourceProfileInfo fromResrouceProfile(ResourceProfile resourceProfile) {
        return resourceProfile.equals(ResourceProfile.UNKNOWN)
                ? new ResourceProfileInfo()
                : new ResourceProfileInfo(resourceProfile);
    }

    private static Map<String, Double> getExetendedResources(
            Map<String, Resource> exetendedResources) {
        return exetendedResources.entrySet().stream()
                .collect(
                        Collectors.toMap(
                                Map.Entry::getKey, e -> e.getValue().getValue().doubleValue()));
    }

    public double getCpu() {
        return cpu;
    }

    public int getTaskHeapMB() {
        return taskHeapMB;
    }

    public int getTaskOffHeapMB() {
        return taskOffHeapMB;
    }

    public int getManagedMB() {
        return managedMB;
    }

    public int getNetworkMB() {
        return networkMB;
    }

    public Map<String, Double> getExtendedResources() {
        return Collections.unmodifiableMap(extendedResources);
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }
        ResourceProfileInfo that = (ResourceProfileInfo) o;
        return cpu == that.cpu
                && taskHeapMB == that.taskHeapMB
                && taskOffHeapMB == that.taskOffHeapMB
                && managedMB == that.managedMB
                && networkMB == that.networkMB
                && Objects.equals(extendedResources, that.extendedResources);
    }

    @Override
    public int hashCode() {
        return Objects.hash(
                cpu, taskHeapMB, taskOffHeapMB, managedMB, networkMB, extendedResources);
    }
}
