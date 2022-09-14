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

package org.apache.flink.runtime.resourcemanager;

import org.apache.flink.api.common.resources.CPUResource;
import org.apache.flink.api.common.resources.ExternalResource;
import org.apache.flink.configuration.MemorySize;
import org.apache.flink.runtime.clusterframework.TaskExecutorProcessSpec;
import org.apache.flink.runtime.clusterframework.types.ResourceProfile;
import org.apache.flink.runtime.externalresource.ExternalResourceUtils;
import org.apache.flink.util.Preconditions;

import java.io.Serializable;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.Objects;
import java.util.function.Function;
import java.util.stream.Collectors;

/**
 * Resource specification of a worker, mainly used by SlotManager requesting from ResourceManager.
 */
public final class WorkerResourceSpec implements Serializable {

    private static final long serialVersionUID = 1L;

    public static final WorkerResourceSpec ZERO = new Builder().build();

    private final CPUResource cpuCores;

    private final MemorySize taskHeapSize;

    private final MemorySize taskOffHeapSize;

    private final MemorySize networkMemSize;

    private final MemorySize managedMemSize;

    private final int numSlots;

    private final Map<String, ExternalResource> extendedResources;

    private WorkerResourceSpec(
            CPUResource cpuCores,
            MemorySize taskHeapSize,
            MemorySize taskOffHeapSize,
            MemorySize networkMemSize,
            MemorySize managedMemSize,
            int numSlots,
            Collection<ExternalResource> extendedResources) {

        this.cpuCores = Preconditions.checkNotNull(cpuCores);
        this.taskHeapSize = Preconditions.checkNotNull(taskHeapSize);
        this.taskOffHeapSize = Preconditions.checkNotNull(taskOffHeapSize);
        this.networkMemSize = Preconditions.checkNotNull(networkMemSize);
        this.managedMemSize = Preconditions.checkNotNull(managedMemSize);
        this.numSlots = numSlots;
        this.extendedResources =
                Preconditions.checkNotNull(extendedResources).stream()
                        .filter(resource -> !resource.isZero())
                        .collect(Collectors.toMap(ExternalResource::getName, Function.identity()));
        Preconditions.checkArgument(
                this.extendedResources.size() == extendedResources.size(),
                "Duplicate resource name encountered in external resources.");
    }

    public static WorkerResourceSpec fromTaskExecutorProcessSpec(
            final TaskExecutorProcessSpec taskExecutorProcessSpec) {
        Preconditions.checkNotNull(taskExecutorProcessSpec);
        return new WorkerResourceSpec(
                taskExecutorProcessSpec.getCpuCores(),
                taskExecutorProcessSpec.getTaskHeapSize(),
                taskExecutorProcessSpec.getTaskOffHeapSize(),
                taskExecutorProcessSpec.getNetworkMemSize(),
                taskExecutorProcessSpec.getManagedMemorySize(),
                taskExecutorProcessSpec.getNumSlots(),
                taskExecutorProcessSpec.getExtendedResources().values());
    }

    public static WorkerResourceSpec fromTotalResourceProfile(
            final ResourceProfile resourceProfile, final int numSlots) {
        Preconditions.checkNotNull(resourceProfile);
        return new WorkerResourceSpec(
                resourceProfile.getCpuCores(),
                resourceProfile.getTaskHeapMemory(),
                resourceProfile.getTaskOffHeapMemory(),
                resourceProfile.getNetworkMemory(),
                resourceProfile.getManagedMemory(),
                numSlots,
                resourceProfile.getExtendedResources().values());
    }

    public CPUResource getCpuCores() {
        return cpuCores;
    }

    public MemorySize getTaskHeapSize() {
        return taskHeapSize;
    }

    public MemorySize getTaskOffHeapSize() {
        return taskOffHeapSize;
    }

    public MemorySize getNetworkMemSize() {
        return networkMemSize;
    }

    public MemorySize getManagedMemSize() {
        return managedMemSize;
    }

    public MemorySize getTotalMemSize() {
        return taskHeapSize.add(taskOffHeapSize).add(networkMemSize).add(managedMemSize);
    }

    public int getNumSlots() {
        return numSlots;
    }

    public Map<String, ExternalResource> getExtendedResources() {
        return Collections.unmodifiableMap(extendedResources);
    }

    @Override
    public int hashCode() {
        return Objects.hash(
                cpuCores,
                taskHeapSize,
                taskOffHeapSize,
                networkMemSize,
                managedMemSize,
                numSlots,
                extendedResources);
    }

    @Override
    public boolean equals(Object obj) {
        if (obj == this) {
            return true;
        } else if (obj != null && obj.getClass() == WorkerResourceSpec.class) {
            WorkerResourceSpec that = (WorkerResourceSpec) obj;
            return Objects.equals(this.cpuCores, that.cpuCores)
                    && Objects.equals(this.taskHeapSize, that.taskHeapSize)
                    && Objects.equals(this.taskOffHeapSize, that.taskOffHeapSize)
                    && Objects.equals(this.networkMemSize, that.networkMemSize)
                    && Objects.equals(this.managedMemSize, that.managedMemSize)
                    && Objects.equals(this.numSlots, that.numSlots)
                    && Objects.equals(this.extendedResources, that.extendedResources);
        }
        return false;
    }

    @Override
    public String toString() {
        return "WorkerResourceSpec {"
                + "cpuCores="
                + cpuCores.getValue().doubleValue()
                + ", taskHeapSize="
                + taskHeapSize.toHumanReadableString()
                + ", taskOffHeapSize="
                + taskOffHeapSize.toHumanReadableString()
                + ", networkMemSize="
                + networkMemSize.toHumanReadableString()
                + ", managedMemSize="
                + managedMemSize.toHumanReadableString()
                + ", numSlots="
                + numSlots
                + (extendedResources.isEmpty()
                        ? ""
                        : (", "
                                + ExternalResourceUtils.generateExternalResourcesString(
                                        extendedResources.values())))
                + "}";
    }

    /** Builder for {@link WorkerResourceSpec}. */
    public static class Builder {
        private CPUResource cpuCores = new CPUResource(0.0);
        private MemorySize taskHeapSize = MemorySize.ZERO;
        private MemorySize taskOffHeapSize = MemorySize.ZERO;
        private MemorySize networkMemSize = MemorySize.ZERO;
        private MemorySize managedMemSize = MemorySize.ZERO;
        private int numSlots = 1;
        private Map<String, ExternalResource> extendedResources = new HashMap<>();

        public Builder() {}

        public Builder setCpuCores(double cpuCores) {
            this.cpuCores = new CPUResource(cpuCores);
            return this;
        }

        public Builder setTaskHeapMemoryMB(int taskHeapMemoryMB) {
            this.taskHeapSize = MemorySize.ofMebiBytes(taskHeapMemoryMB);
            return this;
        }

        public Builder setTaskOffHeapMemoryMB(int taskOffHeapMemoryMB) {
            this.taskOffHeapSize = MemorySize.ofMebiBytes(taskOffHeapMemoryMB);
            return this;
        }

        public Builder setNetworkMemoryMB(int networkMemoryMB) {
            this.networkMemSize = MemorySize.ofMebiBytes(networkMemoryMB);
            return this;
        }

        public Builder setManagedMemoryMB(int managedMemoryMB) {
            this.managedMemSize = MemorySize.ofMebiBytes(managedMemoryMB);
            return this;
        }

        public Builder setNumSlots(int numSlots) {
            this.numSlots = numSlots;
            return this;
        }

        /**
         * Add the given extended resource. The old value with the same resource name will be
         * replaced if present.
         */
        public Builder setExtendedResource(ExternalResource extendedResource) {
            this.extendedResources.put(extendedResource.getName(), extendedResource);
            return this;
        }

        /**
         * Add the given extended resources. This will discard all the previous added extended
         * resources.
         */
        public Builder setExtendedResources(Collection<ExternalResource> extendedResources) {
            this.extendedResources =
                    extendedResources.stream()
                            .collect(
                                    Collectors.toMap(
                                            ExternalResource::getName, Function.identity()));
            return this;
        }

        public WorkerResourceSpec build() {
            return new WorkerResourceSpec(
                    cpuCores,
                    taskHeapSize,
                    taskOffHeapSize,
                    networkMemSize,
                    managedMemSize,
                    numSlots,
                    extendedResources.values());
        }
    }
}
