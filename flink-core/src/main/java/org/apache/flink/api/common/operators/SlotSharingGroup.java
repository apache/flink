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

package org.apache.flink.api.common.operators;

import org.apache.flink.annotation.PublicEvolving;
import org.apache.flink.api.common.resources.CPUResource;
import org.apache.flink.configuration.MemorySize;

import javax.annotation.Nullable;

import java.io.Serializable;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;

import static org.apache.flink.util.Preconditions.checkArgument;
import static org.apache.flink.util.Preconditions.checkNotNull;

/** Describe the name and the the different resource components of a slot sharing group. */
@PublicEvolving
public class SlotSharingGroup implements Serializable {
    private static final long serialVersionUID = 1L;

    private final String name;

    /** How many cpu cores are needed. Can be null only if it is unknown. */
    @Nullable // can be null only for UNKNOWN
    private final CPUResource cpuCores;

    /** How much task heap memory is needed. */
    @Nullable // can be null only for UNKNOWN
    private final MemorySize taskHeapMemory;

    /** How much task off-heap memory is needed. */
    @Nullable // can be null only for UNKNOWN
    private final MemorySize taskOffHeapMemory;

    /** How much managed memory is needed. */
    @Nullable // can be null only for UNKNOWN
    private final MemorySize managedMemory;

    /** A extensible field for user specified resources from {@link SlotSharingGroup}. */
    private final Map<String, Double> externalResources = new HashMap<>();

    private SlotSharingGroup(
            String name,
            CPUResource cpuCores,
            MemorySize taskHeapMemory,
            MemorySize taskOffHeapMemory,
            MemorySize managedMemory,
            Map<String, Double> extendedResources) {
        this.name = checkNotNull(name);
        this.cpuCores = checkNotNull(cpuCores);
        this.taskHeapMemory = checkNotNull(taskHeapMemory);
        this.taskOffHeapMemory = checkNotNull(taskOffHeapMemory);
        this.managedMemory = checkNotNull(managedMemory);
        this.externalResources.putAll(checkNotNull(extendedResources));
    }

    private SlotSharingGroup(String name) {
        this.name = checkNotNull(name);
        this.cpuCores = null;
        this.taskHeapMemory = null;
        this.taskOffHeapMemory = null;
        this.managedMemory = null;
    }

    public String getName() {
        return name;
    }

    public Optional<MemorySize> getManagedMemory() {
        return Optional.ofNullable(managedMemory);
    }

    public Optional<MemorySize> getTaskHeapMemory() {
        return Optional.ofNullable(taskHeapMemory);
    }

    public Optional<MemorySize> getTaskOffHeapMemory() {
        return Optional.ofNullable(taskOffHeapMemory);
    }

    public Optional<Double> getCpuCores() {
        return Optional.ofNullable(cpuCores)
                .map(cpuResource -> cpuResource.getValue().doubleValue());
    }

    public Map<String, Double> getExternalResources() {
        return Collections.unmodifiableMap(externalResources);
    }

    public static Builder newBuilder(String name) {
        return new Builder(name);
    }

    @Override
    public boolean equals(Object obj) {
        if (obj == this) {
            return true;
        } else if (obj != null && obj.getClass() == SlotSharingGroup.class) {
            SlotSharingGroup that = (SlotSharingGroup) obj;
            return Objects.equals(this.cpuCores, that.cpuCores)
                    && Objects.equals(taskHeapMemory, that.taskHeapMemory)
                    && Objects.equals(taskOffHeapMemory, that.taskOffHeapMemory)
                    && Objects.equals(managedMemory, that.managedMemory)
                    && Objects.equals(externalResources, that.externalResources);
        }
        return false;
    }

    @Override
    public int hashCode() {
        int result = Objects.hashCode(cpuCores);
        result = 31 * result + Objects.hashCode(taskHeapMemory);
        result = 31 * result + Objects.hashCode(taskOffHeapMemory);
        result = 31 * result + Objects.hashCode(managedMemory);
        result = 31 * result + externalResources.hashCode();
        return result;
    }

    /** Builder for the {@link SlotSharingGroup}. */
    public static class Builder {

        private String name;
        private CPUResource cpuCores;
        private MemorySize taskHeapMemory;
        private MemorySize taskOffHeapMemory;
        private MemorySize managedMemory;
        private Map<String, Double> externalResources = new HashMap<>();

        private Builder(String name) {
            this.name = name;
        }

        /** Set the CPU cores for this SlotSharingGroup. */
        public Builder setCpuCores(double cpuCores) {
            checkArgument(cpuCores > 0, "The cpu cores should be positive.");
            this.cpuCores = new CPUResource(cpuCores);
            return this;
        }

        /** Set the task heap memory for this SlotSharingGroup. */
        public Builder setTaskHeapMemory(MemorySize taskHeapMemory) {
            checkArgument(
                    taskHeapMemory.compareTo(MemorySize.ZERO) > 0,
                    "The task heap memory should be positive.");
            this.taskHeapMemory = taskHeapMemory;
            return this;
        }

        /** Set the task heap memory for this SlotSharingGroup in MB. */
        public Builder setTaskHeapMemoryMB(int taskHeapMemoryMB) {
            checkArgument(taskHeapMemoryMB > 0, "The task heap memory should be positive.");
            this.taskHeapMemory = MemorySize.ofMebiBytes(taskHeapMemoryMB);
            return this;
        }

        /** Set the task off-heap memory for this SlotSharingGroup. */
        public Builder setTaskOffHeapMemory(MemorySize taskOffHeapMemory) {
            this.taskOffHeapMemory = taskOffHeapMemory;
            return this;
        }

        /** Set the task off-heap memory for this SlotSharingGroup in MB. */
        public Builder setTaskOffHeapMemoryMB(int taskOffHeapMemoryMB) {
            this.taskOffHeapMemory = MemorySize.ofMebiBytes(taskOffHeapMemoryMB);
            return this;
        }

        /** Set the task managed memory for this SlotSharingGroup. */
        public Builder setManagedMemory(MemorySize managedMemory) {
            this.managedMemory = managedMemory;
            return this;
        }

        /** Set the task managed memory for this SlotSharingGroup in MB. */
        public Builder setManagedMemoryMB(int managedMemoryMB) {
            this.managedMemory = MemorySize.ofMebiBytes(managedMemoryMB);
            return this;
        }

        /**
         * Add the given external resource. The old value with the same resource name will be
         * replaced if present.
         */
        public Builder setExternalResource(String name, double value) {
            this.externalResources.put(name, value);
            return this;
        }

        /** Build the SlotSharingGroup. */
        public SlotSharingGroup build() {
            if (cpuCores != null && taskHeapMemory != null) {
                taskOffHeapMemory = Optional.ofNullable(taskOffHeapMemory).orElse(MemorySize.ZERO);
                managedMemory = Optional.ofNullable(managedMemory).orElse(MemorySize.ZERO);
                return new SlotSharingGroup(
                        name,
                        cpuCores,
                        taskHeapMemory,
                        taskOffHeapMemory,
                        managedMemory,
                        externalResources);
            } else if (cpuCores != null
                    || taskHeapMemory != null
                    || taskOffHeapMemory != null
                    || managedMemory != null
                    || !externalResources.isEmpty()) {
                throw new IllegalArgumentException(
                        "The cpu cores and task heap memory are required when specifying the resource of a slot sharing group. "
                                + "You need to explicitly configure them with positive value.");
            } else {
                return new SlotSharingGroup(name);
            }
        }
    }
}
