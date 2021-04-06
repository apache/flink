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

import org.apache.flink.annotation.VisibleForTesting;
import org.apache.flink.api.common.operators.ResourceSpec;
import org.apache.flink.api.common.resources.CPUResource;
import org.apache.flink.api.common.resources.ExternalResource;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.configuration.MemorySize;
import org.apache.flink.runtime.externalresource.ExternalResourceUtils;
import org.apache.flink.util.Preconditions;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;

import java.io.Serializable;
import java.math.BigDecimal;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.Objects;
import java.util.function.Function;
import java.util.stream.Collectors;

import static org.apache.flink.util.Preconditions.checkArgument;
import static org.apache.flink.util.Preconditions.checkNotNull;

/**
 * Describe the immutable resource profile of the slot, either when requiring or offering it. The
 * profile can be checked whether it can match another profile's requirement, and furthermore we may
 * calculate a matching score to decide which profile we should choose when we have lots of
 * candidate slots. It should be generated from {@link ResourceSpec} with the input and output
 * memory calculated in JobMaster.
 *
 * <p>Resource Profiles have a total ordering, defined by comparing these fields in sequence:
 *
 * <ol>
 *   <li>Memory Size
 *   <li>CPU cores
 *   <li>Extended resources
 * </ol>
 *
 * The extended resources are compared ordered by the resource names.
 */
public class ResourceProfile implements Serializable {

    private static final long serialVersionUID = 1L;

    /**
     * A ResourceProfile that indicates an unknown resource requirement. This is mainly used for
     * describing resource requirements that the exact amount of resource needed is not specified.
     * It can also be used for describing remaining resource of a multi task slot that contains
     * tasks with unknown resource requirements. It should not be used for describing total resource
     * of a task executor / slot, which should always be specific.
     */
    public static final ResourceProfile UNKNOWN = new ResourceProfile();

    /**
     * A ResourceProfile that indicates infinite resource that matches any resource requirement, for
     * testability purpose only.
     */
    @VisibleForTesting
    public static final ResourceProfile ANY =
            newBuilder()
                    .setCpuCores(Double.MAX_VALUE)
                    .setTaskHeapMemory(MemorySize.MAX_VALUE)
                    .setTaskOffHeapMemory(MemorySize.MAX_VALUE)
                    .setManagedMemory(MemorySize.MAX_VALUE)
                    .setNetworkMemory(MemorySize.MAX_VALUE)
                    .build();

    /** A ResourceProfile describing zero resources. */
    public static final ResourceProfile ZERO = newBuilder().build();

    /** Maximum number of cpu cores to output in {@link #toString()}. */
    static final BigDecimal MAX_CPU_CORE_NUMBER_TO_LOG = new BigDecimal(16384);

    /** Maximum memory resource size to output in {@link #toString()}. */
    static final MemorySize MAX_MEMORY_SIZE_TO_LOG = new MemorySize(1L << 50); // 1Pb

    // ------------------------------------------------------------------------

    /** How many cpu cores are needed. Can be null only if it is unknown. */
    @Nullable private final CPUResource cpuCores;

    /** How much task heap memory is needed. */
    @Nullable // can be null only for UNKNOWN
    private final MemorySize taskHeapMemory;

    /** How much task off-heap memory is needed. */
    @Nullable // can be null only for UNKNOWN
    private final MemorySize taskOffHeapMemory;

    /** How much managed memory is needed. */
    @Nullable // can be null only for UNKNOWN
    private final MemorySize managedMemory;

    /** How much network memory is needed. */
    @Nullable // can be null only for UNKNOWN
    private final MemorySize networkMemory;

    /** A extensible field for user specified resources from {@link ResourceSpec}. */
    private final Map<String, ExternalResource> extendedResources;

    // ------------------------------------------------------------------------

    /**
     * Creates a new ResourceProfile.
     *
     * @param cpuCores The number of CPU cores (possibly fractional, i.e., 0.2 cores)
     * @param taskHeapMemory The size of the task heap memory.
     * @param taskOffHeapMemory The size of the task off-heap memory.
     * @param managedMemory The size of the managed memory.
     * @param networkMemory The size of the network memory.
     * @param extendedResources The extended resources such as GPU and FPGA
     */
    private ResourceProfile(
            final CPUResource cpuCores,
            final MemorySize taskHeapMemory,
            final MemorySize taskOffHeapMemory,
            final MemorySize managedMemory,
            final MemorySize networkMemory,
            final Map<String, ExternalResource> extendedResources) {

        checkNotNull(cpuCores);

        this.cpuCores = cpuCores;
        this.taskHeapMemory = checkNotNull(taskHeapMemory);
        this.taskOffHeapMemory = checkNotNull(taskOffHeapMemory);
        this.managedMemory = checkNotNull(managedMemory);
        this.networkMemory = checkNotNull(networkMemory);

        this.extendedResources =
                checkNotNull(extendedResources).entrySet().stream()
                        .filter(entry -> !checkNotNull(entry.getValue()).isZero())
                        .collect(Collectors.toMap(Map.Entry::getKey, Map.Entry::getValue));
    }

    /**
     * Creates a special ResourceProfile with negative values, indicating resources are unspecified.
     */
    private ResourceProfile() {
        this.cpuCores = null;
        this.taskHeapMemory = null;
        this.taskOffHeapMemory = null;
        this.managedMemory = null;
        this.networkMemory = null;
        this.extendedResources = new HashMap<>();
    }

    // ------------------------------------------------------------------------

    /**
     * Get the cpu cores needed.
     *
     * @return The cpu cores, 1.0 means a full cpu thread
     */
    public CPUResource getCpuCores() {
        throwUnsupportedOperationExecptionIfUnknown();
        return cpuCores;
    }

    /**
     * Get the task heap memory needed.
     *
     * @return The task heap memory
     */
    public MemorySize getTaskHeapMemory() {
        throwUnsupportedOperationExecptionIfUnknown();
        return taskHeapMemory;
    }

    /**
     * Get the task off-heap memory needed.
     *
     * @return The task off-heap memory
     */
    public MemorySize getTaskOffHeapMemory() {
        throwUnsupportedOperationExecptionIfUnknown();
        return taskOffHeapMemory;
    }

    /**
     * Get the managed memory needed.
     *
     * @return The managed memory
     */
    public MemorySize getManagedMemory() {
        throwUnsupportedOperationExecptionIfUnknown();
        return managedMemory;
    }

    /**
     * Get the network memory needed.
     *
     * @return The network memory
     */
    public MemorySize getNetworkMemory() {
        throwUnsupportedOperationExecptionIfUnknown();
        return networkMemory;
    }

    /**
     * Get the total memory needed.
     *
     * @return The total memory
     */
    public MemorySize getTotalMemory() {
        throwUnsupportedOperationExecptionIfUnknown();
        return getOperatorsMemory().add(networkMemory);
    }

    /**
     * Get the memory the operators needed.
     *
     * @return The operator memory
     */
    public MemorySize getOperatorsMemory() {
        throwUnsupportedOperationExecptionIfUnknown();
        return taskHeapMemory.add(taskOffHeapMemory).add(managedMemory);
    }

    /**
     * Get the extended resources.
     *
     * @return The extended resources
     */
    public Map<String, ExternalResource> getExtendedResources() {
        throwUnsupportedOperationExecptionIfUnknown();
        return Collections.unmodifiableMap(extendedResources);
    }

    private void throwUnsupportedOperationExecptionIfUnknown() {
        if (this.equals(UNKNOWN)) {
            throw new UnsupportedOperationException();
        }
    }

    /**
     * Check whether required resource profile can be matched.
     *
     * @param required the required resource profile
     * @return true if the requirement is matched, otherwise false
     */
    public boolean isMatching(final ResourceProfile required) {
        checkNotNull(required, "Cannot check matching with null resources");
        throwUnsupportedOperationExecptionIfUnknown();

        if (this.equals(ANY)) {
            return true;
        }

        if (this.equals(required)) {
            return true;
        }

        if (required.equals(UNKNOWN)) {
            return true;
        }

        return false;
    }

    /**
     * Check whether all fields of this resource profile are no less than the given resource
     * profile.
     *
     * <p>It is not same with the total resource comparison. It return true iff each resource
     * field(cpu, task heap memory, managed memory, etc.) is no less than the respective field of
     * the given profile.
     *
     * <p>For example, assume that this profile has 1 core, 50 managed memory and 100 heap memory.
     *
     * <ol>
     *   <li>The comparison will return false if the other profile has 2 core, 10 managed memory and
     *       1000 heap memory.
     *   <li>The comparison will return true if the other profile has 1 core, 50 managed memory and
     *       150 heap memory.
     * </ol>
     *
     * @param other the other resource profile
     * @return true if all fields of this are no less than the other's, otherwise false
     */
    public boolean allFieldsNoLessThan(final ResourceProfile other) {
        checkNotNull(other, "Cannot compare null resources");

        if (this.equals(ANY)) {
            return true;
        }

        if (this.equals(other)) {
            return true;
        }

        if (this.equals(UNKNOWN)) {
            return false;
        }

        if (other.equals(UNKNOWN)) {
            return true;
        }

        if (cpuCores.getValue().compareTo(other.cpuCores.getValue()) >= 0
                && taskHeapMemory.compareTo(other.taskHeapMemory) >= 0
                && taskOffHeapMemory.compareTo(other.taskOffHeapMemory) >= 0
                && managedMemory.compareTo(other.managedMemory) >= 0
                && networkMemory.compareTo(other.networkMemory) >= 0) {

            for (Map.Entry<String, ExternalResource> resource :
                    other.extendedResources.entrySet()) {
                if (!extendedResources.containsKey(resource.getKey())
                        || extendedResources
                                        .get(resource.getKey())
                                        .getValue()
                                        .compareTo(resource.getValue().getValue())
                                < 0) {
                    return false;
                }
            }
            return true;
        }
        return false;
    }

    // ------------------------------------------------------------------------

    @Override
    public int hashCode() {
        int result = Objects.hashCode(cpuCores);
        result = 31 * result + Objects.hashCode(taskHeapMemory);
        result = 31 * result + Objects.hashCode(taskOffHeapMemory);
        result = 31 * result + Objects.hashCode(managedMemory);
        result = 31 * result + Objects.hashCode(networkMemory);
        result = 31 * result + extendedResources.hashCode();
        return result;
    }

    @Override
    public boolean equals(Object obj) {
        if (obj == this) {
            return true;
        } else if (obj != null && obj.getClass() == ResourceProfile.class) {
            ResourceProfile that = (ResourceProfile) obj;
            return Objects.equals(this.cpuCores, that.cpuCores)
                    && Objects.equals(taskHeapMemory, that.taskHeapMemory)
                    && Objects.equals(taskOffHeapMemory, that.taskOffHeapMemory)
                    && Objects.equals(managedMemory, that.managedMemory)
                    && Objects.equals(networkMemory, that.networkMemory)
                    && Objects.equals(extendedResources, that.extendedResources);
        }
        return false;
    }

    /**
     * Calculates the sum of two resource profiles.
     *
     * @param other The other resource profile to add.
     * @return The merged resource profile.
     */
    @Nonnull
    public ResourceProfile merge(final ResourceProfile other) {
        checkNotNull(other, "Cannot merge with null resources");

        if (equals(ANY) || other.equals(ANY)) {
            return ANY;
        }

        if (this.equals(UNKNOWN) || other.equals(UNKNOWN)) {
            return UNKNOWN;
        }

        Map<String, ExternalResource> resultExtendedResource = new HashMap<>(extendedResources);

        other.extendedResources.forEach(
                (String name, ExternalResource resource) -> {
                    resultExtendedResource.compute(
                            name,
                            (ignored, oldResource) ->
                                    oldResource == null ? resource : oldResource.merge(resource));
                });

        return new ResourceProfile(
                cpuCores.merge(other.cpuCores),
                taskHeapMemory.add(other.taskHeapMemory),
                taskOffHeapMemory.add(other.taskOffHeapMemory),
                managedMemory.add(other.managedMemory),
                networkMemory.add(other.networkMemory),
                resultExtendedResource);
    }

    /**
     * Subtracts another piece of resource profile from this one.
     *
     * @param other The other resource profile to subtract.
     * @return The subtracted resource profile.
     */
    public ResourceProfile subtract(final ResourceProfile other) {
        checkNotNull(other, "Cannot subtract with null resources");

        if (equals(ANY) || other.equals(ANY)) {
            return ANY;
        }

        if (this.equals(UNKNOWN) || other.equals(UNKNOWN)) {
            return UNKNOWN;
        }

        checkArgument(
                allFieldsNoLessThan(other),
                "Try to subtract an unmatched resource profile from this one.");

        Map<String, ExternalResource> resultExtendedResource = new HashMap<>(extendedResources);

        other.extendedResources.forEach(
                (String name, ExternalResource resource) ->
                        resultExtendedResource.compute(
                                name, (ignored, oldResource) -> oldResource.subtract(resource)));

        return new ResourceProfile(
                cpuCores.subtract(other.cpuCores),
                taskHeapMemory.subtract(other.taskHeapMemory),
                taskOffHeapMemory.subtract(other.taskOffHeapMemory),
                managedMemory.subtract(other.managedMemory),
                networkMemory.subtract(other.networkMemory),
                resultExtendedResource);
    }

    @Nonnull
    public ResourceProfile multiply(final int multiplier) {
        checkArgument(multiplier >= 0, "multiplier must be >= 0");
        if (equals(ANY)) {
            return ANY;
        }

        if (this.equals(UNKNOWN)) {
            return UNKNOWN;
        }

        if (multiplier == 0) {
            return ZERO;
        }

        Map<String, ExternalResource> resultExtendedResource =
                extendedResources.entrySet().stream()
                        .map(
                                entry ->
                                        Tuple2.of(
                                                entry.getKey(),
                                                entry.getValue().multiply(multiplier)))
                        .collect(Collectors.toMap(tuple -> tuple.f0, tuple -> tuple.f1));

        return new ResourceProfile(
                cpuCores.multiply(multiplier),
                taskHeapMemory.multiply(multiplier),
                taskOffHeapMemory.multiply(multiplier),
                managedMemory.multiply(multiplier),
                networkMemory.multiply(multiplier),
                resultExtendedResource);
    }

    @Override
    public String toString() {
        if (this.equals(UNKNOWN)) {
            return "ResourceProfile{UNKNOWN}";
        }

        if (this.equals(ANY)) {
            return "ResourceProfile{ANY}";
        }

        return "ResourceProfile{"
                + getResourceString()
                + (extendedResources.isEmpty()
                        ? ""
                        : (", "
                                + ExternalResourceUtils.generateExternalResourcesString(
                                        extendedResources.values())))
                + '}';
    }

    private String getResourceString() {
        String resourceStr =
                cpuCores == null || cpuCores.getValue().compareTo(MAX_CPU_CORE_NUMBER_TO_LOG) > 0
                        ? ""
                        : "cpuCores=" + cpuCores.getValue();
        resourceStr = addMemorySizeString(resourceStr, "taskHeapMemory", taskHeapMemory);
        resourceStr = addMemorySizeString(resourceStr, "taskOffHeapMemory", taskOffHeapMemory);
        resourceStr = addMemorySizeString(resourceStr, "managedMemory", managedMemory);
        resourceStr = addMemorySizeString(resourceStr, "networkMemory", networkMemory);
        return resourceStr;
    }

    private static String addMemorySizeString(String resourceStr, String name, MemorySize size) {
        String comma = resourceStr.isEmpty() ? "" : ", ";
        String memorySizeStr =
                size == null || size.compareTo(MAX_MEMORY_SIZE_TO_LOG) > 0
                        ? ""
                        : comma + name + '=' + size.toHumanReadableString();
        return resourceStr + memorySizeStr;
    }

    // ------------------------------------------------------------------------
    //  serialization
    // ------------------------------------------------------------------------

    private Object readResolve() {
        // try to preserve the singleton property for UNKNOWN and ANY

        if (this.equals(UNKNOWN)) {
            return UNKNOWN;
        }

        if (this.equals(ANY)) {
            return ANY;
        }

        return this;
    }

    // ------------------------------------------------------------------------
    //  factories
    // ------------------------------------------------------------------------

    @VisibleForTesting
    static ResourceProfile fromResourceSpec(ResourceSpec resourceSpec) {
        return fromResourceSpec(resourceSpec, MemorySize.ZERO);
    }

    public static ResourceProfile fromResourceSpec(
            ResourceSpec resourceSpec, MemorySize networkMemory) {
        if (ResourceSpec.UNKNOWN.equals(resourceSpec)) {
            return UNKNOWN;
        }

        return newBuilder()
                .setCpuCores(resourceSpec.getCpuCores())
                .setTaskHeapMemory(resourceSpec.getTaskHeapMemory())
                .setTaskOffHeapMemory(resourceSpec.getTaskOffHeapMemory())
                .setManagedMemory(resourceSpec.getManagedMemory())
                .setNetworkMemory(networkMemory)
                .setExtendedResources(resourceSpec.getExtendedResources().values())
                .build();
    }

    @VisibleForTesting
    public static ResourceProfile fromResources(final double cpuCores, final int taskHeapMemoryMB) {
        return newBuilder().setCpuCores(cpuCores).setTaskHeapMemoryMB(taskHeapMemoryMB).build();
    }

    public static Builder newBuilder() {
        return new Builder();
    }

    public static Builder newBuilder(ResourceProfile resourceProfile) {
        Preconditions.checkArgument(!resourceProfile.equals(UNKNOWN));
        return newBuilder()
                .setCpuCores(resourceProfile.cpuCores)
                .setTaskHeapMemory(resourceProfile.taskHeapMemory)
                .setTaskOffHeapMemory(resourceProfile.taskOffHeapMemory)
                .setManagedMemory(resourceProfile.managedMemory)
                .setNetworkMemory(resourceProfile.networkMemory)
                .setExtendedResources(resourceProfile.extendedResources.values());
    }

    /** Builder for the {@link ResourceProfile}. */
    public static class Builder {

        private CPUResource cpuCores = new CPUResource(0.0);
        private MemorySize taskHeapMemory = MemorySize.ZERO;
        private MemorySize taskOffHeapMemory = MemorySize.ZERO;
        private MemorySize managedMemory = MemorySize.ZERO;
        private MemorySize networkMemory = MemorySize.ZERO;
        private Map<String, ExternalResource> extendedResources = new HashMap<>();

        private Builder() {}

        public Builder setCpuCores(CPUResource cpuCores) {
            this.cpuCores = cpuCores;
            return this;
        }

        public Builder setCpuCores(double cpuCores) {
            this.cpuCores = new CPUResource(cpuCores);
            return this;
        }

        public Builder setTaskHeapMemory(MemorySize taskHeapMemory) {
            this.taskHeapMemory = taskHeapMemory;
            return this;
        }

        public Builder setTaskHeapMemoryMB(int taskHeapMemoryMB) {
            this.taskHeapMemory = MemorySize.ofMebiBytes(taskHeapMemoryMB);
            return this;
        }

        public Builder setTaskOffHeapMemory(MemorySize taskOffHeapMemory) {
            this.taskOffHeapMemory = taskOffHeapMemory;
            return this;
        }

        public Builder setTaskOffHeapMemoryMB(int taskOffHeapMemoryMB) {
            this.taskOffHeapMemory = MemorySize.ofMebiBytes(taskOffHeapMemoryMB);
            return this;
        }

        public Builder setManagedMemory(MemorySize managedMemory) {
            this.managedMemory = managedMemory;
            return this;
        }

        public Builder setManagedMemoryMB(int managedMemoryMB) {
            this.managedMemory = MemorySize.ofMebiBytes(managedMemoryMB);
            return this;
        }

        public Builder setNetworkMemory(MemorySize networkMemory) {
            this.networkMemory = networkMemory;
            return this;
        }

        public Builder setNetworkMemoryMB(int networkMemoryMB) {
            this.networkMemory = MemorySize.ofMebiBytes(networkMemoryMB);
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

        public ResourceProfile build() {
            return new ResourceProfile(
                    cpuCores,
                    taskHeapMemory,
                    taskOffHeapMemory,
                    managedMemory,
                    networkMemory,
                    extendedResources);
        }
    }
}
