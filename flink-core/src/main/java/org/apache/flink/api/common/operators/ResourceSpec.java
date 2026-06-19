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

import org.apache.flink.annotation.Internal;
import org.apache.flink.api.common.resources.CPUResource;
import org.apache.flink.api.common.resources.ExternalResource;
import org.apache.flink.configuration.MemorySize;

import javax.annotation.Nullable;

import java.io.Serializable;
import java.math.BigDecimal;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.function.Function;
import java.util.stream.Collectors;

import static org.apache.flink.util.Preconditions.checkArgument;
import static org.apache.flink.util.Preconditions.checkNotNull;

/**
 * Describe the different resource factors of the operator with UDF.
 *
 * <p>Resource provides {@link #merge(ResourceSpec)} method for chained operators when generating
 * job graph.
 *
 * <p>Resource provides {@link #lessThanOrEqual(ResourceSpec)} method to compare these fields in
 * sequence:
 *
 * <ol>
 *   <li>CPU cores
 *   <li>Task Heap Memory
 *   <li>Task Off-Heap Memory
 *   <li>Managed Memory
 *   <li>Extended resources
 * </ol>
 */
@Internal
public final class ResourceSpec implements Serializable {

    private static final long serialVersionUID = 1L;

    /** A ResourceSpec that indicates an unknown set of resources. */
    public static final ResourceSpec UNKNOWN = new ResourceSpec();

    /**
     * The default ResourceSpec used for operators and transformation functions. Currently equal to
     * {@link #UNKNOWN}.
     */
    public static final ResourceSpec DEFAULT = UNKNOWN;

    /** A ResourceSpec that indicates zero amount of resources. */
    public static final ResourceSpec ZERO =
            new ResourceSpec(
                    new CPUResource(0.0),
                    MemorySize.ZERO,
                    MemorySize.ZERO,
                    MemorySize.ZERO,
                    Collections.emptyMap());

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

    private final Map<String, ExternalResource> extendedResources;

    private ResourceSpec(
            final CPUResource cpuCores,
            final MemorySize taskHeapMemory,
            final MemorySize taskOffHeapMemory,
            final MemorySize managedMemory,
            final Map<String, ExternalResource> extendedResources) {

        checkNotNull(cpuCores);

        this.cpuCores = cpuCores;
        this.taskHeapMemory = checkNotNull(taskHeapMemory);
        this.taskOffHeapMemory = checkNotNull(taskOffHeapMemory);
        this.managedMemory = checkNotNull(managedMemory);

        this.extendedResources =
                checkNotNull(extendedResources).entrySet().stream()
                        .filter(entry -> !checkNotNull(entry.getValue()).isZero())
                        .collect(Collectors.toMap(Map.Entry::getKey, Map.Entry::getValue));
    }

    /** Creates a new ResourceSpec with all fields unknown. */
    private ResourceSpec() {
        this.cpuCores = null;
        this.taskHeapMemory = null;
        this.taskOffHeapMemory = null;
        this.managedMemory = null;
        this.extendedResources = new HashMap<>();
    }

    /**
     * Used by system internally to merge the other resources of chained operators when generating
     * the job graph.
     *
     * @param other Reference to resource to merge in.
     * @return The new resource with merged values.
     */
    public ResourceSpec merge(final ResourceSpec other) {
        checkNotNull(other, "Cannot merge with null resources");

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

        return new ResourceSpec(
                this.cpuCores.merge(other.cpuCores),
                this.taskHeapMemory.add(other.taskHeapMemory),
                this.taskOffHeapMemory.add(other.taskOffHeapMemory),
                this.managedMemory.add(other.managedMemory),
                resultExtendedResource);
    }

    /**
     * Subtracts another resource spec from this one.
     *
     * @param other The other resource spec to subtract.
     * @return The subtracted resource spec.
     */
    public ResourceSpec subtract(final ResourceSpec other) {
        checkNotNull(other, "Cannot subtract null resources");

        if (this.equals(UNKNOWN) || other.equals(UNKNOWN)) {
            return UNKNOWN;
        }

        checkArgument(
                other.lessThanOrEqual(this),
                "Cannot subtract a larger ResourceSpec from this one.");

        Map<String, ExternalResource> resultExtendedResources = new HashMap<>(extendedResources);
        for (ExternalResource resource : other.extendedResources.values()) {
            resultExtendedResources.merge(
                    resource.getName(), resource, (v1, v2) -> v1.subtract(v2));
        }

        return new ResourceSpec(
                this.cpuCores.subtract(other.cpuCores),
                this.taskHeapMemory.subtract(other.taskHeapMemory),
                this.taskOffHeapMemory.subtract(other.taskOffHeapMemory),
                this.managedMemory.subtract(other.managedMemory),
                resultExtendedResources);
    }

    public CPUResource getCpuCores() {
        throwUnsupportedOperationExceptionIfUnknown();
        return this.cpuCores;
    }

    public MemorySize getTaskHeapMemory() {
        throwUnsupportedOperationExceptionIfUnknown();
        return this.taskHeapMemory;
    }

    public MemorySize getTaskOffHeapMemory() {
        throwUnsupportedOperationExceptionIfUnknown();
        return taskOffHeapMemory;
    }

    public MemorySize getManagedMemory() {
        throwUnsupportedOperationExceptionIfUnknown();
        return managedMemory;
    }

    public Optional<ExternalResource> getExtendedResource(String name) {
        throwUnsupportedOperationExceptionIfUnknown();
        return Optional.ofNullable(extendedResources.get(name));
    }

    public Map<String, ExternalResource> getExtendedResources() {
        throwUnsupportedOperationExceptionIfUnknown();
        return Collections.unmodifiableMap(extendedResources);
    }

    private void throwUnsupportedOperationExceptionIfUnknown() {
        if (this.equals(UNKNOWN)) {
            throw new UnsupportedOperationException();
        }
    }

    /**
     * Checks the current resource less than or equal with the other resource by comparing all the
     * fields in the resource.
     *
     * @param other The resource to compare
     * @return True if current resource is less than or equal with the other resource, otherwise
     *     return false.
     */
    public boolean lessThanOrEqual(final ResourceSpec other) {
        checkNotNull(other, "Cannot compare with null resources");

        if (this.equals(UNKNOWN) && other.equals(UNKNOWN)) {
            return true;
        } else if (this.equals(UNKNOWN) || other.equals(UNKNOWN)) {
            throw new IllegalArgumentException(
                    "Cannot compare specified resources with UNKNOWN resources.");
        }

        int cmp1 = this.cpuCores.getValue().compareTo(other.getCpuCores().getValue());
        int cmp2 = this.taskHeapMemory.compareTo(other.taskHeapMemory);
        int cmp3 = this.taskOffHeapMemory.compareTo(other.taskOffHeapMemory);
        int cmp4 = this.managedMemory.compareTo(other.managedMemory);
        if (cmp1 <= 0 && cmp2 <= 0 && cmp3 <= 0 && cmp4 <= 0) {
            for (ExternalResource resource : extendedResources.values()) {
                if (!other.extendedResources.containsKey(resource.getName())
                        || other.extendedResources
                                        .get(resource.getName())
                                        .getValue()
                                        .compareTo(resource.getValue())
                                < 0) {
                    return false;
                }
            }
            return true;
        }
        return false;
    }

    @Override
    public boolean equals(Object obj) {
        if (obj == this) {
            return true;
        } else if (obj != null && obj.getClass() == ResourceSpec.class) {
            ResourceSpec that = (ResourceSpec) obj;
            return Objects.equals(this.cpuCores, that.cpuCores)
                    && Objects.equals(this.taskHeapMemory, that.taskHeapMemory)
                    && Objects.equals(this.taskOffHeapMemory, that.taskOffHeapMemory)
                    && Objects.equals(this.managedMemory, that.managedMemory)
                    && Objects.equals(extendedResources, that.extendedResources);
        } else {
            return false;
        }
    }

    @Override
    public int hashCode() {
        int result = Objects.hashCode(cpuCores);
        result = 31 * result + Objects.hashCode(taskHeapMemory);
        result = 31 * result + Objects.hashCode(taskOffHeapMemory);
        result = 31 * result + Objects.hashCode(managedMemory);
        result = 31 * result + extendedResources.hashCode();
        return result;
    }

    @Override
    public String toString() {
        if (this.equals(UNKNOWN)) {
            return "ResourceSpec{UNKNOWN}";
        }

        final StringBuilder extResources = new StringBuilder(extendedResources.size() * 10);
        for (Map.Entry<String, ExternalResource> resource : extendedResources.entrySet()) {
            extResources
                    .append(", ")
                    .append(resource.getKey())
                    .append('=')
                    .append(resource.getValue().getValue());
        }
        return "ResourceSpec{"
                + "cpuCores="
                + cpuCores.getValue()
                + ", taskHeapMemory="
                + taskHeapMemory.toHumanReadableString()
                + ", taskOffHeapMemory="
                + taskOffHeapMemory.toHumanReadableString()
                + ", managedMemory="
                + managedMemory.toHumanReadableString()
                + extResources
                + '}';
    }

    // ------------------------------------------------------------------------
    //  serialization
    // ------------------------------------------------------------------------

    private Object readResolve() {
        // try to preserve the singleton property for UNKNOWN
        return this.equals(UNKNOWN) ? UNKNOWN : this;
    }

    // ------------------------------------------------------------------------
    //  builder
    // ------------------------------------------------------------------------

    public static Builder newBuilder(double cpuCores, int taskHeapMemoryMB) {
        return new Builder(new CPUResource(cpuCores), MemorySize.ofMebiBytes(taskHeapMemoryMB));
    }

    public static Builder newBuilder(double cpuCores, MemorySize taskHeapMemory) {
        return new Builder(new CPUResource(cpuCores), taskHeapMemory);
    }

    /** Builder for the {@link ResourceSpec}. */
    public static class Builder {

        private CPUResource cpuCores;
        private MemorySize taskHeapMemory;
        private MemorySize taskOffHeapMemory = MemorySize.ZERO;
        private MemorySize managedMemory = MemorySize.ZERO;
        private Map<String, ExternalResource> extendedResources = new HashMap<>();

        private Builder(CPUResource cpuCores, MemorySize taskHeapMemory) {
            this.cpuCores = cpuCores;
            this.taskHeapMemory = taskHeapMemory;
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

        public ResourceSpec build() {
            checkArgument(cpuCores.getValue().compareTo(BigDecimal.ZERO) > 0);
            checkArgument(taskHeapMemory.compareTo(MemorySize.ZERO) > 0);
            return new ResourceSpec(
                    cpuCores, taskHeapMemory, taskOffHeapMemory, managedMemory, extendedResources);
        }
    }
}
