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

package org.apache.flink.runtime.taskexecutor;

import org.apache.flink.configuration.ConfigOption;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.configuration.MemorySize;

import org.apache.flink.shaded.guava18.com.google.common.base.MoreObjects;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.annotation.JsonCreator;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.annotation.JsonInclude;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.annotation.JsonProperty;

import java.io.Serializable;
import java.util.Objects;

import static org.apache.flink.configuration.TaskManagerOptions.FRAMEWORK_HEAP_MEMORY;
import static org.apache.flink.configuration.TaskManagerOptions.FRAMEWORK_OFF_HEAP_MEMORY;
import static org.apache.flink.configuration.TaskManagerOptions.JVM_METASPACE;
import static org.apache.flink.configuration.TaskManagerOptions.JVM_OVERHEAD_MAX;
import static org.apache.flink.configuration.TaskManagerOptions.MANAGED_MEMORY_SIZE;
import static org.apache.flink.configuration.TaskManagerOptions.NETWORK_MEMORY_MAX;
import static org.apache.flink.configuration.TaskManagerOptions.TASK_HEAP_MEMORY;
import static org.apache.flink.configuration.TaskManagerOptions.TASK_OFF_HEAP_MEMORY;
import static org.apache.flink.runtime.taskexecutor.TaskExecutorResourceUtils.calculateTotalFlinkMemoryFromComponents;
import static org.apache.flink.runtime.taskexecutor.TaskExecutorResourceUtils.calculateTotalProcessMemoryFromComponents;

/** TaskExecutorConfiguration collects the configuration of a TaskExecutor instance. */
public class TaskExecutorMemoryConfiguration implements Serializable {
    public static final String FIELD_NAME_FRAMEWORK_HEAP = "frameworkHeap";
    public static final String FIELD_NAME_TASK_HEAP = "taskHeap";

    public static final String FIELD_NAME_FRAMEWORK_OFFHEAP = "frameworkOffHeap";
    public static final String FIELD_NAME_TASK_OFFHEAP = "taskOffHeap";

    public static final String FIELD_NAME_NETWORK_MEMORY = "networkMemory";

    public static final String FIELD_NAME_MANAGED_MEMORY = "managedMemory";

    public static final String FIELD_NAME_JVM_METASPACE = "jvmMetaspace";

    public static final String FIELD_NAME_JVM_OVERHEAD = "jvmOverhead";

    public static final String FIELD_NAME_TOTAL_FLINK_MEMORY = "totalFlinkMemory";

    public static final String FIELD_NAME_TOTAL_PROCESS_MEMORY = "totalProcessMemory";

    @JsonProperty(FIELD_NAME_FRAMEWORK_HEAP)
    @JsonInclude
    private final Long frameworkHeap;

    @JsonProperty(FIELD_NAME_TASK_HEAP)
    private final Long taskHeap;

    @JsonProperty(FIELD_NAME_FRAMEWORK_OFFHEAP)
    private final Long frameworkOffHeap;

    @JsonProperty(FIELD_NAME_TASK_OFFHEAP)
    private final Long taskOffHeap;

    @JsonProperty(FIELD_NAME_NETWORK_MEMORY)
    private final Long networkMemory;

    @JsonProperty(FIELD_NAME_MANAGED_MEMORY)
    private final Long managedMemoryTotal;

    @JsonProperty(FIELD_NAME_JVM_METASPACE)
    private final Long jvmMetaspace;

    @JsonProperty(FIELD_NAME_JVM_OVERHEAD)
    private final Long jvmOverhead;

    @JsonProperty(FIELD_NAME_TOTAL_FLINK_MEMORY)
    private final Long totalFlinkMemory;

    @JsonProperty(FIELD_NAME_TOTAL_PROCESS_MEMORY)
    private final Long totalProcessMemory;

    private static Long getConfigurationValue(
            Configuration config, ConfigOption<? extends MemorySize> option) {
        MemorySize memorySize = config.get(option);
        return memorySize != null ? memorySize.getBytes() : null;
    }

    /**
     * Factory method for initializing a TaskExecutorMemoryConfiguration based on the passed
     * Configuration.
     *
     * @param config The Configuration used for initializing the TaskExecutorMemoryConfiguration.
     * @return The newly instantiated TaskExecutorMemoryConfiguration.
     */
    public static TaskExecutorMemoryConfiguration create(Configuration config) {
        return new TaskExecutorMemoryConfiguration(
                getConfigurationValue(config, FRAMEWORK_HEAP_MEMORY),
                getConfigurationValue(config, TASK_HEAP_MEMORY),
                getConfigurationValue(config, FRAMEWORK_OFF_HEAP_MEMORY),
                getConfigurationValue(config, TASK_OFF_HEAP_MEMORY),
                getConfigurationValue(config, NETWORK_MEMORY_MAX),
                getConfigurationValue(config, MANAGED_MEMORY_SIZE),
                getConfigurationValue(config, JVM_METASPACE),
                getConfigurationValue(config, JVM_OVERHEAD_MAX),
                calculateTotalFlinkMemoryFromComponents(config),
                calculateTotalProcessMemoryFromComponents(config));
    }

    @JsonCreator
    public TaskExecutorMemoryConfiguration(
            @JsonProperty(FIELD_NAME_FRAMEWORK_HEAP) Long frameworkHeap,
            @JsonProperty(FIELD_NAME_TASK_HEAP) Long taskHeap,
            @JsonProperty(FIELD_NAME_FRAMEWORK_OFFHEAP) Long frameworkOffHeap,
            @JsonProperty(FIELD_NAME_TASK_OFFHEAP) Long taskOffHeap,
            @JsonProperty(FIELD_NAME_NETWORK_MEMORY) Long networkMemory,
            @JsonProperty(FIELD_NAME_MANAGED_MEMORY) Long managedMemoryTotal,
            @JsonProperty(FIELD_NAME_JVM_METASPACE) Long jvmMetaspace,
            @JsonProperty(FIELD_NAME_JVM_OVERHEAD) Long jvmOverhead,
            @JsonProperty(FIELD_NAME_TOTAL_FLINK_MEMORY) Long totalFlinkMemory,
            @JsonProperty(FIELD_NAME_TOTAL_PROCESS_MEMORY) Long totalProcessMemory) {
        this.frameworkHeap = frameworkHeap;
        this.taskHeap = taskHeap;
        this.frameworkOffHeap = frameworkOffHeap;
        this.taskOffHeap = taskOffHeap;
        this.networkMemory = networkMemory;
        this.managedMemoryTotal = managedMemoryTotal;
        this.jvmMetaspace = jvmMetaspace;
        this.jvmOverhead = jvmOverhead;
        this.totalFlinkMemory = totalFlinkMemory;
        this.totalProcessMemory = totalProcessMemory;
    }

    /** Returns the configured heap size used by the framework. */
    public Long getFrameworkHeap() {
        return frameworkHeap;
    }

    /** Returns the configured heap size used by the tasks. */
    public Long getTaskHeap() {
        return taskHeap;
    }

    /** Returns the configured off-heap size used by the framework. */
    public Long getFrameworkOffHeap() {
        return frameworkOffHeap;
    }

    /** Returns the configured off-heap size used by the tasks. */
    public Long getTaskOffHeap() {
        return taskOffHeap;
    }

    /** Returns the configured maximum network memory. */
    public Long getNetworkMemory() {
        return networkMemory;
    }

    /** Returns the total amount of memory reserved for by the MemoryManager. */
    public Long getManagedMemoryTotal() {
        return managedMemoryTotal;
    }

    /** Returns the maximum Metaspace size allowed for the task manager. */
    public Long getJvmMetaspace() {
        return jvmMetaspace;
    }

    /**
     * Returns the threshold for defining the maximum amount of memory used for the JVM overhead.
     */
    public Long getJvmOverhead() {
        return jvmOverhead;
    }

    /**
     * Returns the amount of memory configured to be used by Flink excluding things like JVM
     * Metaspace and other JVM overhead.
     */
    public Long getTotalFlinkMemory() {
        return totalFlinkMemory;
    }

    /**
     * Returns the total amount of memory configured to be used by the JVM including all the
     * different memory pools.
     */
    public Long getTotalProcessMemory() {
        return totalProcessMemory;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }
        TaskExecutorMemoryConfiguration that = (TaskExecutorMemoryConfiguration) o;
        return Objects.equals(frameworkHeap, that.frameworkHeap)
                && Objects.equals(taskHeap, that.taskHeap)
                && Objects.equals(frameworkOffHeap, that.frameworkOffHeap)
                && Objects.equals(taskOffHeap, that.taskOffHeap)
                && Objects.equals(networkMemory, that.networkMemory)
                && Objects.equals(managedMemoryTotal, that.managedMemoryTotal)
                && Objects.equals(jvmMetaspace, that.jvmMetaspace)
                && Objects.equals(jvmOverhead, that.jvmOverhead)
                && Objects.equals(totalFlinkMemory, that.totalFlinkMemory)
                && Objects.equals(totalProcessMemory, that.totalProcessMemory);
    }

    @Override
    public int hashCode() {
        return Objects.hash(
                frameworkHeap,
                taskHeap,
                frameworkOffHeap,
                taskOffHeap,
                networkMemory,
                managedMemoryTotal,
                jvmMetaspace,
                jvmOverhead,
                totalFlinkMemory,
                totalProcessMemory);
    }

    @Override
    public String toString() {
        return MoreObjects.toStringHelper(this)
                .add(FIELD_NAME_FRAMEWORK_HEAP, frameworkHeap)
                .add(FIELD_NAME_TASK_HEAP, taskHeap)
                .add(FIELD_NAME_FRAMEWORK_OFFHEAP, frameworkOffHeap)
                .add(FIELD_NAME_TASK_OFFHEAP, taskOffHeap)
                .add(FIELD_NAME_NETWORK_MEMORY, networkMemory)
                .add(FIELD_NAME_MANAGED_MEMORY, managedMemoryTotal)
                .add(FIELD_NAME_JVM_METASPACE, jvmMetaspace)
                .add(FIELD_NAME_JVM_OVERHEAD, jvmOverhead)
                .add(FIELD_NAME_TOTAL_FLINK_MEMORY, totalFlinkMemory)
                .add(FIELD_NAME_TOTAL_PROCESS_MEMORY, totalProcessMemory)
                .toString();
    }
}
