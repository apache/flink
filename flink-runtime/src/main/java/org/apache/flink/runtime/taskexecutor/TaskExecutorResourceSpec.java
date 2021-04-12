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

import org.apache.flink.api.common.resources.CPUResource;
import org.apache.flink.api.common.resources.ExternalResource;
import org.apache.flink.configuration.MemorySize;
import org.apache.flink.util.Preconditions;

import java.util.Collection;
import java.util.Collections;
import java.util.Map;
import java.util.function.Function;
import java.util.stream.Collectors;

/**
 * Specification of resources to use in running {@link
 * org.apache.flink.runtime.taskexecutor.TaskExecutor}.
 */
public class TaskExecutorResourceSpec {
    private final CPUResource cpuCores;

    private final MemorySize taskHeapSize;

    private final MemorySize taskOffHeapSize;

    private final MemorySize networkMemSize;

    private final MemorySize managedMemorySize;

    private final Map<String, ExternalResource> extendedResources;

    public TaskExecutorResourceSpec(
            CPUResource cpuCores,
            MemorySize taskHeapSize,
            MemorySize taskOffHeapSize,
            MemorySize networkMemSize,
            MemorySize managedMemorySize,
            Collection<ExternalResource> extendedResources) {
        this.cpuCores = cpuCores;
        this.taskHeapSize = taskHeapSize;
        this.taskOffHeapSize = taskOffHeapSize;
        this.networkMemSize = networkMemSize;
        this.managedMemorySize = managedMemorySize;
        this.extendedResources =
                Preconditions.checkNotNull(extendedResources).stream()
                        .filter(resource -> !resource.isZero())
                        .collect(Collectors.toMap(ExternalResource::getName, Function.identity()));
        Preconditions.checkArgument(
                this.extendedResources.size() == extendedResources.size(),
                "Duplicate resource name encountered in external resources.");
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

    public MemorySize getManagedMemorySize() {
        return managedMemorySize;
    }

    public Map<String, ExternalResource> getExtendedResources() {
        return Collections.unmodifiableMap(extendedResources);
    }
}
