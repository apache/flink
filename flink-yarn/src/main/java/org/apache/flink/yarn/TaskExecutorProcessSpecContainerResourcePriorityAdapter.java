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

package org.apache.flink.yarn;

import org.apache.flink.runtime.clusterframework.TaskExecutorProcessSpec;
import org.apache.flink.util.Preconditions;

import org.apache.hadoop.yarn.api.records.Priority;
import org.apache.hadoop.yarn.api.records.Resource;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.HashMap;
import java.util.Map;
import java.util.Optional;

/**
 * Utility class for converting between Flink {@link TaskExecutorProcessSpec} and Yarn {@link
 * Resource} and {@link Priority}.
 */
public class TaskExecutorProcessSpecContainerResourcePriorityAdapter {

    private static final Logger LOG =
            LoggerFactory.getLogger(TaskExecutorProcessSpecContainerResourcePriorityAdapter.class);

    private final Map<TaskExecutorProcessSpec, PriorityAndResource>
            taskExecutorProcessSpecToPriorityAndResource;
    private final Map<Priority, TaskExecutorProcessSpec> priorityToTaskExecutorProcessSpec;

    private final Resource maxContainerResource;
    private final Map<String, Long> externalResourceConfigs;

    private int nextPriority = 1;

    TaskExecutorProcessSpecContainerResourcePriorityAdapter(
            final Resource maxContainerResource, final Map<String, Long> externalResourceConfigs) {
        this.maxContainerResource = Preconditions.checkNotNull(maxContainerResource);
        this.externalResourceConfigs = Preconditions.checkNotNull(externalResourceConfigs);

        taskExecutorProcessSpecToPriorityAndResource = new HashMap<>();
        priorityToTaskExecutorProcessSpec = new HashMap<>();

        validateExternalResourceConfigs();
    }

    Optional<PriorityAndResource> getPriorityAndResource(
            final TaskExecutorProcessSpec taskExecutorProcessSpec) {
        tryAdaptAndAddTaskExecutorResourceSpecIfNotExist(taskExecutorProcessSpec);
        return Optional.ofNullable(
                taskExecutorProcessSpecToPriorityAndResource.get(taskExecutorProcessSpec));
    }

    Optional<TaskExecutorProcessSpecAndResource> getTaskExecutorProcessSpecAndResource(
            Priority priority) {
        final TaskExecutorProcessSpec taskExecutorProcessSpec =
                priorityToTaskExecutorProcessSpec.get(priority);

        if (taskExecutorProcessSpec == null) {
            return Optional.empty();
        }

        final PriorityAndResource priorityAndResource =
                taskExecutorProcessSpecToPriorityAndResource.get(taskExecutorProcessSpec);
        Preconditions.checkState(priorityAndResource != null);
        Preconditions.checkState(priority.equals(priorityAndResource.getPriority()));

        return Optional.of(
                new TaskExecutorProcessSpecAndResource(
                        taskExecutorProcessSpec, priorityAndResource.getResource()));
    }

    private void validateExternalResourceConfigs() {
        final Map<String, Long> maxExternalResources =
                ResourceInformationReflector.INSTANCE.getExternalResources(maxContainerResource);
        for (Map.Entry<String, Long> entry : externalResourceConfigs.entrySet()) {
            final String resourceName = entry.getKey();
            final long configuredValue = entry.getValue();
            Preconditions.checkState(
                    maxExternalResources.containsKey(resourceName),
                    "External resource %s is not supported by the Yarn cluster.",
                    resourceName);
            Preconditions.checkState(
                    configuredValue <= maxExternalResources.get(resourceName),
                    "Configured value for external resource %s (%s) exceeds the max limitation of the Yarn cluster (%s).",
                    resourceName,
                    configuredValue,
                    maxExternalResources.get(resourceName));
        }
    }

    private void tryAdaptAndAddTaskExecutorResourceSpecIfNotExist(
            final TaskExecutorProcessSpec taskExecutorProcessSpec) {
        if (!taskExecutorProcessSpecToPriorityAndResource.containsKey(taskExecutorProcessSpec)) {
            tryAdaptResource(taskExecutorProcessSpec)
                    .ifPresent(
                            (resource) -> {
                                final Priority priority = Priority.newInstance(nextPriority++);
                                taskExecutorProcessSpecToPriorityAndResource.put(
                                        taskExecutorProcessSpec,
                                        new PriorityAndResource(priority, resource));
                                priorityToTaskExecutorProcessSpec.put(
                                        priority, taskExecutorProcessSpec);
                            });
        }
    }

    private Optional<Resource> tryAdaptResource(
            final TaskExecutorProcessSpec taskExecutorProcessSpec) {
        final Resource resource =
                Resource.newInstance(
                        taskExecutorProcessSpec.getTotalProcessMemorySize().getMebiBytes(),
                        taskExecutorProcessSpec.getCpuCores().getValue().intValue());

        if (resource.getMemory() > maxContainerResource.getMemory()
                || resource.getVirtualCores() > maxContainerResource.getVirtualCores()) {
            LOG.warn(
                    "Requested container resource ({}) exceeds the max limitation of the Yarn cluster ({}). Will not allocate resource.",
                    resource,
                    maxContainerResource);
            return Optional.empty();
        }

        for (Map.Entry<String, Long> externalResource : externalResourceConfigs.entrySet()) {
            ResourceInformationReflector.INSTANCE.setResourceInformation(
                    resource, externalResource.getKey(), externalResource.getValue());
        }

        return Optional.of(resource);
    }

    class PriorityAndResource {
        private final Priority priority;
        private final Resource resource;

        private PriorityAndResource(Priority priority, Resource resource) {
            this.priority = Preconditions.checkNotNull(priority);
            this.resource = Preconditions.checkNotNull(resource);
        }

        Priority getPriority() {
            return priority;
        }

        Resource getResource() {
            return resource;
        }
    }

    class TaskExecutorProcessSpecAndResource {
        private final TaskExecutorProcessSpec taskExecutorProcessSpec;
        private final Resource resource;

        private TaskExecutorProcessSpecAndResource(
                TaskExecutorProcessSpec taskExecutorProcessSpec, Resource resource) {
            this.taskExecutorProcessSpec = Preconditions.checkNotNull(taskExecutorProcessSpec);
            this.resource = Preconditions.checkNotNull(resource);
        }

        TaskExecutorProcessSpec getTaskExecutorProcessSpec() {
            return taskExecutorProcessSpec;
        }

        Resource getResource() {
            return resource;
        }
    }
}
