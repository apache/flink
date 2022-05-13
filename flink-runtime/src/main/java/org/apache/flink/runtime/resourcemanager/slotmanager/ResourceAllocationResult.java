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

package org.apache.flink.runtime.resourcemanager.slotmanager;

import org.apache.flink.api.common.JobID;
import org.apache.flink.runtime.clusterframework.types.ResourceProfile;
import org.apache.flink.runtime.instance.InstanceID;
import org.apache.flink.runtime.util.ResourceCounter;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

/** Contains the results of the {@link ResourceAllocationStrategy}. */
public class ResourceAllocationResult {
    private final Set<JobID> unfulfillableJobs;
    private final Map<JobID, Map<InstanceID, ResourceCounter>> allocationsOnRegisteredResources;
    private final List<PendingTaskManager> pendingTaskManagersToAllocate;
    private final Map<PendingTaskManagerId, Map<JobID, ResourceCounter>>
            allocationsOnPendingResources;

    private ResourceAllocationResult(
            Set<JobID> unfulfillableJobs,
            Map<JobID, Map<InstanceID, ResourceCounter>> allocationsOnRegisteredResources,
            List<PendingTaskManager> pendingTaskManagersToAllocate,
            Map<PendingTaskManagerId, Map<JobID, ResourceCounter>> allocationsOnPendingResources) {
        this.unfulfillableJobs = unfulfillableJobs;
        this.allocationsOnRegisteredResources = allocationsOnRegisteredResources;
        this.pendingTaskManagersToAllocate = pendingTaskManagersToAllocate;
        this.allocationsOnPendingResources = allocationsOnPendingResources;
    }

    public List<PendingTaskManager> getPendingTaskManagersToAllocate() {
        return Collections.unmodifiableList(pendingTaskManagersToAllocate);
    }

    public Set<JobID> getUnfulfillableJobs() {
        return Collections.unmodifiableSet(unfulfillableJobs);
    }

    public Map<JobID, Map<InstanceID, ResourceCounter>> getAllocationsOnRegisteredResources() {
        return Collections.unmodifiableMap(allocationsOnRegisteredResources);
    }

    public Map<PendingTaskManagerId, Map<JobID, ResourceCounter>>
            getAllocationsOnPendingResources() {
        return Collections.unmodifiableMap(allocationsOnPendingResources);
    }

    public static Builder builder() {
        return new Builder();
    }

    public static class Builder {
        private final Set<JobID> unfulfillableJobs = new HashSet<>();
        private final Map<JobID, Map<InstanceID, ResourceCounter>>
                allocationsOnRegisteredResources = new HashMap<>();
        private final List<PendingTaskManager> pendingTaskManagersToAllocate = new ArrayList<>();
        private final Map<PendingTaskManagerId, Map<JobID, ResourceCounter>>
                allocationsOnPendingResources = new HashMap<>();

        public Builder addUnfulfillableJob(JobID jobId) {
            this.unfulfillableJobs.add(jobId);
            return this;
        }

        public Builder addPendingTaskManagerAllocate(PendingTaskManager pendingTaskManager) {
            this.pendingTaskManagersToAllocate.add(pendingTaskManager);
            return this;
        }

        public Builder addAllocationOnPendingResource(
                JobID jobId,
                PendingTaskManagerId pendingTaskManagerId,
                ResourceProfile resourceProfile) {
            this.allocationsOnPendingResources
                    .computeIfAbsent(pendingTaskManagerId, ignored -> new HashMap<>())
                    .compute(
                            jobId,
                            (id, counter) -> {
                                if (counter == null) {
                                    return ResourceCounter.withResource(resourceProfile, 1);
                                } else {
                                    return counter.add(resourceProfile, 1);
                                }
                            });
            return this;
        }

        public Builder addAllocationOnRegisteredResource(
                JobID jobId, InstanceID instanceId, ResourceProfile resourceProfile) {
            this.allocationsOnRegisteredResources
                    .computeIfAbsent(jobId, jobID -> new HashMap<>())
                    .compute(
                            instanceId,
                            (id, counter) -> {
                                if (counter == null) {
                                    return ResourceCounter.withResource(resourceProfile, 1);
                                } else {
                                    return counter.add(resourceProfile, 1);
                                }
                            });
            return this;
        }

        public ResourceAllocationResult build() {
            return new ResourceAllocationResult(
                    unfulfillableJobs,
                    allocationsOnRegisteredResources,
                    pendingTaskManagersToAllocate,
                    allocationsOnPendingResources);
        }
    }
}
