/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.flink.runtime.resourcemanager.slotmanager;

import org.apache.flink.api.common.JobID;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.runtime.clusterframework.types.ResourceProfile;
import org.apache.flink.runtime.instance.InstanceID;
import org.apache.flink.runtime.slots.ResourceRequirement;
import org.apache.flink.util.Preconditions;
import org.apache.flink.util.function.TriFunction;

import java.util.Collection;
import java.util.List;
import java.util.Map;

/** Implementation of {@link ResourceAllocationStrategy} for testing purpose. */
public class TestingResourceAllocationStrategy implements ResourceAllocationStrategy {
    private final TriFunction<
                    Map<JobID, Collection<ResourceRequirement>>,
                    Map<InstanceID, Tuple2<ResourceProfile, ResourceProfile>>,
                    List<PendingTaskManager>,
                    ResourceAllocationResult>
            tryFulfillRequirementsFunction;

    private TestingResourceAllocationStrategy(
            TriFunction<
                            Map<JobID, Collection<ResourceRequirement>>,
                            Map<InstanceID, Tuple2<ResourceProfile, ResourceProfile>>,
                            List<PendingTaskManager>,
                            ResourceAllocationResult>
                    tryFulfillRequirementsFunction) {
        this.tryFulfillRequirementsFunction =
                Preconditions.checkNotNull(tryFulfillRequirementsFunction);
    }

    @Override
    public ResourceAllocationResult tryFulfillRequirements(
            Map<JobID, Collection<ResourceRequirement>> missingResources,
            Map<InstanceID, Tuple2<ResourceProfile, ResourceProfile>> registeredResources,
            List<PendingTaskManager> pendingTaskManagers) {
        return tryFulfillRequirementsFunction.apply(
                missingResources, registeredResources, pendingTaskManagers);
    }

    public static Builder newBuilder() {
        return new Builder();
    }

    public static class Builder {
        private TriFunction<
                        Map<JobID, Collection<ResourceRequirement>>,
                        Map<InstanceID, Tuple2<ResourceProfile, ResourceProfile>>,
                        List<PendingTaskManager>,
                        ResourceAllocationResult>
                tryFulfillRequirementsFunction =
                        (ignored0, ignored1, ignored2) ->
                                ResourceAllocationResult.builder().build();

        public Builder setTryFulfillRequirementsFunction(
                TriFunction<
                                Map<JobID, Collection<ResourceRequirement>>,
                                Map<InstanceID, Tuple2<ResourceProfile, ResourceProfile>>,
                                List<PendingTaskManager>,
                                ResourceAllocationResult>
                        tryFulfillRequirementsFunction) {
            this.tryFulfillRequirementsFunction = tryFulfillRequirementsFunction;
            return this;
        }

        public TestingResourceAllocationStrategy build() {
            return new TestingResourceAllocationStrategy(tryFulfillRequirementsFunction);
        }
    }
}
