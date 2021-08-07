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

package org.apache.flink.runtime.io.network.partition;

import org.apache.flink.runtime.clusterframework.types.ResourceID;
import org.apache.flink.runtime.deployment.ResultPartitionDeploymentDescriptor;

import java.util.Collection;
import java.util.Collections;

/** No-op implementation of {@link JobMasterPartitionTracker}. */
public enum NoOpJobMasterPartitionTracker implements JobMasterPartitionTracker {
    INSTANCE;

    public static final PartitionTrackerFactory FACTORY = lookup -> INSTANCE;

    @Override
    public void startTrackingPartition(
            ResourceID producingTaskExecutorId,
            ResultPartitionDeploymentDescriptor resultPartitionDeploymentDescriptor) {}

    @Override
    public Collection<PartitionTrackerEntry<ResourceID, ResultPartitionDeploymentDescriptor>>
            stopTrackingPartitionsFor(ResourceID key) {
        return Collections.emptyList();
    }

    @Override
    public void stopTrackingAndReleasePartitions(
            Collection<ResultPartitionID> resultPartitionIds, boolean releaseOnShuffleMaster) {}

    @Override
    public void stopTrackingAndReleaseOrPromotePartitions(
            Collection<ResultPartitionID> resultPartitionIds) {}

    @Override
    public Collection<ResultPartitionDeploymentDescriptor> getAllTrackedPartitions() {
        return Collections.emptyList();
    }

    @Override
    public Collection<PartitionTrackerEntry<ResourceID, ResultPartitionDeploymentDescriptor>>
            stopTrackingPartitions(Collection<ResultPartitionID> resultPartitionIds) {
        return Collections.emptyList();
    }

    @Override
    public boolean isTrackingPartitionsFor(ResourceID producingTaskExecutorId) {
        return false;
    }

    @Override
    public boolean isPartitionTracked(final ResultPartitionID resultPartitionID) {
        return false;
    }
}
