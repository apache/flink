/*
* Ported from flink-run time tests
* used as a dependency of execution graph generator
 */

package org.apache.flink.streaming.api.environment;

import org.apache.flink.runtime.clusterframework.types.ResourceID;
import org.apache.flink.runtime.deployment.ResultPartitionDeploymentDescriptor;
import org.apache.flink.runtime.io.network.partition.JobMasterPartitionTracker;
import org.apache.flink.runtime.io.network.partition.PartitionTrackerEntry;
import org.apache.flink.runtime.io.network.partition.PartitionTrackerFactory;
import org.apache.flink.runtime.io.network.partition.ResultPartitionID;
import org.apache.flink.runtime.jobgraph.IntermediateDataSetID;
import org.apache.flink.runtime.resourcemanager.ResourceManagerGateway;
import org.apache.flink.runtime.shuffle.ShuffleDescriptor;

import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.CompletableFuture;

/** No-op implementation of {@link JobMasterPartitionTracker}. */
public class NoOpJobMasterPartitionTracker implements JobMasterPartitionTracker {
    public static final NoOpJobMasterPartitionTracker INSTANCE =
            new NoOpJobMasterPartitionTracker();

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
    public CompletableFuture<Void> stopTrackingAndPromotePartitions(
            Collection<ResultPartitionID> resultPartitionIds) {
        return null;
    }

    @Override
    public Collection<ResultPartitionDeploymentDescriptor> getAllTrackedPartitions() {
        return Collections.emptyList();
    }

    @Override
    public void connectToResourceManager(ResourceManagerGateway resourceManagerGateway) {}

    @Override
    public List<ShuffleDescriptor> getClusterPartitionShuffleDescriptors(
            IntermediateDataSetID intermediateDataSetID) {
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
