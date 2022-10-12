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
import org.apache.flink.runtime.jobgraph.IntermediateDataSetID;
import org.apache.flink.runtime.resourcemanager.ResourceManagerGateway;
import org.apache.flink.runtime.shuffle.ShuffleDescriptor;

import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.function.BiConsumer;
import java.util.function.Consumer;
import java.util.function.Function;
import java.util.function.Supplier;

/** Test {@link JobMasterPartitionTracker} implementation. */
public class TestingJobMasterPartitionTracker implements JobMasterPartitionTracker {

    private Function<ResourceID, Boolean> isTrackingPartitionsForFunction = ignored -> false;
    private Function<ResultPartitionID, Boolean> isPartitionTrackedFunction = ignored -> false;
    private Consumer<ResourceID> stopTrackingAllPartitionsConsumer = ignored -> {};
    private BiConsumer<ResourceID, ResultPartitionDeploymentDescriptor>
            startTrackingPartitionsConsumer = (ignoredA, ignoredB) -> {};
    private Consumer<Collection<ResultPartitionID>> stopTrackingAndReleasePartitionsConsumer =
            ignored -> {};

    private Consumer<Collection<ResultPartitionID>> stopTrackingAndPromotePartitionsConsumer =
            ignored -> {};
    private Consumer<Collection<ResultPartitionID>> stopTrackingPartitionsConsumer = ignored -> {};
    private Supplier<Collection<ResultPartitionDeploymentDescriptor>>
            getAllTrackedPartitionsSupplier = () -> Collections.emptyList();

    public void setStartTrackingPartitionsConsumer(
            BiConsumer<ResourceID, ResultPartitionDeploymentDescriptor>
                    startTrackingPartitionsConsumer) {
        this.startTrackingPartitionsConsumer = startTrackingPartitionsConsumer;
    }

    public void setIsTrackingPartitionsForFunction(
            Function<ResourceID, Boolean> isTrackingPartitionsForFunction) {
        this.isTrackingPartitionsForFunction = isTrackingPartitionsForFunction;
    }

    public void setIsPartitionTrackedFunction(
            Function<ResultPartitionID, Boolean> isPartitionTrackedFunction) {
        this.isPartitionTrackedFunction = isPartitionTrackedFunction;
    }

    public void setStopTrackingAllPartitionsConsumer(
            Consumer<ResourceID> stopTrackingAllPartitionsConsumer) {
        this.stopTrackingAllPartitionsConsumer = stopTrackingAllPartitionsConsumer;
    }

    public void setStopTrackingAndReleasePartitionsConsumer(
            Consumer<Collection<ResultPartitionID>> stopTrackingAndReleasePartitionsConsumer) {
        this.stopTrackingAndReleasePartitionsConsumer = stopTrackingAndReleasePartitionsConsumer;
    }

    public void setStopTrackingPartitionsConsumer(
            Consumer<Collection<ResultPartitionID>> stopTrackingPartitionsConsumer) {
        this.stopTrackingPartitionsConsumer = stopTrackingPartitionsConsumer;
    }

    public void setGetAllTrackedPartitionsSupplier(
            Supplier<Collection<ResultPartitionDeploymentDescriptor>>
                    getAllTrackedPartitionsSupplier) {
        this.getAllTrackedPartitionsSupplier = getAllTrackedPartitionsSupplier;
    }

    public void setStopTrackingAndPromotePartitionsConsumer(
            Consumer<Collection<ResultPartitionID>> stopTrackingAndPromotePartitionsConsumer) {
        this.stopTrackingAndPromotePartitionsConsumer = stopTrackingAndPromotePartitionsConsumer;
    }

    @Override
    public void startTrackingPartition(
            ResourceID producingTaskExecutorId,
            ResultPartitionDeploymentDescriptor resultPartitionDeploymentDescriptor) {
        this.startTrackingPartitionsConsumer.accept(
                producingTaskExecutorId, resultPartitionDeploymentDescriptor);
    }

    @Override
    public Collection<PartitionTrackerEntry<ResourceID, ResultPartitionDeploymentDescriptor>>
            stopTrackingPartitionsFor(ResourceID producingTaskExecutorId) {
        stopTrackingAllPartitionsConsumer.accept(producingTaskExecutorId);
        return Collections.emptyList();
    }

    @Override
    public void stopTrackingAndReleasePartitions(
            Collection<ResultPartitionID> resultPartitionIds, boolean releaseOnShuffleMaster) {
        stopTrackingAndReleasePartitionsConsumer.accept(resultPartitionIds);
    }

    @Override
    public CompletableFuture<Void> stopTrackingAndPromotePartitions(
            Collection<ResultPartitionID> resultPartitionIds) {
        stopTrackingAndPromotePartitionsConsumer.accept(resultPartitionIds);
        return CompletableFuture.completedFuture(null);
    }

    @Override
    public Collection<PartitionTrackerEntry<ResourceID, ResultPartitionDeploymentDescriptor>>
            stopTrackingPartitions(Collection<ResultPartitionID> resultPartitionIds) {
        stopTrackingPartitionsConsumer.accept(resultPartitionIds);
        return Collections.emptyList();
    }

    @Override
    public Collection<ResultPartitionDeploymentDescriptor> getAllTrackedPartitions() {
        return getAllTrackedPartitionsSupplier.get();
    }

    @Override
    public void connectToResourceManager(ResourceManagerGateway resourceManagerGateway) {}

    @Override
    public List<ShuffleDescriptor> getClusterPartitionShuffleDescriptors(
            IntermediateDataSetID intermediateDataSetID) {
        return Collections.emptyList();
    }

    @Override
    public boolean isTrackingPartitionsFor(ResourceID producingTaskExecutorId) {
        return isTrackingPartitionsForFunction.apply(producingTaskExecutorId);
    }

    @Override
    public boolean isPartitionTracked(final ResultPartitionID resultPartitionID) {
        return isPartitionTrackedFunction.apply(resultPartitionID);
    }
}
