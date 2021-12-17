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

import org.apache.flink.api.common.JobID;
import org.apache.flink.runtime.clusterframework.types.ResourceID;
import org.apache.flink.runtime.deployment.ResultPartitionDeploymentDescriptor;
import org.apache.flink.runtime.shuffle.ShuffleMaster;
import org.apache.flink.util.Preconditions;

import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.function.BiConsumer;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import static java.util.stream.Collectors.toList;

/**
 * Utility for tracking partitions and issuing release calls to task executors and shuffle masters.
 */
public class JobMasterPartitionTrackerImpl
        extends AbstractPartitionTracker<ResourceID, ResultPartitionDeploymentDescriptor>
        implements JobMasterPartitionTracker {

    // Besides below fields, JobMasterPartitionTrackerImpl inherits 'partitionTable' and
    // 'partitionInfos' from parent and tracks partitions from different dimensions:
    // 'partitionTable' tracks partitions which occupie local resource on TM;
    // 'partitionInfos' tracks all available partitions no matter they are accommodated
    // externally on remote or internally on TM;

    private final JobID jobId;

    private final ShuffleMaster<?> shuffleMaster;

    private final PartitionTrackerFactory.TaskExecutorGatewayLookup taskExecutorGatewayLookup;

    public JobMasterPartitionTrackerImpl(
            JobID jobId,
            ShuffleMaster<?> shuffleMaster,
            PartitionTrackerFactory.TaskExecutorGatewayLookup taskExecutorGatewayLookup) {

        this.jobId = Preconditions.checkNotNull(jobId);
        this.shuffleMaster = Preconditions.checkNotNull(shuffleMaster);
        this.taskExecutorGatewayLookup = taskExecutorGatewayLookup;
    }

    @Override
    public void startTrackingPartition(
            ResourceID producingTaskExecutorId,
            ResultPartitionDeploymentDescriptor resultPartitionDeploymentDescriptor) {
        Preconditions.checkNotNull(producingTaskExecutorId);
        Preconditions.checkNotNull(resultPartitionDeploymentDescriptor);

        // blocking and PIPELINED_APPROXIMATE partitions require explicit partition release calls
        // reconnectable will be removed after FLINK-19895, see also {@link
        // ResultPartitionType#isReconnectable}.
        if (!resultPartitionDeploymentDescriptor.getPartitionType().isReconnectable()) {
            return;
        }

        final ResultPartitionID resultPartitionId =
                resultPartitionDeploymentDescriptor.getShuffleDescriptor().getResultPartitionID();

        startTrackingPartition(
                producingTaskExecutorId, resultPartitionId, resultPartitionDeploymentDescriptor);
    }

    @Override
    void startTrackingPartition(
            ResourceID key,
            ResultPartitionID resultPartitionId,
            ResultPartitionDeploymentDescriptor metaInfo) {
        // A partition is registered into 'partitionTable' only when it occupies
        // resource on the corresponding TM;
        if (metaInfo.getShuffleDescriptor().storesLocalResourcesOn().isPresent()) {
            partitionTable.startTrackingPartitions(
                    key, Collections.singletonList(resultPartitionId));
        }
        partitionInfos.put(resultPartitionId, new PartitionInfo<>(key, metaInfo));
    }

    @Override
    public void stopTrackingAndReleasePartitions(
            Collection<ResultPartitionID> resultPartitionIds, boolean releaseOnShuffleMaster) {
        stopTrackingAndHandlePartitions(
                resultPartitionIds,
                (tmID, partitionDescs) ->
                        internalReleasePartitions(tmID, partitionDescs, releaseOnShuffleMaster));
    }

    @Override
    public void stopTrackingAndReleaseOrPromotePartitions(
            Collection<ResultPartitionID> resultPartitionIds) {
        stopTrackingAndHandlePartitions(
                resultPartitionIds,
                (tmID, partitionDescs) -> internalReleaseOrPromotePartitions(tmID, partitionDescs));
    }

    @Override
    public Collection<ResultPartitionDeploymentDescriptor> getAllTrackedPartitions() {
        return partitionInfos.values().stream().map(PartitionInfo::getMetaInfo).collect(toList());
    }

    private void stopTrackingAndHandlePartitions(
            Collection<ResultPartitionID> resultPartitionIds,
            BiConsumer<ResourceID, Collection<ResultPartitionDeploymentDescriptor>>
                    partitionHandler) {
        Preconditions.checkNotNull(resultPartitionIds);

        // stop tracking partitions to handle and group them by task executor ID
        Map<ResourceID, List<ResultPartitionDeploymentDescriptor>> partitionsToReleaseByResourceId =
                stopTrackingPartitions(resultPartitionIds).stream()
                        .collect(
                                Collectors.groupingBy(
                                        PartitionTrackerEntry::getKey,
                                        Collectors.mapping(
                                                PartitionTrackerEntry::getMetaInfo, toList())));

        partitionsToReleaseByResourceId.forEach(partitionHandler);
    }

    private void internalReleasePartitions(
            ResourceID potentialPartitionLocation,
            Collection<ResultPartitionDeploymentDescriptor> partitionDeploymentDescriptors,
            boolean releaseOnShuffleMaster) {

        internalReleasePartitionsOnTaskExecutor(
                potentialPartitionLocation, partitionDeploymentDescriptors);
        if (releaseOnShuffleMaster) {
            internalReleasePartitionsOnShuffleMaster(partitionDeploymentDescriptors.stream());
        }
    }

    private void internalReleaseOrPromotePartitions(
            ResourceID potentialPartitionLocation,
            Collection<ResultPartitionDeploymentDescriptor> partitionDeploymentDescriptors) {

        internalReleaseOrPromotePartitionsOnTaskExecutor(
                potentialPartitionLocation, partitionDeploymentDescriptors);
        internalReleasePartitionsOnShuffleMaster(
                excludePersistentPartitions(partitionDeploymentDescriptors));
    }

    private void internalReleasePartitionsOnTaskExecutor(
            ResourceID potentialPartitionLocation,
            Collection<ResultPartitionDeploymentDescriptor> partitionDeploymentDescriptors) {

        final Set<ResultPartitionID> partitionsRequiringRpcReleaseCalls =
                partitionDeploymentDescriptors.stream()
                        .filter(JobMasterPartitionTrackerImpl::isPartitionWithLocalResources)
                        .map(JobMasterPartitionTrackerImpl::getResultPartitionId)
                        .collect(Collectors.toSet());

        internalReleaseOrPromotePartitionsOnTaskExecutor(
                potentialPartitionLocation,
                partitionsRequiringRpcReleaseCalls,
                Collections.emptySet());
    }

    private void internalReleaseOrPromotePartitionsOnTaskExecutor(
            ResourceID potentialPartitionLocation,
            Collection<ResultPartitionDeploymentDescriptor> partitionDeploymentDescriptors) {

        Map<Boolean, Set<ResultPartitionID>> partitionsToReleaseByPersistence =
                partitionDeploymentDescriptors.stream()
                        .filter(JobMasterPartitionTrackerImpl::isPartitionWithLocalResources)
                        .collect(
                                Collectors.partitioningBy(
                                        resultPartitionDeploymentDescriptor ->
                                                resultPartitionDeploymentDescriptor
                                                        .getPartitionType()
                                                        .isPersistent(),
                                        Collectors.mapping(
                                                JobMasterPartitionTrackerImpl::getResultPartitionId,
                                                Collectors.toSet())));

        internalReleaseOrPromotePartitionsOnTaskExecutor(
                potentialPartitionLocation,
                partitionsToReleaseByPersistence.get(false),
                partitionsToReleaseByPersistence.get(true));
    }

    private void internalReleaseOrPromotePartitionsOnTaskExecutor(
            ResourceID potentialPartitionLocation,
            Set<ResultPartitionID> partitionsRequiringRpcReleaseCalls,
            Set<ResultPartitionID> partitionsRequiringRpcPromoteCalls) {

        if (!partitionsRequiringRpcReleaseCalls.isEmpty()
                || !partitionsRequiringRpcPromoteCalls.isEmpty()) {
            taskExecutorGatewayLookup
                    .lookup(potentialPartitionLocation)
                    .ifPresent(
                            taskExecutorGateway ->
                                    taskExecutorGateway.releaseOrPromotePartitions(
                                            jobId,
                                            partitionsRequiringRpcReleaseCalls,
                                            partitionsRequiringRpcPromoteCalls));
        }
    }

    private void internalReleasePartitionsOnShuffleMaster(
            Stream<ResultPartitionDeploymentDescriptor> partitionDeploymentDescriptors) {
        partitionDeploymentDescriptors
                .map(ResultPartitionDeploymentDescriptor::getShuffleDescriptor)
                .forEach(shuffleMaster::releasePartitionExternally);
    }

    private static boolean isPartitionWithLocalResources(
            ResultPartitionDeploymentDescriptor resultPartitionDeploymentDescriptor) {
        return resultPartitionDeploymentDescriptor
                .getShuffleDescriptor()
                .storesLocalResourcesOn()
                .isPresent();
    }

    private static Stream<ResultPartitionDeploymentDescriptor> excludePersistentPartitions(
            Collection<ResultPartitionDeploymentDescriptor> partitionDeploymentDescriptors) {
        return partitionDeploymentDescriptors.stream()
                .filter(
                        resultPartitionDeploymentDescriptor ->
                                !resultPartitionDeploymentDescriptor
                                        .getPartitionType()
                                        .isPersistent());
    }

    private static ResultPartitionID getResultPartitionId(
            ResultPartitionDeploymentDescriptor resultPartitionDeploymentDescriptor) {
        return resultPartitionDeploymentDescriptor.getShuffleDescriptor().getResultPartitionID();
    }
}
