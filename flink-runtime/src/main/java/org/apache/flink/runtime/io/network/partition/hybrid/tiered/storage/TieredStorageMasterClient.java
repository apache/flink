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

package org.apache.flink.runtime.io.network.partition.hybrid.tiered.storage;

import org.apache.flink.api.common.JobID;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.runtime.executiongraph.ResultPartitionBytes;
import org.apache.flink.runtime.io.network.partition.ResultPartitionID;
import org.apache.flink.runtime.io.network.partition.hybrid.tiered.shuffle.AllTieredShuffleMasterSnapshots;
import org.apache.flink.runtime.io.network.partition.hybrid.tiered.shuffle.ShuffleDescriptorRetriever;
import org.apache.flink.runtime.io.network.partition.hybrid.tiered.shuffle.TieredInternalShuffleMasterSnapshot;
import org.apache.flink.runtime.io.network.partition.hybrid.tiered.shuffle.TieredShuffleMasterSnapshot;
import org.apache.flink.runtime.io.network.partition.hybrid.tiered.tier.TierMasterAgent;
import org.apache.flink.runtime.io.network.partition.hybrid.tiered.tier.TierShuffleDescriptor;
import org.apache.flink.runtime.io.network.partition.hybrid.tiered.tier.TierShuffleHandler;
import org.apache.flink.runtime.shuffle.DefaultPartitionWithMetrics;
import org.apache.flink.runtime.shuffle.DefaultShuffleMetrics;
import org.apache.flink.runtime.shuffle.JobShuffleContext;
import org.apache.flink.runtime.shuffle.NettyShuffleDescriptor;
import org.apache.flink.runtime.shuffle.PartitionWithMetrics;
import org.apache.flink.runtime.shuffle.ShuffleDescriptor;
import org.apache.flink.runtime.shuffle.ShuffleMasterSnapshotContext;
import org.apache.flink.runtime.shuffle.ShuffleMetrics;
import org.apache.flink.util.concurrent.FutureUtils;

import java.time.Duration;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.function.BiConsumer;
import java.util.stream.Collectors;

import static org.apache.flink.util.Preconditions.checkState;

/** Client of the Tiered Storage used by the master. */
public class TieredStorageMasterClient {
    private final List<Tuple2<String, TierMasterAgent>> tiers;

    private final Map<String, TierMasterAgent> tierMasterAgentMap;

    private final boolean allPartitionInRemote;

    private final ShuffleDescriptorRetriever shuffleDescriptorRetriever;

    public TieredStorageMasterClient(
            List<Tuple2<String, TierMasterAgent>> tiers,
            ShuffleDescriptorRetriever shuffleDescriptorRetriever) {
        this.tiers = tiers;
        this.allPartitionInRemote = tiers.stream().allMatch(tier -> tier.f1.partitionInRemote());
        this.tierMasterAgentMap = new HashMap<>();
        for (Tuple2<String, TierMasterAgent> tier : tiers) {
            tierMasterAgentMap.put(tier.f0, tier.f1);
        }
        this.shuffleDescriptorRetriever = shuffleDescriptorRetriever;
    }

    public void registerJob(JobID jobID, TierShuffleHandler shuffleHandler) {
        tiers.forEach(tierMasterAgent -> tierMasterAgent.f1.registerJob(jobID, shuffleHandler));
    }

    public void unregisterJob(JobID jobID) {
        tiers.forEach(tierMasterAgent -> tierMasterAgent.f1.unregisterJob(jobID));
    }

    public List<TierShuffleDescriptor> addPartitionAndGetShuffleDescriptor(
            JobID jobID, int numSubpartitions, ResultPartitionID resultPartitionID) {
        return tiers.stream()
                .map(
                        tierMasterAgent ->
                                tierMasterAgent.f1.addPartitionAndGetShuffleDescriptor(
                                        jobID, numSubpartitions, resultPartitionID))
                .collect(Collectors.toList());
    }

    public void releasePartition(ShuffleDescriptor shuffleDescriptor) {
        checkState(shuffleDescriptor instanceof NettyShuffleDescriptor);
        List<TierShuffleDescriptor> tierShuffleDescriptors =
                ((NettyShuffleDescriptor) shuffleDescriptor).getTierShuffleDescriptors();
        if (tierShuffleDescriptors != null && !tierShuffleDescriptors.isEmpty()) {
            checkState(tierShuffleDescriptors.size() == tiers.size());
            for (int i = 0; i < tierShuffleDescriptors.size(); i++) {
                tiers.get(i).f1.releasePartition(tierShuffleDescriptors.get(i));
            }
        }
    }

    public void snapshotState(
            CompletableFuture<AllTieredShuffleMasterSnapshots> snapshotFuture,
            ShuffleMasterSnapshotContext context,
            JobID jobId) {
        snapshotStateInternal(
                snapshotFuture, (agent, future) -> agent.snapshotState(future, context, jobId));
    }

    public void snapshotState(CompletableFuture<AllTieredShuffleMasterSnapshots> snapshotFuture) {
        snapshotStateInternal(snapshotFuture, TierMasterAgent::snapshotState);
    }

    private void snapshotStateInternal(
            CompletableFuture<AllTieredShuffleMasterSnapshots> snapshotFuture,
            BiConsumer<TierMasterAgent, CompletableFuture<TieredShuffleMasterSnapshot>>
                    masterAgentConsumer) {
        List<CompletableFuture<Tuple2<String, TieredShuffleMasterSnapshot>>> futures =
                new ArrayList<>(tiers.size());
        for (Tuple2<String, TierMasterAgent> tier : tiers) {
            CompletableFuture<TieredShuffleMasterSnapshot> future = new CompletableFuture<>();
            futures.add(future.thenApply(snap -> Tuple2.of(tier.f0, snap)));
            masterAgentConsumer.accept(tier.f1, future);
        }

        FutureUtils.combineAll(futures)
                .thenAccept(
                        snapshotWithIdentifiers ->
                                snapshotFuture.complete(
                                        new AllTieredShuffleMasterSnapshots(
                                                snapshotWithIdentifiers)));
    }

    public void restoreState(TieredInternalShuffleMasterSnapshot clusterSnapshot) {
        checkState(clusterSnapshot != null);
        AllTieredShuffleMasterSnapshots allTierSnapshots = clusterSnapshot.getAllTierSnapshots();
        Collection<Tuple2<String, TieredShuffleMasterSnapshot>> snapshots =
                allTierSnapshots.getSnapshots();
        for (Tuple2<String, TieredShuffleMasterSnapshot> identifierWithSnap : snapshots) {
            String identifier = identifierWithSnap.f0;
            tierMasterAgentMap.get(identifier).restoreState(identifierWithSnap.f1);
        }
    }

    public void restoreState(List<TieredInternalShuffleMasterSnapshot> snapshots, JobID jobId) {
        for (TieredInternalShuffleMasterSnapshot internalSnapshot : snapshots) {
            checkState(internalSnapshot != null);
            AllTieredShuffleMasterSnapshots allTierSnapshots =
                    internalSnapshot.getAllTierSnapshots();
            Collection<Tuple2<String, TieredShuffleMasterSnapshot>> tierSnapshots =
                    allTierSnapshots.getSnapshots();
            for (Tuple2<String, TieredShuffleMasterSnapshot> identifierWithSnap : tierSnapshots) {
                String identifier = identifierWithSnap.f0;
                tierMasterAgentMap.get(identifier).restoreState(identifierWithSnap.f1, jobId);
            }
        }
    }

    public CompletableFuture<Collection<PartitionWithMetrics>> getPartitionWithMetrics(
            JobShuffleContext jobShuffleContext,
            Duration timeout,
            Set<ResultPartitionID> expectedPartitions) {
        JobID jobId = jobShuffleContext.getJobId();
        if (!allPartitionInRemote) {
            return jobShuffleContext.getPartitionWithMetrics(timeout, expectedPartitions);
        }

        List<CompletableFuture<Map<ResultPartitionID, ShuffleMetrics>>> futures =
                new ArrayList<>(tiers.size());
        for (Tuple2<String, TierMasterAgent> tier : tiers) {
            CompletableFuture<Map<ResultPartitionID, ShuffleMetrics>> tierPartitionMapFuture =
                    tier.f1.getPartitionWithMetrics(jobId, timeout, expectedPartitions);
            futures.add(tierPartitionMapFuture);
        }
        return FutureUtils.combineAll(futures)
                .thenApply(
                        allPartitions -> {
                            int tierNums = allPartitions.size();
                            List<PartitionWithMetrics> result = new ArrayList<>();
                            expectedPartitions.forEach(
                                    partitionId -> {
                                        List<ResultPartitionBytes> partitionBytes =
                                                new ArrayList<>();
                                        for (Map<ResultPartitionID, ShuffleMetrics> partitionMap :
                                                allPartitions) {
                                            ShuffleMetrics shuffleMetrics =
                                                    partitionMap.get(partitionId);
                                            if (shuffleMetrics == null) {
                                                break;
                                            }
                                            partitionBytes.add(shuffleMetrics.getPartitionBytes());
                                        }
                                        if (partitionBytes.size() == tierNums) {
                                            Optional<ShuffleDescriptor> shuffleDescriptor =
                                                    shuffleDescriptorRetriever.getShuffleDescriptor(
                                                            jobId, partitionId);
                                            shuffleDescriptor.ifPresent(
                                                    descriptor ->
                                                            result.add(
                                                                    new DefaultPartitionWithMetrics(
                                                                            descriptor,
                                                                            new DefaultShuffleMetrics(
                                                                                    ResultPartitionBytes
                                                                                            .mergeAll(
                                                                                                    partitionBytes)))));
                                        }
                                    });
                            return result;
                        });
    }

    public void close() {
        tiers.forEach(tier -> tier.f1.close());
    }
}
