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

package org.apache.flink.runtime.io.network.partition.hybrid.tiered.tier;

import org.apache.flink.api.common.JobID;
import org.apache.flink.runtime.io.network.partition.ResultPartitionID;
import org.apache.flink.runtime.io.network.partition.hybrid.tiered.shuffle.EmptyTieredShuffleMasterSnapshot;
import org.apache.flink.runtime.io.network.partition.hybrid.tiered.shuffle.TieredShuffleMasterSnapshot;
import org.apache.flink.runtime.shuffle.ShuffleMasterSnapshotContext;
import org.apache.flink.runtime.shuffle.ShuffleMetrics;

import java.time.Duration;
import java.util.Collections;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.CompletableFuture;

/** The master-side agent of a Tier. */
public interface TierMasterAgent {

    /** Register a job id with a {@link TierShuffleHandler}. */
    void registerJob(JobID jobID, TierShuffleHandler tierShuffleHandler);

    /** Unregister a job id. */
    void unregisterJob(JobID jobID);

    /** Add a new tiered storage partition and get the {@link TierShuffleDescriptor}. */
    TierShuffleDescriptor addPartitionAndGetShuffleDescriptor(
            JobID jobID, int numSubpartitions, ResultPartitionID resultPartitionID);

    /** Triggers a snapshot of the tier master agent's state which related the specified job. */
    default void snapshotState(
            CompletableFuture<TieredShuffleMasterSnapshot> snapshotFuture,
            ShuffleMasterSnapshotContext context,
            JobID jobId) {
        snapshotFuture.complete(EmptyTieredShuffleMasterSnapshot.getInstance());
    }

    /** Triggers a snapshot of the tier master agent's state. */
    default void snapshotState(CompletableFuture<TieredShuffleMasterSnapshot> snapshotFuture) {
        snapshotFuture.complete(EmptyTieredShuffleMasterSnapshot.getInstance());
    }

    /** Restores the state of the tier master agent from the provided snapshots. */
    default void restoreState(TieredShuffleMasterSnapshot snapshot, JobID jobId) {}

    /**
     * Restores the state of the tier master agent from the provided snapshots for the specified
     * job.
     */
    default void restoreState(TieredShuffleMasterSnapshot snapshot) {}

    /**
     * Retrieves specified partitions and their metrics (identified by {@code expectedPartitions}),
     * the metrics include sizes of sub-partitions in a result partition.
     *
     * @param jobId ID of the target job
     * @param timeout The timeout used for retrieve the specified partitions.
     * @param expectedPartitions The set of identifiers for the result partitions whose metrics are
     *     to be fetched.
     * @return A future will contain a map of the partitions with their metrics that could be
     *     retrieved from the expected partitions within the specified timeout period.
     */
    default CompletableFuture<Map<ResultPartitionID, ShuffleMetrics>> getPartitionWithMetrics(
            JobID jobId, Duration timeout, Set<ResultPartitionID> expectedPartitions) {
        if (!partitionInRemote()) {
            return CompletableFuture.completedFuture(Collections.emptyMap());
        } else {
            throw new UnsupportedOperationException(
                    "remote partition should be reported by tier itself.");
        }
    }

    /**
     * Release a tiered storage partition.
     *
     * @param shuffleDescriptor the partition shuffle descriptor to be released
     */
    void releasePartition(TierShuffleDescriptor shuffleDescriptor);

    /** Close this tier master agent. */
    void close();

    /** Is this tier manage the partition in remote cluster instead of flink taskmanager. */
    default boolean partitionInRemote() {
        return false;
    }
}
