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

package org.apache.flink.runtime.executiongraph;

import org.apache.flink.annotation.VisibleForTesting;
import org.apache.flink.runtime.blob.PermanentBlobKey;
import org.apache.flink.runtime.deployment.CachedShuffleDescriptors;
import org.apache.flink.runtime.deployment.TaskDeploymentDescriptor.Offloaded;
import org.apache.flink.runtime.deployment.TaskDeploymentDescriptorFactory.ShuffleDescriptorAndIndex;
import org.apache.flink.runtime.deployment.TaskDeploymentDescriptorFactory.ShuffleDescriptorGroup;
import org.apache.flink.runtime.io.network.partition.ResultPartitionType;
import org.apache.flink.runtime.jobgraph.DistributionPattern;
import org.apache.flink.runtime.jobgraph.IntermediateDataSet;
import org.apache.flink.runtime.jobgraph.IntermediateDataSetID;
import org.apache.flink.runtime.jobgraph.IntermediateResultPartitionID;
import org.apache.flink.runtime.jobgraph.JobVertexID;
import org.apache.flink.runtime.scheduler.adaptivebatch.ExecutionPlanSchedulingContext;
import org.apache.flink.runtime.scheduler.strategy.ConsumedPartitionGroup;

import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

import static org.apache.flink.util.Preconditions.checkArgument;
import static org.apache.flink.util.Preconditions.checkNotNull;

public class IntermediateResult {

    private final IntermediateDataSet intermediateDataSet;

    private final IntermediateDataSetID id;

    private final ExecutionJobVertex producer;

    private final IntermediateResultPartition[] partitions;

    /**
     * Maps intermediate result partition IDs to a partition index. This is used for ID lookups of
     * intermediate results. I didn't dare to change the partition connect logic in other places
     * that is tightly coupled to the partitions being held as an array.
     */
    private final HashMap<IntermediateResultPartitionID, Integer> partitionLookupHelper =
            new HashMap<>();

    private final int numParallelProducers;

    private final ExecutionPlanSchedulingContext executionPlanSchedulingContext;
    private final boolean singleSubpartitionContainsAllData;

    private int partitionsAssigned;

    private final int connectionIndex;

    private final ResultPartitionType resultType;

    private final Map<ConsumedPartitionGroup, CachedShuffleDescriptors> shuffleDescriptorCache;

    public IntermediateResult(
            IntermediateDataSet intermediateDataSet,
            ExecutionJobVertex producer,
            int numParallelProducers,
            ResultPartitionType resultType,
            ExecutionPlanSchedulingContext executionPlanSchedulingContext) {

        this.intermediateDataSet = checkNotNull(intermediateDataSet);
        this.id = checkNotNull(intermediateDataSet.getId());

        this.producer = checkNotNull(producer);

        checkArgument(numParallelProducers >= 1);
        this.numParallelProducers = numParallelProducers;

        this.partitions = new IntermediateResultPartition[numParallelProducers];

        // we do not set the intermediate result partitions here, because we let them be initialized
        // by
        // the execution vertex that produces them

        // assign a random connection index
        this.connectionIndex = (int) (Math.random() * Integer.MAX_VALUE);

        // The runtime type for this produced result
        this.resultType = checkNotNull(resultType);

        this.shuffleDescriptorCache = new HashMap<>();

        this.executionPlanSchedulingContext = checkNotNull(executionPlanSchedulingContext);

        this.singleSubpartitionContainsAllData = intermediateDataSet.isBroadcast();
    }

    public boolean areAllConsumerVerticesCreated() {
        return intermediateDataSet.areAllConsumerVerticesCreated();
    }

    public void setPartition(int partitionNumber, IntermediateResultPartition partition) {
        if (partition == null || partitionNumber < 0 || partitionNumber >= numParallelProducers) {
            throw new IllegalArgumentException();
        }

        if (partitions[partitionNumber] != null) {
            throw new IllegalStateException(
                    "Partition #" + partitionNumber + " has already been assigned.");
        }

        partitions[partitionNumber] = partition;
        partitionLookupHelper.put(partition.getPartitionId(), partitionNumber);
        partitionsAssigned++;
    }

    public IntermediateDataSetID getId() {
        return id;
    }

    public ExecutionJobVertex getProducer() {
        return producer;
    }

    public IntermediateResultPartition[] getPartitions() {
        return partitions;
    }

    public List<JobVertexID> getConsumerVertices() {
        return intermediateDataSet.getConsumers().stream()
                .map(jobEdge -> jobEdge.getTarget().getID())
                .collect(Collectors.toList());
    }

    /**
     * Returns the partition with the given ID.
     *
     * @param resultPartitionId ID of the partition to look up
     * @throws NullPointerException If partition ID <code>null</code>
     * @throws IllegalArgumentException Thrown if unknown partition ID
     * @return Intermediate result partition with the given ID
     */
    public IntermediateResultPartition getPartitionById(
            IntermediateResultPartitionID resultPartitionId) {
        // Looks ups the partition number via the helper map and returns the
        // partition. Currently, this happens infrequently enough that we could
        // consider removing the map and scanning the partitions on every lookup.
        // The lookup (currently) only happen when the producer of an intermediate
        // result cannot be found via its registered execution.
        Integer partitionNumber =
                partitionLookupHelper.get(
                        checkNotNull(resultPartitionId, "IntermediateResultPartitionID"));
        if (partitionNumber != null) {
            return partitions[partitionNumber];
        } else {
            throw new IllegalArgumentException(
                    "Unknown intermediate result partition ID " + resultPartitionId);
        }
    }

    public int getNumberOfAssignedPartitions() {
        return partitionsAssigned;
    }

    public ResultPartitionType getResultType() {
        return resultType;
    }

    int getNumParallelProducers() {
        return numParallelProducers;
    }

    /**
     * Currently, this method is only used to compute the maximum number of consumers. For dynamic
     * graph, it should be called before adaptively deciding the downstream consumer parallelism.
     */
    int getConsumersParallelism() {
        return executionPlanSchedulingContext.getConsumersParallelism(
                jobVertexId -> producer.getGraph().getJobVertex(jobVertexId).getParallelism(),
                intermediateDataSet);
    }

    int getConsumersMaxParallelism() {
        return executionPlanSchedulingContext.getConsumersMaxParallelism(
                jobVertexId -> producer.getGraph().getJobVertex(jobVertexId).getMaxParallelism(),
                intermediateDataSet);
    }

    public DistributionPattern getConsumingDistributionPattern() {
        return intermediateDataSet.getDistributionPattern();
    }

    /**
     * Determines whether the associated intermediate data set uses a broadcast distribution
     * pattern.
     *
     * <p>A broadcast distribution pattern indicates that all data produced by this intermediate
     * data set should be broadcast to every downstream consumer.
     *
     * @return true if the intermediate data set is using a broadcast distribution pattern; false
     *     otherwise.
     */
    public boolean isBroadcast() {
        return intermediateDataSet.isBroadcast();
    }

    public boolean isForward() {
        return intermediateDataSet.isForward();
    }

    /**
     * Checks if a single subpartition contains all the produced data. This condition indicate that
     * the data was intended to be broadcast to all consumers. If the decision to broadcast was made
     * before the data production, this flag would likely be set accordingly. Conversely, if the
     * broadcasting decision was made post-production, this flag will be false.
     *
     * @return true if a single subpartition contains all the data; false otherwise.
     */
    public boolean isSingleSubpartitionContainsAllData() {
        return singleSubpartitionContainsAllData;
    }

    public int getConnectionIndex() {
        return connectionIndex;
    }

    @VisibleForTesting
    void resetForNewExecution() {
        for (IntermediateResultPartition partition : partitions) {
            partition.resetForNewExecution();
        }
    }

    public CachedShuffleDescriptors getCachedShuffleDescriptors(
            ConsumedPartitionGroup consumedPartitionGroup) {
        return shuffleDescriptorCache.get(consumedPartitionGroup);
    }

    public CachedShuffleDescriptors cacheShuffleDescriptors(
            ConsumedPartitionGroup consumedPartitionGroup,
            ShuffleDescriptorAndIndex[] shuffleDescriptors) {
        CachedShuffleDescriptors cachedShuffleDescriptors =
                new CachedShuffleDescriptors(consumedPartitionGroup, shuffleDescriptors);
        shuffleDescriptorCache.put(consumedPartitionGroup, cachedShuffleDescriptors);
        return cachedShuffleDescriptors;
    }

    public void markPartitionFinished(
            ConsumedPartitionGroup consumedPartitionGroup,
            IntermediateResultPartition resultPartition) {
        // only hybrid result partition need this notification.
        if (!resultPartition.getResultType().isHybridResultPartition()) {
            return;
        }
        // if this consumedPartitionGroup is not cached, ignore partition finished notification.
        // In this case, shuffle descriptor will be computed directly by
        // TaskDeploymentDescriptorFactory#getConsumedPartitionShuffleDescriptors.
        if (shuffleDescriptorCache.containsKey(consumedPartitionGroup)) {
            CachedShuffleDescriptors cachedShuffleDescriptors =
                    shuffleDescriptorCache.get(consumedPartitionGroup);
            cachedShuffleDescriptors.markPartitionFinished(resultPartition);
        }
    }

    public void clearCachedInformationForPartitionGroup(
            ConsumedPartitionGroup consumedPartitionGroup) {
        // When a ConsumedPartitionGroup changes, the cache of ShuffleDescriptors for this
        // partition group is no longer valid and needs to be removed.
        //
        // Currently, there are two scenarios:
        // 1. The ConsumedPartitionGroup is released
        // 2. Its producer encounters a failover

        // Remove the cache for the ConsumedPartitionGroup and notify the BLOB writer to delete the
        // cache if it is offloaded
        final CachedShuffleDescriptors cache =
                this.shuffleDescriptorCache.remove(consumedPartitionGroup);
        if (cache != null) {
            cache.getAllSerializedShuffleDescriptorGroups()
                    .forEach(
                            shuffleDescriptors -> {
                                if (shuffleDescriptors instanceof Offloaded) {
                                    PermanentBlobKey blobKey =
                                            ((Offloaded<ShuffleDescriptorGroup>) shuffleDescriptors)
                                                    .serializedValueKey;
                                    this.producer
                                            .getGraph()
                                            .deleteBlobs(Collections.singletonList(blobKey));
                                }
                            });
        }
    }

    @Override
    public String toString() {
        return "IntermediateResult " + id.toString();
    }
}
