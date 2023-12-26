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
import org.apache.flink.runtime.jobgraph.JobEdge;
import org.apache.flink.runtime.jobgraph.JobVertexID;
import org.apache.flink.runtime.scheduler.strategy.ConsumedPartitionGroup;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.apache.flink.util.Preconditions.checkArgument;
import static org.apache.flink.util.Preconditions.checkNotNull;
import static org.apache.flink.util.Preconditions.checkState;

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

    private int partitionsAssigned;

    private final int connectionIndex;

    private final ResultPartitionType resultType;

    private final Map<ConsumedPartitionGroup, CachedShuffleDescriptors> shuffleDescriptorCache;

    /** All consumer job vertex ids of this dataset. */
    private final List<JobVertexID> consumerVertices = new ArrayList<>();

    public IntermediateResult(
            IntermediateDataSet intermediateDataSet,
            ExecutionJobVertex producer,
            int numParallelProducers,
            ResultPartitionType resultType) {

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

        intermediateDataSet
                .getConsumers()
                .forEach(jobEdge -> consumerVertices.add(jobEdge.getTarget().getID()));
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
        return consumerVertices;
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
        List<JobEdge> consumers = intermediateDataSet.getConsumers();
        checkState(!consumers.isEmpty());

        InternalExecutionGraphAccessor graph = getProducer().getGraph();
        int consumersParallelism =
                graph.getJobVertex(consumers.get(0).getTarget().getID()).getParallelism();
        if (consumers.size() == 1) {
            return consumersParallelism;
        }

        // sanity check, all consumer vertices must have the same parallelism:
        // 1. for vertices that are not assigned a parallelism initially (for example, dynamic
        // graph), the parallelisms will all be -1 (parallelism not decided yet)
        // 2. for vertices that are initially assigned a parallelism, the parallelisms must be the
        // same, which is guaranteed at compilation phase
        for (JobVertexID jobVertexID : consumerVertices) {
            checkState(
                    consumersParallelism == graph.getJobVertex(jobVertexID).getParallelism(),
                    "Consumers must have the same parallelism.");
        }
        return consumersParallelism;
    }

    int getConsumersMaxParallelism() {
        List<JobEdge> consumers = intermediateDataSet.getConsumers();
        checkState(!consumers.isEmpty());

        InternalExecutionGraphAccessor graph = getProducer().getGraph();
        int consumersMaxParallelism =
                graph.getJobVertex(consumers.get(0).getTarget().getID()).getMaxParallelism();
        if (consumers.size() == 1) {
            return consumersMaxParallelism;
        }

        // sanity check, all consumer vertices must have the same max parallelism
        for (JobVertexID jobVertexID : consumerVertices) {
            checkState(
                    consumersMaxParallelism == graph.getJobVertex(jobVertexID).getMaxParallelism(),
                    "Consumers must have the same max parallelism.");
        }
        return consumersMaxParallelism;
    }

    public DistributionPattern getConsumingDistributionPattern() {
        return intermediateDataSet.getDistributionPattern();
    }

    public boolean isBroadcast() {
        return intermediateDataSet.isBroadcast();
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
