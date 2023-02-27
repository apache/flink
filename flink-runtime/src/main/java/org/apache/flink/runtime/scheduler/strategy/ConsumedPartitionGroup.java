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
 * limitations under the License
 */

package org.apache.flink.runtime.scheduler.strategy;

import org.apache.flink.runtime.io.network.partition.ResultPartitionType;
import org.apache.flink.runtime.jobgraph.IntermediateDataSetID;
import org.apache.flink.runtime.jobgraph.IntermediateResultPartitionID;
import org.apache.flink.util.Preconditions;

import javax.annotation.Nullable;

import java.util.Collections;
import java.util.Iterator;
import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;

import static org.apache.flink.util.Preconditions.checkArgument;
import static org.apache.flink.util.Preconditions.checkNotNull;
import static org.apache.flink.util.Preconditions.checkState;

/**
 * Group of consumed {@link IntermediateResultPartitionID}s. One such a group corresponds to one
 * {@link ConsumerVertexGroup}.
 */
public class ConsumedPartitionGroup implements Iterable<IntermediateResultPartitionID> {

    private final List<IntermediateResultPartitionID> resultPartitions;

    private final AtomicInteger unfinishedPartitions;

    private final IntermediateDataSetID intermediateDataSetID;

    private final ResultPartitionType resultPartitionType;

    /** Number of consumer tasks in the corresponding {@link ConsumerVertexGroup}. */
    private final int numConsumers;

    @Nullable private ConsumerVertexGroup consumerVertexGroup;

    private ConsumedPartitionGroup(
            int numConsumers,
            List<IntermediateResultPartitionID> resultPartitions,
            ResultPartitionType resultPartitionType) {
        checkArgument(
                resultPartitions.size() > 0,
                "The size of result partitions in the ConsumedPartitionGroup should be larger than 0.");
        this.numConsumers = numConsumers;
        this.intermediateDataSetID = resultPartitions.get(0).getIntermediateDataSetID();
        this.resultPartitionType = Preconditions.checkNotNull(resultPartitionType);

        // Sanity check: all the partitions in one ConsumedPartitionGroup should have the same
        // IntermediateDataSetID
        for (IntermediateResultPartitionID resultPartition : resultPartitions) {
            checkArgument(
                    resultPartition.getIntermediateDataSetID().equals(this.intermediateDataSetID));
        }
        this.resultPartitions = resultPartitions;

        this.unfinishedPartitions = new AtomicInteger(resultPartitions.size());
    }

    public static ConsumedPartitionGroup fromMultiplePartitions(
            int numConsumers,
            List<IntermediateResultPartitionID> resultPartitions,
            ResultPartitionType resultPartitionType) {
        return new ConsumedPartitionGroup(numConsumers, resultPartitions, resultPartitionType);
    }

    public static ConsumedPartitionGroup fromSinglePartition(
            int numConsumers,
            IntermediateResultPartitionID resultPartition,
            ResultPartitionType resultPartitionType) {
        return new ConsumedPartitionGroup(
                numConsumers, Collections.singletonList(resultPartition), resultPartitionType);
    }

    @Override
    public Iterator<IntermediateResultPartitionID> iterator() {
        return resultPartitions.iterator();
    }

    public int size() {
        return resultPartitions.size();
    }

    public boolean isEmpty() {
        return resultPartitions.isEmpty();
    }

    /**
     * In dynamic graph cases, the number of consumers of ConsumedPartitionGroup can be different
     * even if they contain the same IntermediateResultPartition.
     */
    public int getNumConsumers() {
        return numConsumers;
    }

    public IntermediateResultPartitionID getFirst() {
        return iterator().next();
    }

    /** Get the ID of IntermediateDataSet this ConsumedPartitionGroup belongs to. */
    public IntermediateDataSetID getIntermediateDataSetID() {
        return intermediateDataSetID;
    }

    public int partitionUnfinished() {
        return unfinishedPartitions.incrementAndGet();
    }

    public int partitionFinished() {
        return unfinishedPartitions.decrementAndGet();
    }

    public int getNumberOfUnfinishedPartitions() {
        return unfinishedPartitions.get();
    }

    public boolean areAllPartitionsFinished() {
        return unfinishedPartitions.get() == 0;
    }

    public ResultPartitionType getResultPartitionType() {
        return resultPartitionType;
    }

    public ConsumerVertexGroup getConsumerVertexGroup() {
        return checkNotNull(consumerVertexGroup, "ConsumerVertexGroup is not properly set.");
    }

    public void setConsumerVertexGroup(ConsumerVertexGroup consumerVertexGroup) {
        checkState(this.consumerVertexGroup == null);
        this.consumerVertexGroup = checkNotNull(consumerVertexGroup);
    }
}
