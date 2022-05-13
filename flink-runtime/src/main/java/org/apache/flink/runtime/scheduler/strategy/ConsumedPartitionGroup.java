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

import org.apache.flink.annotation.VisibleForTesting;
import org.apache.flink.runtime.jobgraph.IntermediateDataSetID;
import org.apache.flink.runtime.jobgraph.IntermediateResultPartitionID;

import java.util.Collections;
import java.util.Iterator;
import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;

import static org.apache.flink.util.Preconditions.checkArgument;

/** Group of consumed {@link IntermediateResultPartitionID}s. */
public class ConsumedPartitionGroup implements Iterable<IntermediateResultPartitionID> {

    private final List<IntermediateResultPartitionID> resultPartitions;

    private final AtomicInteger unfinishedPartitions;

    private final IntermediateDataSetID intermediateDataSetID;

    private ConsumedPartitionGroup(List<IntermediateResultPartitionID> resultPartitions) {
        checkArgument(
                resultPartitions.size() > 0,
                "The size of result partitions in the ConsumedPartitionGroup should be larger than 0.");
        this.intermediateDataSetID = resultPartitions.get(0).getIntermediateDataSetID();

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
            List<IntermediateResultPartitionID> resultPartitions) {
        return new ConsumedPartitionGroup(resultPartitions);
    }

    public static ConsumedPartitionGroup fromSinglePartition(
            IntermediateResultPartitionID resultPartition) {
        return new ConsumedPartitionGroup(Collections.singletonList(resultPartition));
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

    @VisibleForTesting
    public int getNumberOfUnfinishedPartitions() {
        return unfinishedPartitions.get();
    }

    public boolean areAllPartitionsFinished() {
        return unfinishedPartitions.get() == 0;
    }
}
