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

package org.apache.flink.runtime.deployment;

import org.apache.flink.runtime.io.network.partition.ResultPartition;
import org.apache.flink.runtime.io.network.partition.ResultPartitionType;
import org.apache.flink.runtime.jobgraph.IntermediateDataSetID;
import org.apache.flink.runtime.jobgraph.IntermediateResultPartitionID;
import org.apache.flink.runtime.shuffle.PartitionDescriptor;
import org.apache.flink.runtime.shuffle.ShuffleDescriptor;
import org.apache.flink.runtime.state.KeyGroupRangeAssignment;

import java.io.Serializable;

import static org.apache.flink.util.Preconditions.checkArgument;
import static org.apache.flink.util.Preconditions.checkNotNull;

/**
 * Deployment descriptor for a result partition.
 *
 * @see ResultPartition
 */
public class ResultPartitionDeploymentDescriptor implements Serializable {

    private static final long serialVersionUID = 6343547936086963705L;

    private final PartitionDescriptor partitionDescriptor;

    private final ShuffleDescriptor shuffleDescriptor;

    private final int maxParallelism;

    /** Parallelism of the consuming job vertices, or {@link #UNKNOWN_CONSUMER_PARALLELISM}. */
    private final int consumerParallelism;

    /**
     * The consumer parallelism is not decided yet when the producer is deployed. This only happens
     * in dynamic graphs (AdaptiveBatchScheduler), where the parallelism of a consumer vertex may be
     * decided after its producers are deployed. Every other partition carries the actual consumer
     * parallelism.
     */
    public static final int UNKNOWN_CONSUMER_PARALLELISM = -1;

    public ResultPartitionDeploymentDescriptor(
            PartitionDescriptor partitionDescriptor,
            ShuffleDescriptor shuffleDescriptor,
            int maxParallelism,
            int consumerParallelism) {
        this.partitionDescriptor = checkNotNull(partitionDescriptor);
        this.shuffleDescriptor = checkNotNull(shuffleDescriptor);
        KeyGroupRangeAssignment.checkParallelismPreconditions(maxParallelism);
        this.maxParallelism = maxParallelism;
        checkArgument(
                consumerParallelism > 0 || consumerParallelism == UNKNOWN_CONSUMER_PARALLELISM,
                "Invalid consumer parallelism %s.",
                consumerParallelism);
        this.consumerParallelism = consumerParallelism;
    }

    public IntermediateDataSetID getResultId() {
        return partitionDescriptor.getResultId();
    }

    public IntermediateResultPartitionID getPartitionId() {
        return partitionDescriptor.getPartitionId();
    }

    /** Whether the resultPartition is a broadcast edge. */
    public boolean isBroadcast() {
        return partitionDescriptor.isBroadcast();
    }

    public ResultPartitionType getPartitionType() {
        return partitionDescriptor.getPartitionType();
    }

    public int getTotalNumberOfPartitions() {
        return partitionDescriptor.getTotalNumberOfPartitions();
    }

    public int getNumberOfSubpartitions() {
        return partitionDescriptor.getNumberOfSubpartitions();
    }

    public boolean isNumberOfPartitionConsumerUndefined() {
        return partitionDescriptor.isNumberOfPartitionConsumerUndefined();
    }

    public int getMaxParallelism() {
        return maxParallelism;
    }

    /**
     * Returns the parallelism of the job vertices consuming this partition, or {@link
     * #UNKNOWN_CONSUMER_PARALLELISM} if it is not decided yet. Unlike {@link
     * #getNumberOfSubpartitions()}, this is the actual consumer parallelism for every distribution
     * pattern.
     */
    public int getConsumerParallelism() {
        return consumerParallelism;
    }

    public ShuffleDescriptor getShuffleDescriptor() {
        return shuffleDescriptor;
    }

    @Override
    public String toString() {
        return String.format(
                "ResultPartitionDeploymentDescriptor [PartitionDescriptor: %s, "
                        + "ShuffleDescriptor: %s]",
                partitionDescriptor, shuffleDescriptor);
    }
}
