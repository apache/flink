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

package org.apache.flink.streaming.api.graph;

import org.apache.flink.annotation.Internal;
import org.apache.flink.runtime.io.network.partition.ResultPartitionType;
import org.apache.flink.runtime.jobgraph.IntermediateDataSetID;
import org.apache.flink.streaming.runtime.partitioner.StreamPartitioner;
import org.apache.flink.util.OutputTag;

import java.io.Serializable;
import java.util.Objects;

/**
 * Used by operator chain and represents a non-chained output of the corresponding stream operator.
 */
@Internal
public class NonChainedOutput implements Serializable {

    private static final long serialVersionUID = 1L;

    /** Is unaligned checkpoint supported. */
    private final boolean supportsUnalignedCheckpoints;

    /** ID of the producer {@link StreamNode}. */
    private final int sourceNodeId;

    /** Parallelism of the consumer vertex. */
    private final int consumerParallelism;

    /** Max parallelism of the consumer vertex. */
    private final int consumerMaxParallelism;

    /** Buffer flush timeout of this output. */
    private final long bufferTimeout;

    /** ID of the produced intermediate dataset. */
    private final IntermediateDataSetID dataSetId;

    /** Whether this intermediate dataset is a persistent dataset or not. */
    private final boolean isPersistentDataSet;

    /** The side-output tag (if any). */
    private final OutputTag<?> outputTag;

    /** The corresponding data partitioner. */
    private StreamPartitioner<?> partitioner;

    /** Target {@link ResultPartitionType}. */
    private ResultPartitionType partitionType;

    public NonChainedOutput(
            boolean supportsUnalignedCheckpoints,
            int sourceNodeId,
            int consumerParallelism,
            int consumerMaxParallelism,
            long bufferTimeout,
            boolean isPersistentDataSet,
            IntermediateDataSetID dataSetId,
            OutputTag<?> outputTag,
            StreamPartitioner<?> partitioner,
            ResultPartitionType partitionType) {
        this.supportsUnalignedCheckpoints = supportsUnalignedCheckpoints;
        this.sourceNodeId = sourceNodeId;
        this.consumerParallelism = consumerParallelism;
        this.consumerMaxParallelism = consumerMaxParallelism;
        this.bufferTimeout = bufferTimeout;
        this.isPersistentDataSet = isPersistentDataSet;
        this.dataSetId = dataSetId;
        this.outputTag = outputTag;
        this.partitioner = partitioner;
        this.partitionType = partitionType;
    }

    public boolean supportsUnalignedCheckpoints() {
        return supportsUnalignedCheckpoints;
    }

    public int getSourceNodeId() {
        return sourceNodeId;
    }

    public int getConsumerParallelism() {
        return consumerParallelism;
    }

    public int getConsumerMaxParallelism() {
        return consumerMaxParallelism;
    }

    public long getBufferTimeout() {
        return bufferTimeout;
    }

    public IntermediateDataSetID getDataSetId() {
        return dataSetId;
    }

    public IntermediateDataSetID getPersistentDataSetId() {
        return isPersistentDataSet ? dataSetId : null;
    }

    public OutputTag<?> getOutputTag() {
        return outputTag;
    }

    public void setPartitioner(StreamPartitioner<?> partitioner) {
        this.partitioner = partitioner;
    }

    public void setPartitionType(ResultPartitionType partitionType) {
        this.partitionType = partitionType;
    }

    public StreamPartitioner<?> getPartitioner() {
        return partitioner;
    }

    public ResultPartitionType getPartitionType() {
        return partitionType;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }
        NonChainedOutput output = (NonChainedOutput) o;
        return Objects.equals(dataSetId, output.dataSetId);
    }

    @Override
    public int hashCode() {
        return Objects.hash(dataSetId);
    }
}
