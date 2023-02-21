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

package org.apache.flink.runtime.scheduler.adaptivebatch;

import org.apache.flink.runtime.executiongraph.IndexRange;
import org.apache.flink.runtime.executiongraph.IntermediateResultInfo;
import org.apache.flink.runtime.executiongraph.ResultPartitionBytes;

/**
 * The blocking result info, which will be used to calculate the vertex parallelism and input infos.
 */
public interface BlockingResultInfo extends IntermediateResultInfo {

    /**
     * Return the num of bytes produced(numBytesProduced) by the producer.
     *
     * <p>The difference between numBytesProduced and numBytesOut : numBytesProduced represents the
     * number of bytes actually produced, and numBytesOut represents the number of bytes sent to
     * downstream tasks. In unicast scenarios, these two values should be equal. In broadcast
     * scenarios, numBytesOut should be (N * numBytesProduced), where N refers to the number of
     * subpartitions.
     *
     * @return the num of bytes produced by the producer
     */
    long getNumBytesProduced();

    /**
     * Return the aggregated num of bytes according to the index range for partition and
     * subpartition.
     *
     * @param partitionIndexRange range of the index of the consumed partition.
     * @param subpartitionIndexRange range of the index of the consumed subpartition.
     * @return aggregated bytes according to the index ranges.
     */
    long getNumBytesProduced(IndexRange partitionIndexRange, IndexRange subpartitionIndexRange);

    /**
     * Record the information of the result partition.
     *
     * @param partitionIndex the intermediate result partition index
     * @param partitionBytes the {@link ResultPartitionBytes} of the partition
     */
    void recordPartitionInfo(int partitionIndex, ResultPartitionBytes partitionBytes);

    /**
     * Reset the information of the result partition.
     *
     * @param partitionIndex the intermediate result partition index
     */
    void resetPartitionInfo(int partitionIndex);
}
