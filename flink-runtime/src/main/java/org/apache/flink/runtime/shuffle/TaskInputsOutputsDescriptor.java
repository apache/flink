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

package org.apache.flink.runtime.shuffle;

import org.apache.flink.runtime.io.network.partition.ResultPartitionType;
import org.apache.flink.runtime.jobgraph.IntermediateDataSetID;

import java.util.Collections;
import java.util.Map;

import static org.apache.flink.util.Preconditions.checkNotNull;

/** Describes inputs and outputs information of a task. */
public class TaskInputsOutputsDescriptor {

    // Number of input gates
    private final int inputGateNums;

    // Number of input channels per dataSet.
    private final Map<IntermediateDataSetID, Integer> inputChannelNums;

    // Number of the partitions to be re-consumed.
    Map<IntermediateDataSetID, Integer> partitionReuseCount;

    // Number of subpartitions per dataSet.
    private final Map<IntermediateDataSetID, Integer> subpartitionNums;

    // Result partition types of input channels.
    private final Map<IntermediateDataSetID, ResultPartitionType> inputPartitionTypes;

    // ResultPartitionType per dataSet.
    private final Map<IntermediateDataSetID, ResultPartitionType> partitionTypes;

    private TaskInputsOutputsDescriptor(
            int inputGateNums,
            Map<IntermediateDataSetID, Integer> inputChannelNums,
            Map<IntermediateDataSetID, Integer> partitionReuseCount,
            Map<IntermediateDataSetID, Integer> subpartitionNums,
            Map<IntermediateDataSetID, ResultPartitionType> inputPartitionTypes,
            Map<IntermediateDataSetID, ResultPartitionType> partitionTypes) {

        checkNotNull(inputChannelNums);
        checkNotNull(partitionReuseCount);
        checkNotNull(subpartitionNums);
        checkNotNull(inputPartitionTypes);
        checkNotNull(partitionTypes);

        this.inputGateNums = inputGateNums;
        this.inputChannelNums = inputChannelNums;
        this.partitionReuseCount = partitionReuseCount;
        this.subpartitionNums = subpartitionNums;
        this.inputPartitionTypes = inputPartitionTypes;
        this.partitionTypes = partitionTypes;
    }

    public int getInputGateNums() {
        return inputGateNums;
    }

    public Map<IntermediateDataSetID, Integer> getInputChannelNums() {
        return Collections.unmodifiableMap(inputChannelNums);
    }

    public Map<IntermediateDataSetID, Integer> getPartitionReuseCount() {
        return partitionReuseCount;
    }

    public Map<IntermediateDataSetID, Integer> getSubpartitionNums() {
        return Collections.unmodifiableMap(subpartitionNums);
    }

    public Map<IntermediateDataSetID, ResultPartitionType> getInputPartitionTypes() {
        return Collections.unmodifiableMap(inputPartitionTypes);
    }

    public Map<IntermediateDataSetID, ResultPartitionType> getPartitionTypes() {
        return Collections.unmodifiableMap(partitionTypes);
    }

    public static TaskInputsOutputsDescriptor from(
            int inputGateNums,
            Map<IntermediateDataSetID, Integer> inputChannelNums,
            Map<IntermediateDataSetID, Integer> partitionReuseCount,
            Map<IntermediateDataSetID, Integer> subpartitionNums,
            Map<IntermediateDataSetID, ResultPartitionType> inputPartitionTypes,
            Map<IntermediateDataSetID, ResultPartitionType> partitionTypes) {

        return new TaskInputsOutputsDescriptor(
                inputGateNums,
                inputChannelNums,
                partitionReuseCount,
                subpartitionNums,
                inputPartitionTypes,
                partitionTypes);
    }
}
