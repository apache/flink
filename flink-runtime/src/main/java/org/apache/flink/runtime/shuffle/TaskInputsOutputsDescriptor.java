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

    // Number of input channels per dataSet.
    private final Map<IntermediateDataSetID, Integer> inputChannelNums;

    // Number of subpartitions per dataSet.
    private final Map<IntermediateDataSetID, Integer> subpartitionNums;

    // ResultPartitionType per dataSet.
    private final Map<IntermediateDataSetID, ResultPartitionType> partitionTypes;

    private TaskInputsOutputsDescriptor(
            Map<IntermediateDataSetID, Integer> inputChannelNums,
            Map<IntermediateDataSetID, Integer> subpartitionNums,
            Map<IntermediateDataSetID, ResultPartitionType> partitionTypes) {

        checkNotNull(inputChannelNums);
        checkNotNull(subpartitionNums);
        checkNotNull(partitionTypes);

        this.inputChannelNums = inputChannelNums;
        this.subpartitionNums = subpartitionNums;
        this.partitionTypes = partitionTypes;
    }

    public Map<IntermediateDataSetID, Integer> getInputChannelNums() {
        return Collections.unmodifiableMap(inputChannelNums);
    }

    public Map<IntermediateDataSetID, Integer> getSubpartitionNums() {
        return Collections.unmodifiableMap(subpartitionNums);
    }

    public Map<IntermediateDataSetID, ResultPartitionType> getPartitionTypes() {
        return Collections.unmodifiableMap(partitionTypes);
    }

    public static TaskInputsOutputsDescriptor from(
            Map<IntermediateDataSetID, Integer> inputChannelNums,
            Map<IntermediateDataSetID, Integer> subpartitionNums,
            Map<IntermediateDataSetID, ResultPartitionType> partitionTypes) {

        return new TaskInputsOutputsDescriptor(inputChannelNums, subpartitionNums, partitionTypes);
    }
}
