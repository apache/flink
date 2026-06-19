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

package org.apache.flink.runtime.io.network.partition.hybrid.tiered.common;

import org.apache.flink.runtime.io.network.partition.ResultPartitionID;
import org.apache.flink.runtime.jobgraph.IntermediateDataSetID;
import org.apache.flink.util.AbstractID;

/** Utils to convert the Ids to Tiered Storage Ids, or vice versa. */
public class TieredStorageIdMappingUtils {

    public static TieredStorageTopicId convertId(IntermediateDataSetID intermediateDataSetID) {
        return new TieredStorageTopicId(intermediateDataSetID.getBytes());
    }

    public static IntermediateDataSetID convertId(TieredStorageTopicId topicId) {
        return new IntermediateDataSetID(new AbstractID(topicId.getBytes()));
    }

    public static TieredStoragePartitionId convertId(ResultPartitionID resultPartitionId) {
        return new TieredStoragePartitionId(resultPartitionId);
    }

    public static ResultPartitionID convertId(TieredStoragePartitionId partitionId) {
        return partitionId.getPartitionID();
    }

    public static TieredStorageSubpartitionId convertId(int subpartitionId) {
        return new TieredStorageSubpartitionId(subpartitionId);
    }

    public static int convertId(TieredStorageSubpartitionId subpartitionId) {
        return subpartitionId.getSubpartitionId();
    }
}
