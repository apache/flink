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

package org.apache.flink.runtime.io.network.partition.hybrid.tiered.storage;

import org.apache.flink.runtime.io.network.partition.ResultSubpartitionIndexSet;
import org.apache.flink.runtime.io.network.partition.hybrid.tiered.common.TieredStorageInputChannelId;
import org.apache.flink.runtime.io.network.partition.hybrid.tiered.common.TieredStoragePartitionId;

/** Describe the different data sources in {@link TieredStorageConsumerClient}. */
public class TieredStorageConsumerSpec {

    private final int gateIndex;

    private final TieredStoragePartitionId tieredStoragePartitionId;

    private final TieredStorageInputChannelId tieredStorageInputChannelId;

    private final ResultSubpartitionIndexSet tieredStorageSubpartitionIds;

    public TieredStorageConsumerSpec(
            int gateIndex,
            TieredStoragePartitionId tieredStoragePartitionId,
            TieredStorageInputChannelId tieredStorageInputChannelId,
            ResultSubpartitionIndexSet tieredStorageSubpartitionIds) {
        this.gateIndex = gateIndex;
        this.tieredStoragePartitionId = tieredStoragePartitionId;
        this.tieredStorageInputChannelId = tieredStorageInputChannelId;
        this.tieredStorageSubpartitionIds = tieredStorageSubpartitionIds;
    }

    public int getGateIndex() {
        return gateIndex;
    }

    public TieredStoragePartitionId getPartitionId() {
        return tieredStoragePartitionId;
    }

    public TieredStorageInputChannelId getInputChannelId() {
        return tieredStorageInputChannelId;
    }

    public ResultSubpartitionIndexSet getSubpartitionIds() {
        return tieredStorageSubpartitionIds;
    }
}
