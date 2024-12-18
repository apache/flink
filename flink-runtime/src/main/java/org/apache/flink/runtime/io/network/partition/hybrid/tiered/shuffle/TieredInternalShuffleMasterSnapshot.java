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

package org.apache.flink.runtime.io.network.partition.hybrid.tiered.shuffle;

import org.apache.flink.runtime.io.network.partition.ResultPartitionID;
import org.apache.flink.runtime.shuffle.ShuffleDescriptor;
import org.apache.flink.runtime.shuffle.ShuffleMasterSnapshot;

import java.util.Map;

/**
 * The internal {@link ShuffleMasterSnapshot} for hybrid shuffle. This bump shuffle descriptors and
 * all tiers snapshot.
 */
public class TieredInternalShuffleMasterSnapshot implements ShuffleMasterSnapshot {
    private final Map<ResultPartitionID, ShuffleDescriptor> shuffleDescriptors;

    private final AllTieredShuffleMasterSnapshots allTierSnapshots;

    public TieredInternalShuffleMasterSnapshot(
            Map<ResultPartitionID, ShuffleDescriptor> shuffleDescriptors,
            AllTieredShuffleMasterSnapshots allTierSnapshots) {
        this.shuffleDescriptors = shuffleDescriptors;
        this.allTierSnapshots = allTierSnapshots;
    }

    public Map<ResultPartitionID, ShuffleDescriptor> getShuffleDescriptors() {
        return shuffleDescriptors;
    }

    public AllTieredShuffleMasterSnapshots getAllTierSnapshots() {
        return allTierSnapshots;
    }

    @Override
    public boolean isIncremental() {
        return true;
    }
}
