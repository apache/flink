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

import org.apache.flink.runtime.io.network.partition.hybrid.tiered.tier.TierShuffleDescriptor;
import org.apache.flink.runtime.shuffle.ShuffleMetrics;

import static org.apache.flink.util.Preconditions.checkNotNull;

/** Partition with shuffle metrics for tiered storage. */
public class TieredPartitionWithMetrics {
    private final TierShuffleDescriptor shuffleDescriptor;
    private final ShuffleMetrics partitionMetrics;

    public TieredPartitionWithMetrics(
            TierShuffleDescriptor shuffleDescriptor, ShuffleMetrics partitionMetrics) {
        this.shuffleDescriptor = checkNotNull(shuffleDescriptor);
        this.partitionMetrics = checkNotNull(partitionMetrics);
    }

    public ShuffleMetrics getPartitionMetrics() {
        return partitionMetrics;
    }

    public TierShuffleDescriptor getPartition() {
        return shuffleDescriptor;
    }
}
