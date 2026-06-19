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

package org.apache.flink.runtime.io.network.partition.hybrid.tiered.tier;

import org.apache.flink.runtime.io.network.partition.hybrid.tiered.common.TieredStoragePartitionId;

import java.util.Collection;
import java.util.concurrent.CompletableFuture;

/**
 * A handler to process the call back result from each tier, the callbacks can be some events, some
 * errors, some exceptions, etc. If you want to process some new callbacks, you can add more methods
 * to this handler interface.
 *
 * <p>When the tier happens some events, the tier will call these methods, then the framework can
 * process the events.
 */
public interface TierShuffleHandler {

    /** A callback to process the event of releasing a collection of tiered result partitions. */
    CompletableFuture<?> onReleasePartitions(Collection<TieredStoragePartitionId> partitionIds);

    /** A callback to process the fatal error (if the error exists). */
    void onFatalError(Throwable throwable);
}
