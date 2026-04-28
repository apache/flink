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

package org.apache.flink.runtime.checkpoint.channel;

import org.apache.flink.annotation.Internal;

/**
 * Cross-channel coordinator notified by per-channel {@link
 * org.apache.flink.runtime.io.network.partition.consumer.RecoveredBufferStore} instances on
 * lifecycle events. Centralises bookkeeping that spans multiple channels (checkpoint wait-sets,
 * shared on-disk spill state).
 *
 * <p>The {@code onChannel*} callbacks fire from the Task thread <em>outside</em> the calling
 * store's lock, so implementations may freely acquire their own synchronisation. {@link
 * #getCurrentDrainHead()} fires <em>inside</em> the calling store's lock so the store can capture a
 * consistent (readyBuffers, drainHead) pair atomically — implementations must therefore avoid
 * blocking and must not acquire any lock participating in the store-lock cycle (a plain {@code
 * volatile} read is the intended implementation).
 */
@Internal
public interface RecoveredBufferStoreCoordinator {

    /**
     * Position of the next entry the drain bundle will pop from the global FIFO. Returns {@link
     * EntryPosition#END} when no disk entries are pending. Must be cheap and lock-free.
     */
    EntryPosition getCurrentDrainHead();

    /**
     * Invoked from {@code RecoveredBufferStore#checkpoint} after the store has snapshotted its
     * ready buffers. {@code startPos} is the drain-head captured atomically with that snapshot;
     * phase-2 uses it as the per-channel cutoff to split spill entries between this channel's Step
     * 1 snapshot and the global checkpoint drain.
     */
    void onChannelCheckpointStarted(
            long checkpointId, InputChannelInfo channelInfo, EntryPosition startPos);

    /**
     * Invoked from {@code RecoveredBufferStore#notifyCheckpointStopped} when the owning channel has
     * finished or aborted a checkpoint. Used to drop a wait-set still tied to the stopped
     * checkpoint so a later release or late start callback cannot trigger a phase-2 drain into a
     * concluded checkpoint.
     */
    void onChannelCheckpointStopped(long checkpointId, InputChannelInfo channelInfo);

    /**
     * Invoked from {@code RecoveredBufferStore#releaseAll} so the coordinator can drop
     * disk-resident spill entries still associated with the released channel.
     */
    void onChannelReleased(InputChannelInfo channelInfo);
}
