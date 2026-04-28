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
 * Cross-channel coordinator notified by per-channel
 * {@link org.apache.flink.runtime.io.network.partition.consumer.RecoveredBufferStore} instances on
 * lifecycle events. Implementations centralise bookkeeping that spans multiple channels (such as
 * checkpoint wait-sets or shared on-disk spill state) and react to per-channel transitions.
 *
 * <p>{@link #onChannelCheckpointStarted}, {@link #onChannelCheckpointStopped} and
 * {@link #onChannelReleased} are invoked from the Task thread, <em>outside</em> the calling store's
 * lock, so implementations may freely acquire their own synchronisation without deadlock risk.
 *
 * <p>{@link #getCurrentDrainHead()} is invoked <em>inside</em> the calling store's lock so the
 * store can capture a consistent (readyBuffers, drainHead) pair atomically. Implementations must
 * therefore avoid blocking and must not acquire any lock that participates in the store-lock cycle
 * — a plain {@code volatile} read of a field maintained by the drain bundle is the intended
 * implementation.
 */
@Internal
public interface RecoveredBufferStoreCoordinator {

    /**
     * Returns the current drain head — the position of the next entry the dispatcher's drain bundle
     * will pop from the global FIFO queue. Used by {@code RecoveredBufferStore#checkpoint} to
     * record a per-channel checkpoint cutoff (startPos) atomically with the channel's ready-buffer
     * snapshot. Returns {@link EntryPosition#END} when the queue is empty (no more disk entries
     * pending).
     *
     * <p>Reads must be cheap and lock-free; do not block, do not allocate, do not acquire any
     * lock that the calling store holds (or that the dispatcher holds during drain).
     */
    EntryPosition getCurrentDrainHead();

    /**
     * Invoked from inside {@code RecoveredBufferStore#checkpoint} after the store has snapshotted
     * its ready buffers. Implementations use this to maintain a wait-set across channels and, when
     * all channels have reported in, drain pending spill entries into the checkpoint.
     *
     * <p>{@code startPos} is the {@code drainHead} value the store captured atomically with its
     * ready-buffer snapshot, under the store lock. Implementations must record it as the
     * per-channel cutoff used by phase-2 to decide which spill entries belong to this checkpoint
     * versus this channel's Step 1 ready snapshot.
     */
    void onChannelCheckpointStarted(
            long checkpointId, InputChannelInfo channelInfo, EntryPosition startPos);

    /**
     * Invoked from inside {@code RecoveredBufferStore#notifyCheckpointStopped} when the owning
     * channel has finished or aborted a checkpoint. Implementations use this to drop any wait-set
     * still tied to the stopped checkpoint so a later {@link #onChannelReleased} or a late
     * {@link #onChannelCheckpointStarted} cannot trigger a phase-2 drain to a checkpoint that has
     * already been concluded by the task.
     */
    void onChannelCheckpointStopped(long checkpointId, InputChannelInfo channelInfo);

    /**
     * Invoked from inside {@code RecoveredBufferStore#releaseAll} so the coordinator can drop
     * disk-resident spill entries still associated with the released channel.
     */
    void onChannelReleased(InputChannelInfo channelInfo);
}
