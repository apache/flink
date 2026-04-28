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

package org.apache.flink.runtime.io.network.partition.consumer;

import org.apache.flink.annotation.Internal;
import org.apache.flink.runtime.checkpoint.channel.ChannelStateWriter;
import org.apache.flink.runtime.checkpoint.channel.InputChannelInfo;
import org.apache.flink.runtime.checkpoint.channel.RecoveredBufferStoreCoordinator;
import org.apache.flink.runtime.io.network.buffer.Buffer;

import javax.annotation.Nullable;

import java.io.IOException;

/**
 * Per-channel store for recovered buffers during unaligned checkpoint recovery. Buffers can be
 * either in-memory (ready for consumption) or on disk (pending spill entries).
 *
 * <h3>Locking contract</h3>
 *
 * <p>The store's intrinsic monitor IS the channel-private lock. Callers MUST hold {@code synchronized
 * (store)} when invoking the consumer-side query methods ({@link #tryTake}, {@link
 * #peekNextDataType}, {@link #isEmpty}) and the setters ({@link #setCoordinator},
 * {@link #setDataAvailableListener}). Implementations enforce this with an internal {@code assert
 * Thread.holdsLock(this)} that fires under {@code -ea}.
 *
 * <p>{@link #size()} is exempt and lock-free: it is intended for metric / gate-bookkeeping paths
 * that already tolerate a slightly stale read. Callers that need a consistent {@code isEmpty +
 * size} pair must wrap both calls in a single {@code synchronized (store)} block themselves.
 *
 * <p>The lifecycle methods {@link #checkpoint}, {@link #releaseAll} and {@link
 * #notifyCheckpointStopped} self-manage their store-level locking and fire any coordinator
 * callback <em>outside</em> the lock to avoid deadlock with the coordinator's own synchronisation.
 *
 * <p>Use {@link #EMPTY} as a sentinel when no recovered data is present (non-filtering mode, or
 * after recovery has fully drained), rather than holding {@code null} references. The sentinel's
 * methods are no-ops; callers still wrap them in {@code synchronized (store)} to keep the same
 * call shape regardless of which implementation backs the channel.
 */
@Internal
public interface RecoveredBufferStore {

    /**
     * Singleton no-op store used when there is no recovered data for a channel. All query methods
     * return their neutral/empty sentinel values; all mutating methods and callback setters are
     * no-ops.
     */
    RecoveredBufferStore EMPTY = new EmptyRecoveredBufferStore();

    /**
     * Takes the next buffer from the store. Returns null if no ready buffer is available.
     *
     * @return the next buffer, or null if no ready buffer available
     */
    @Nullable
    Buffer tryTake();

    /**
     * Peeks the data type of the next ready buffer without removing it.
     *
     * @return the data type of the next buffer, or {@link Buffer.DataType#NONE} if empty
     */
    Buffer.DataType peekNextDataType();

    /** Returns true if the ready buffer queue is empty and no pending spill entries exist. */
    boolean isEmpty();

    /**
     * Returns the total number of buffers held by this store for the bound channel: ready buffers
     * already in memory plus pending entries currently spilled to disk. Callers use this for
     * in-use / backlog accounting, which should reflect every buffer the recovery pipeline still
     * owes the channel regardless of where it is physically stored.
     */
    int size();

    /**
     * Checkpoints the current store contents to the given ChannelStateWriter. Implementations
     * should snapshot ready buffers first, then notify the registered {@link
     * RecoveredBufferStoreCoordinator} (if any) <em>outside</em> any store-level lock to avoid
     * deadlock with the coordinator's own synchronisation.
     *
     * <p>The store is bound to a single {@link InputChannelInfo} at construction time; both the
     * snapshot and the coordinator notification use that bound channel info.
     *
     * @param writer the channel state writer to checkpoint to
     * @param checkpointId the checkpoint ID
     * @throws IOException if checkpointing fails
     */
    void checkpoint(ChannelStateWriter writer, long checkpointId) throws IOException;

    /**
     * Releases all buffers held in this store and clears all state. Before clearing local state,
     * the registered {@link RecoveredBufferStoreCoordinator} (if any) is notified so the
     * coordinator can drop any still-pending spill entries for this channel from disk.
     */
    void releaseAll();

    /**
     * Notifies the registered {@link RecoveredBufferStoreCoordinator} (if any) that the owning
     * channel has finished or aborted the given checkpoint. The notification fires <em>outside</em>
     * any store-level lock so the coordinator can safely acquire its own lock without deadlock.
     *
     * <p>Called from {@code ChannelStatePersister#stopPersisting}, which itself is invoked once on
     * every channel both on checkpoint completion (all barriers received) and on checkpoint abort.
     * The store does not maintain per-checkpoint state; the call is a pure passthrough so that
     * cross-channel bookkeeping (such as a checkpoint wait-set) lives in the coordinator.
     *
     * @param checkpointId the ID of the checkpoint that just stopped for this channel
     */
    void notifyCheckpointStopped(long checkpointId);

    /**
     * Registers the {@link RecoveredBufferStoreCoordinator} that owns cross-channel bookkeeping
     * (such as the checkpoint wait-set and disk-resident spill entries). The store invokes the
     * coordinator from {@link #checkpoint}, {@link #notifyCheckpointStopped} and
     * {@link #releaseAll} on lifecycle transitions. Do not call this method (or pass {@code null})
     * when there is no coordinator.
     *
     * @param coordinator the coordinator to notify; replaces any previously registered coordinator
     */
    void setCoordinator(RecoveredBufferStoreCoordinator coordinator);

    /**
     * Registers a callback that is invoked when a buffer is added to a previously empty ready
     * queue. Used to notify the InputChannel that data is available for consumption.
     *
     * @param listener the listener to invoke; replaces any previously registered listener
     */
    void setDataAvailableListener(DataAvailableListener listener);

    /**
     * Listener invoked when a buffer has been added to a previously empty ready queue. The typical
     * recipient is the owning InputChannel, which uses it to wake up the Task thread.
     */
    @FunctionalInterface
    interface DataAvailableListener {

        /** Called when a buffer becomes available for consumption. */
        void onDataAvailable();
    }
}
