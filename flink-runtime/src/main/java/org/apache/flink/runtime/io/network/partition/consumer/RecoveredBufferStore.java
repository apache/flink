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
 * Per-channel store for recovered buffers during unaligned checkpoint recovery. Buffers are
 * either in-memory (ready for consumption) or on disk (pending spill entries).
 *
 * <h3>Locking contract</h3>
 *
 * <p>The store's intrinsic monitor IS the channel-private lock. Callers MUST hold
 * {@code synchronized(store)} when invoking the consumer-side queries ({@link #tryTake},
 * {@link #peekNextDataType}, {@link #isEmpty}) and the setters; implementations enforce this with
 * an internal {@code assert Thread.holdsLock(this)} that fires under {@code -ea}.
 *
 * <p>{@link #size()} is exempt and lock-free: a metric/bookkeeping read that tolerates a slightly
 * stale value. Callers that need a consistent {@code isEmpty + size} pair must take the lock
 * around both reads themselves.
 *
 * <p>The lifecycle methods ({@link #checkpoint}, {@link #releaseAll},
 * {@link #notifyCheckpointStopped}) self-manage their store-level locking and fire any
 * coordinator callback <em>outside</em> the lock to avoid deadlock with the coordinator.
 *
 * <p>Use {@link #EMPTY} as a sentinel when no recovered data is present, rather than holding
 * {@code null}; callers still wrap calls in {@code synchronized(store)} to keep the same call
 * shape regardless of which implementation backs the channel.
 */
@Internal
public interface RecoveredBufferStore {

    /** Singleton no-op store used when there is no recovered data for a channel. */
    RecoveredBufferStore EMPTY = new EmptyRecoveredBufferStore();

    /** Next buffer from the store; null if no ready buffer is available. */
    @Nullable
    Buffer tryTake();

    /** Data type of the next ready buffer, or {@link Buffer.DataType#NONE} if empty. */
    Buffer.DataType peekNextDataType();

    /** True if the ready queue is empty and no pending spill entries exist. */
    boolean isEmpty();

    /**
     * Total buffers held for the bound channel: ready buffers in memory plus pending entries on
     * disk. Used for in-use / backlog accounting.
     */
    int size();

    /**
     * Snapshots ready buffers into {@code writer}, then notifies the registered coordinator
     * (outside the store lock to avoid deadlock).
     */
    void checkpoint(ChannelStateWriter writer, long checkpointId) throws IOException;

    /**
     * Releases all buffers and clears state. Notifies the coordinator (if any) so it can drop
     * still-pending spill entries for this channel.
     */
    void releaseAll();

    /**
     * Forwards a checkpoint-stopped notification (completion or abort) to the coordinator. The
     * store keeps no per-checkpoint state; cross-channel bookkeeping (wait-set) lives there.
     */
    void notifyCheckpointStopped(long checkpointId);

    /** Registers the cross-channel coordinator. Pass non-null when one exists. */
    void setCoordinator(RecoveredBufferStoreCoordinator coordinator);

    /**
     * Listener fired when a buffer is added to a previously empty ready queue. The typical
     * recipient is the owning InputChannel, which uses it to wake up the Task thread.
     */
    void setDataAvailableListener(DataAvailableListener listener);

    @FunctionalInterface
    interface DataAvailableListener {

        void onDataAvailable();
    }
}
