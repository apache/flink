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
import org.apache.flink.annotation.VisibleForTesting;
import org.apache.flink.runtime.checkpoint.channel.ChannelStateWriter;
import org.apache.flink.runtime.checkpoint.channel.EntryPosition;
import org.apache.flink.runtime.checkpoint.channel.InputChannelInfo;
import org.apache.flink.runtime.checkpoint.channel.RecoveredBufferStoreCoordinator;
import org.apache.flink.runtime.io.network.buffer.Buffer;
import org.apache.flink.util.CloseableIterator;

import javax.annotation.Nullable;
import javax.annotation.concurrent.GuardedBy;

import java.io.IOException;
import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.List;

import static org.apache.flink.util.Preconditions.checkNotNull;

/**
 * Per-channel store for recovered buffers. Buffers are either ready (in-memory) or pending (on
 * disk, tracked by count only — the actual entries are owned by FilteredBufferDispatcher).
 *
 * <h3>Locking</h3>
 *
 * <p>Two monitors guard the race-relevant paths: the owning gate's lock (returned by {@code
 * SingleInputGate#getGateLock()}, henceforth <i>gate lock</i>) and this store's intrinsic monitor.
 * The acquisition order is always {@code gate → store}; the same order is enforced by the consumer
 * (task read), the producer (drain / EOICS publish) and conversion ({@code
 * SingleInputGate#convertRecoveredInputChannels}). This is what closes the FLINK-39519
 * stale-enqueue race: producer add+fire and conversion's listener replace + {@code channels[i]}
 * swap are serialised through the same gate lock.
 *
 * <p>Per-method contracts (each one's preconditions are restated as {@code assert}s at the entry):
 *
 * <ul>
 *   <li><b>Producer mutators</b> ({@link #addBuffer}, {@link #addBufferAfterDisk}): caller holds
 *       the gate lock; the store self-manages its own monitor and fires the data-available
 *       listener inline. Firing inside the store monitor is safe because the gate lock is held by
 *       the caller, so {@code queueChannel} re-acquires it as a recursive intrinsic-monitor entry
 *       — no AB-BA cycle.
 *   <li><b>Race-path readers / setter</b> ({@link #tryTake}, {@link #peekNextDataType}, {@link
 *       #setDataAvailableListener}): {@code @GuardedBy("this")} — caller wraps the call in {@code
 *       synchronized(store)} so compound operations such as {@code tryTake() + peekNextDataType()}
 *       observe a consistent snapshot — and additionally holds the gate lock so the {@code gate →
 *       store} order is explicit.
 *   <li><b>Store-only readers / mutators</b> ({@link #isEmpty}, {@link #setCoordinator}, {@link
 *       #incrementPending}): {@code @GuardedBy("this")} only. {@link #isEmpty} is consulted from
 *       {@code RemoteInputChannel#onSenderBacklog} on the netty event loop where the gate lock is
 *       not held; {@link #setCoordinator} / {@link #incrementPending} run before the recovery
 *       flush, off the race path.
 *   <li><b>Lifecycle / coordinator</b> ({@link #checkpoint}, {@link #releaseAll}, {@link
 *       #notifyCheckpointStopped}): self-manage the store monitor and fire any captured
 *       coordinator callback after exiting the lock. Independent of the producer/consumer hot
 *       path.
 *   <li>{@link #size()} is a deliberate lock-free best-effort read.
 * </ul>
 */
@Internal
public class RecoveredBufferStoreImpl implements RecoveredBufferStore {

    private final InputChannelInfo channelInfo;

    private final Object gateLock;

    @GuardedBy("this")
    private final ArrayDeque<Buffer> readyBuffers = new ArrayDeque<>();

    @GuardedBy("this")
    private int pendingCount = 0;

    /**
     * Holds buffers whose contract is "everything before me has been delivered" (e.g.
     * {@link EndOfInputChannelStateEvent}) until {@link #pendingCount} reaches zero, then promotes
     * them into {@link #readyBuffers}. Excluded from {@link #size()} / {@link #isEmpty()}: while
     * something is deferred, {@code pendingCount > 0} already keeps the store non-empty.
     */
    @GuardedBy("this")
    private final ArrayDeque<Buffer> deferredBuffers = new ArrayDeque<>();

    private volatile boolean released = false;

    @GuardedBy("this")
    private DataAvailableListener dataAvailableListener;

    @GuardedBy("this")
    private RecoveredBufferStoreCoordinator coordinator;

    public RecoveredBufferStoreImpl(InputChannelInfo channelInfo, Object gateLock) {
        this.channelInfo = checkNotNull(channelInfo);
        this.gateLock = checkNotNull(gateLock);
    }

    /** Test-only: ties {@code gateLock} to the store itself so {@code synchronized(store)} alone satisfies both preconditions. */
    @VisibleForTesting
    public RecoveredBufferStoreImpl(InputChannelInfo channelInfo) {
        this.channelInfo = checkNotNull(channelInfo);
        this.gateLock = this;
    }

    public InputChannelInfo getChannelInfo() {
        return channelInfo;
    }

    public Object getGateLock() {
        return gateLock;
    }

    @Nullable
    @Override
    @GuardedBy("this")
    public Buffer tryTake() {
        assert Thread.holdsLock(this);
        assert Thread.holdsLock(gateLock);
        return readyBuffers.poll();
    }

    @Override
    @GuardedBy("this")
    public Buffer.DataType peekNextDataType() {
        assert Thread.holdsLock(this);
        assert Thread.holdsLock(gateLock);
        Buffer peeked = readyBuffers.peek();
        return peeked != null ? peeked.getDataType() : Buffer.DataType.NONE;
    }

    @Override
    @GuardedBy("this")
    public boolean isEmpty() {
        assert Thread.holdsLock(this);
        return readyBuffers.isEmpty() && pendingCount == 0;
    }

    @Override
    public int size() {
        return readyBuffers.size() + pendingCount;
    }

    @Override
    public void checkpoint(ChannelStateWriter writer, long checkpointId) throws IOException {
        RecoveredBufferStoreCoordinator c;
        EntryPosition startPos;
        synchronized (this) {
            c = coordinator;
            startPos = c != null ? c.getCurrentDrainHead() : EntryPosition.END;
            if (!readyBuffers.isEmpty()) {
                // Skip non-data entries (notably EndOfInputChannelStateEvent): they would be
                // rejected by ChannelStateWriteRequest#checkBufferIsBuffer and kill the writer.
                List<Buffer> retained = new ArrayList<>(readyBuffers.size());
                for (Buffer buffer : readyBuffers) {
                    if (buffer.isBuffer()) {
                        retained.add(buffer.retainBuffer());
                    }
                }
                if (!retained.isEmpty()) {
                    writer.addInputData(
                            checkpointId,
                            channelInfo,
                            ChannelStateWriter.SEQUENCE_NUMBER_RESTORED,
                            CloseableIterator.fromList(retained, Buffer::recycleBuffer));
                }
            }
        }

        if (c != null) {
            c.onChannelCheckpointStarted(checkpointId, channelInfo, startPos);
        }
    }

    @Override
    public void releaseAll() {
        RecoveredBufferStoreCoordinator c;
        synchronized (this) {
            released = true;
            for (Buffer buffer : readyBuffers) {
                buffer.recycleBuffer();
            }
            readyBuffers.clear();
            for (Buffer buffer : deferredBuffers) {
                buffer.recycleBuffer();
            }
            deferredBuffers.clear();
            pendingCount = 0;
            c = coordinator;
        }

        if (c != null) {
            c.onChannelReleased(channelInfo);
        }
    }

    @Override
    public void notifyCheckpointStopped(long checkpointId) {
        RecoveredBufferStoreCoordinator c;
        synchronized (this) {
            c = coordinator;
        }
        if (c != null) {
            c.onChannelCheckpointStopped(checkpointId, channelInfo);
        }
    }

    @Override
    @GuardedBy("this")
    public void setCoordinator(RecoveredBufferStoreCoordinator coordinator) {
        assert Thread.holdsLock(this);
        this.coordinator = coordinator;
    }

    @Override
    @GuardedBy("this")
    public void setDataAvailableListener(DataAvailableListener listener) {
        assert Thread.holdsLock(this);
        assert Thread.holdsLock(gateLock);
        this.dataAvailableListener = listener;
    }

    public void addBuffer(Buffer buffer) {
        assert Thread.holdsLock(gateLock);
        DataAvailableListener listenerToFire;
        synchronized (this) {
            if (released) {
                buffer.recycleBuffer();
                return;
            }
            boolean wasEmpty = readyBuffers.isEmpty();
            readyBuffers.add(buffer);
            if (pendingCount > 0) {
                pendingCount--;
                if (pendingCount == 0 && !deferredBuffers.isEmpty()) {
                    while (!deferredBuffers.isEmpty()) {
                        readyBuffers.add(deferredBuffers.pollFirst());
                    }
                }
            }
            listenerToFire = wasEmpty ? dataAvailableListener : null;
        }
        if (listenerToFire != null) {
            listenerToFire.onDataAvailable();
        }
    }

    /** Counterpart of {@link #addBuffer}: increments the pending-on-disk counter for a spill. */
    @GuardedBy("this")
    public void incrementPending() {
        assert Thread.holdsLock(this);
        pendingCount++;
    }

    /**
     * Adds a buffer that becomes consumer-visible only after all on-disk entries have been drained.
     * Used to publish {@code EndOfInputChannelStateEvent} so it always lands after the last
     * recovered data buffer — without routing the event through the spill path.
     */
    public void addBufferAfterDisk(Buffer buffer) {
        assert Thread.holdsLock(gateLock);
        DataAvailableListener listenerToFire;
        synchronized (this) {
            if (released) {
                buffer.recycleBuffer();
                return;
            }
            if (pendingCount == 0) {
                boolean wasEmpty = readyBuffers.isEmpty();
                readyBuffers.add(buffer);
                listenerToFire = wasEmpty ? dataAvailableListener : null;
            } else {
                deferredBuffers.add(buffer);
                listenerToFire = null;
            }
        }
        if (listenerToFire != null) {
            listenerToFire.onDataAvailable();
        }
    }
}
