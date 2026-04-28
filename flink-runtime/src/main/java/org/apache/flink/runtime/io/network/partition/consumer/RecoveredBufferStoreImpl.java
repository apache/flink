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
 * <p>The store's intrinsic monitor IS the channel-private lock. Methods marked
 * {@link GuardedBy @GuardedBy("this")} require the caller to hold {@code synchronized(store)};
 * each runs {@code assert Thread.holdsLock(this)}. {@link #size()} is the deliberate exception
 * (lock-free best-effort).
 *
 * <p>Two-phase methods ({@code *AndCaptureListener}, {@link #checkpoint}, {@link #releaseAll},
 * {@link #notifyCheckpointStopped}) self-manage their critical section and fire any captured
 * listener / coordinator callback after exiting the lock. This capture-then-fire-outside
 * protocol respects the lock order {@code gate.inputChannelsWithData → store}; firing inside
 * the store lock would form an AB-BA cycle with the consumer side.
 *
 * <p>Consumer methods run on the Task thread; producer-side mutators
 * ({@link #addBuffer}, {@link #addBufferAfterDisk}, {@link #incrementPending}) run on the
 * Recovery thread via FilteredBufferDispatcher.
 */
@Internal
public class RecoveredBufferStoreImpl implements RecoveredBufferStore {

    private final InputChannelInfo channelInfo;

    @GuardedBy("this")
    private final ArrayDeque<Buffer> readyBuffers = new ArrayDeque<>();

    @GuardedBy("this")
    private int pendingCount = 0;

    /**
     * Buffers visible to the consumer only after all on-disk entries have been drained. Used for
     * finish/control events whose contract is "everything before me has been delivered" (currently
     * {@link EndOfInputChannelStateEvent}). Atomically promoted into {@link #readyBuffers} when
     * {@link #pendingCount} reaches zero — a logical FIFO without routing events through the
     * spill path. Not counted in {@link #size()} / {@link #isEmpty()}: while something is
     * deferred, {@code pendingCount > 0} already keeps the store non-empty.
     */
    @GuardedBy("this")
    private final ArrayDeque<Buffer> deferredBuffers = new ArrayDeque<>();

    private volatile boolean released = false;

    @GuardedBy("this")
    private DataAvailableListener dataAvailableListener;

    @GuardedBy("this")
    private RecoveredBufferStoreCoordinator coordinator;

    public RecoveredBufferStoreImpl(InputChannelInfo channelInfo) {
        this.channelInfo = checkNotNull(channelInfo);
    }

    public InputChannelInfo getChannelInfo() {
        return channelInfo;
    }

    @Nullable
    @Override
    @GuardedBy("this")
    public Buffer tryTake() {
        assert Thread.holdsLock(this);
        return readyBuffers.poll();
    }

    @Override
    @GuardedBy("this")
    public Buffer.DataType peekNextDataType() {
        assert Thread.holdsLock(this);
        Buffer peeked = readyBuffers.peek();
        return peeked != null ? peeked.getDataType() : Buffer.DataType.NONE;
    }

    @Override
    @GuardedBy("this")
    public boolean isEmpty() {
        assert Thread.holdsLock(this);
        return readyBuffers.isEmpty() && pendingCount == 0;
    }

    /** Lock-free best-effort read; see class javadoc. */
    @Override
    public int size() {
        return readyBuffers.size() + pendingCount;
    }

    /**
     * Snapshots ready buffers to {@code writer}, then notifies the coordinator outside the store
     * lock. Pending spill entries on disk are checkpointed by the coordinator (which owns them),
     * triggered via {@link RecoveredBufferStoreCoordinator#onChannelCheckpointStarted}.
     */
    @Override
    public void checkpoint(ChannelStateWriter writer, long checkpointId) throws IOException {
        // Snapshot ready buffers AND read drainHead atomically under the store lock. The drain
        // bundle commits addBuffer + drainHead update under the same store lock, so any entry
        // added after this point has already advanced drainHead past its position; cross-channel
        // visibility is provided by the volatile drainHead read.
        RecoveredBufferStoreCoordinator c;
        EntryPosition startPos;
        synchronized (this) {
            c = coordinator;
            startPos = c != null ? c.getCurrentDrainHead() : EntryPosition.END;
            if (!readyBuffers.isEmpty()) {
                // Only data buffers belong in persisted in-flight state. Skip non-data entries
                // (notably EndOfInputChannelStateEvent which finishReadRecoveredState pushes
                // ahead of the task consuming it) — they would be rejected by
                // ChannelStateWriteRequest#checkBufferIsBuffer and kill the writer thread.
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
        this.dataAvailableListener = listener;
    }

    /**
     * Adds a recovered buffer to the ready queue. The listener fires <em>outside</em> the store
     * monitor (gate → store lock-order). Callers that already hold {@code synchronized(store)}
     * must use {@link #addBufferAndCaptureListener(Buffer)} and fire the listener after releasing
     * that outer lock to avoid AB-BA deadlock.
     */
    public void addBuffer(Buffer buffer) {
        DataAvailableListener listenerToFire = addBufferAndCaptureListener(buffer);
        if (listenerToFire != null) {
            listenerToFire.onDataAvailable();
        }
    }

    /**
     * Captures the data-available listener inside the store monitor and returns it. Caller must
     * fire it after releasing any outer lock ordered before the gate monitor.
     *
     * <p>When {@link #pendingCount} is non-zero, the buffer is necessarily a drained spill entry
     * (only producer path into a non-idle store goes through dispatcher drain), so the count is
     * decremented here; reaching zero promotes {@link #deferredBuffers} into {@link #readyBuffers}
     * in the same critical section.
     *
     * @return listener to fire, or {@code null} if no notification is needed
     */
    @Nullable
    public DataAvailableListener addBufferAndCaptureListener(Buffer buffer) {
        synchronized (this) {
            if (released) {
                buffer.recycleBuffer();
                return null;
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
            return wasEmpty ? dataAvailableListener : null;
        }
    }

    /**
     * Called when FilteredBufferDispatcher spills data; the matching decrement happens implicitly
     * inside {@link #addBufferAndCaptureListener} when the entry is drained back into a buffer.
     */
    @GuardedBy("this")
    public void incrementPending() {
        assert Thread.holdsLock(this);
        pendingCount++;
    }

    /**
     * Adds a buffer that becomes consumer-visible only after all on-disk entries have been
     * drained: delivered as a normal ready buffer when {@code pendingCount == 0}, otherwise held
     * in {@link #deferredBuffers} and promoted when the count reaches zero. Used by
     * {@code RecoveredInputChannel#finishReadRecoveredState} to publish
     * {@code EndOfInputChannelStateEvent} so it always lands after the last recovered data buffer
     * — without routing the event through the spill path.
     */
    public void addBufferAfterDisk(Buffer buffer) {
        DataAvailableListener listenerToFire = addBufferAfterDiskAndCaptureListener(buffer);
        if (listenerToFire != null) {
            listenerToFire.onDataAvailable();
        }
    }

    /** Capture-then-fire variant of {@link #addBufferAfterDisk(Buffer)}. */
    @Nullable
    public DataAvailableListener addBufferAfterDiskAndCaptureListener(Buffer buffer) {
        synchronized (this) {
            if (released) {
                buffer.recycleBuffer();
                return null;
            }
            if (pendingCount == 0) {
                boolean wasEmpty = readyBuffers.isEmpty();
                readyBuffers.add(buffer);
                return wasEmpty ? dataAvailableListener : null;
            } else {
                deferredBuffers.add(buffer);
                return null;
            }
        }
    }
}
