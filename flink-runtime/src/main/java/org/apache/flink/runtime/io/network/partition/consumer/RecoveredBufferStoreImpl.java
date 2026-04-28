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
 * Per-channel store for recovered buffers. Buffers are either ready (in-memory, available for
 * consumption) or pending (on disk, tracked by count only — the actual spill entries are owned by
 * FilteredBufferDispatcher).
 *
 * <h3>Locking contract</h3>
 *
 * <p>The store's intrinsic monitor ({@code this}) IS the channel-private lock. The store does NOT
 * synchronise its own methods; callers must hold {@code synchronized (store)} when invoking any
 * method marked {@link GuardedBy @GuardedBy("this")}. Each such method runs an
 * {@code assert Thread.holdsLock(this)} so violations surface immediately under {@code -ea}.
 *
 * <p>{@link #size()} is the deliberate exception: it is a lock-free best-effort read that
 * supports metric / gate-bookkeeping paths which tolerate a slightly stale value. It MUST NOT be
 * combined with {@link #isEmpty()} to derive a stronger invariant unless the caller holds the
 * store lock around both reads itself.
 *
 * <p>Two-phase methods ({@link #addBufferAndCaptureListener}, {@link
 * #addBufferAfterDiskAndCaptureListener}, {@link #checkpoint}, {@link #releaseAll},
 * {@link #notifyCheckpointStopped}) self-manage their own {@code synchronized (this)} block: they
 * commit state inside the block and then fire any captured listener / coordinator callback after
 * the block has been exited (capture-then-fire-outside).
 *
 * <h3>Lock order</h3>
 *
 * <p>Gate's {@code inputChannelsWithData} → channel store. The capture-then-fire-outside protocol
 * exists so the producer side can publish a buffer (taking the store lock) and only afterwards
 * traverse the gate-side notify path (which acquires the gate lock); without it the producer would
 * form an AB-BA deadlock with the consumer (gate → store).
 *
 * <h3>Thread roles</h3>
 *
 * <p>Public consumer methods are driven by the Task thread. Producer-side mutators ({@link
 * #addBuffer}, {@link #addBufferAfterDisk}, {@link #incrementPending}) are called from the
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
     * Buffers that must only become consumer-visible <em>after</em> all on-disk pending entries
     * have been drained. Used for finish/control events (currently only the per-channel
     * {@link org.apache.flink.runtime.io.network.partition.consumer.EndOfInputChannelStateEvent})
     * whose contract is "everything before me has been delivered". When {@link #pendingCount}
     * drops to zero inside {@link #addBufferAndCaptureListener}, the deferred buffers are
     * atomically promoted into {@link #readyBuffers} so the consumer sees them strictly after the
     * last drained spill entry — a logical FIFO that does not require routing events through the
     * spill path.
     *
     * <p>Not counted as part of the public {@link #size()} / {@link #isEmpty()} surface: while
     * something is deferred, {@code pendingCount > 0} already keeps the store non-empty; the
     * promotion to {@code readyBuffers} happens in the same critical section that drops
     * {@code pendingCount} to zero, so the store is never observed as "empty with deferred
     * buffers still hidden".
     */
    @GuardedBy("this")
    private final ArrayDeque<Buffer> deferredBuffers = new ArrayDeque<>();

    private volatile boolean released = false;

    @GuardedBy("this")
    private DataAvailableListener dataAvailableListener;

    @GuardedBy("this")
    private RecoveredBufferStoreCoordinator coordinator;

    /**
     * Creates a store bound to a single input channel. The bound {@link InputChannelInfo} is used
     * when persisting ready buffers during checkpoint and when notifying the checkpoint listener.
     */
    public RecoveredBufferStoreImpl(InputChannelInfo channelInfo) {
        this.channelInfo = checkNotNull(channelInfo);
    }

    /** Returns the input channel this store is bound to. */
    public InputChannelInfo getChannelInfo() {
        return channelInfo;
    }

    // ---------------------------------------------------------------------------
    // Public interface methods (Task thread)
    // ---------------------------------------------------------------------------

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

    /**
     * Lock-free best-effort read used by metric and gate-bookkeeping paths that can tolerate a
     * slightly stale value. Both reads ({@code readyBuffers.size()} and {@code pendingCount}) are
     * single-word and the worst observable inconsistency is "size off by 1 vs. the actual state",
     * which is exactly what every other {@code unsynchronizedGetNumberOfQueuedBuffers} caller
     * already accepts. Callers that need a consistent {@code isEmpty + size} pair must take the
     * store lock around both calls themselves.
     */
    @Override
    public int size() {
        return readyBuffers.size() + pendingCount;
    }

    /**
     * Checkpoints the ready buffers to the given ChannelStateWriter. Ready buffers are retained and
     * passed to the writer via CloseableIterator. After snapshotting, the registered {@link
     * RecoveredBufferStoreCoordinator} is notified <em>outside</em> the store lock so the
     * coordinator can safely acquire its own lock without risking a deadlock.
     *
     * <p>Pending spill entries on disk are checkpointed by the coordinator, which owns the spill
     * entries and file readers, triggered via
     * {@link RecoveredBufferStoreCoordinator#onChannelCheckpointStarted}.
     */
    @Override
    public void checkpoint(ChannelStateWriter writer, long checkpointId) throws IOException {
        // Step 1: snapshot ready buffers AND read the coordinator's current drain head atomically
        // under the store lock. Both reads must be a single consistent observation: the per-channel
        // phase-2 filter compares each spill entry's position against the captured drain head, so
        // any entry the drain bundle adds to readyBuffers after this point must also have advanced
        // the drain head past its position before this snapshot was taken (drain bundle commits
        // addBuffer + drainHead update atomically under the same store lock for the entry's
        // channel; cross-channel visibility is provided by the volatile drain head).
        RecoveredBufferStoreCoordinator c;
        EntryPosition startPos;
        synchronized (this) {
            c = coordinator;
            startPos = c != null ? c.getCurrentDrainHead() : EntryPosition.END;
            if (!readyBuffers.isEmpty()) {
                // Mirror RemoteInputChannel#getInflightBuffersUnsafe: only data buffers belong
                // in the persisted in-flight channel state. Non-data entries in readyBuffers
                // (notably the EndOfInputChannelStateEvent that finishReadRecoveredState pushes
                // and that the task may not yet have consumed when CDR transitions to RUNNING
                // before stateConsumedFuture completes) are control signals — passing them to
                // ChannelStateWriteRequest#checkBufferIsBuffer kills the writer worker thread
                // (IllegalArgumentException), which then surfaces on every subsequent enqueue
                // as "not running". Events stay in readyBuffers and are recycled later by the
                // task's normal tryTake path.
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

        // Step 2: notify the coordinator outside the store lock to avoid deadlock with the
        // coordinator's own synchronisation. The captured startPos is forwarded so the coordinator
        // can record the per-channel cutoff for phase-2 filtering.
        if (c != null) {
            c.onChannelCheckpointStarted(checkpointId, channelInfo, startPos);
        }
    }

    @Override
    public void releaseAll() {
        // Step 1: flip the released flag and recycle ready / deferred buffers under lock; capture
        // the coordinator reference for invocation outside the lock.
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

        // Step 2: notify the coordinator outside the store lock so the coordinator can safely
        // acquire its own lock to drop disk-resident spill entries for this channel.
        if (c != null) {
            c.onChannelReleased(channelInfo);
        }
    }

    @Override
    public void notifyCheckpointStopped(long checkpointId) {
        // Capture the coordinator reference under lock; fire the notification outside so the
        // coordinator can safely acquire its own synchronisation without risking deadlock.
        RecoveredBufferStoreCoordinator c;
        synchronized (this) {
            c = coordinator;
        }
        if (c != null) {
            c.onChannelCheckpointStopped(checkpointId, channelInfo);
        }
    }

    // ---------------------------------------------------------------------------
    // Setters (interface methods)
    // ---------------------------------------------------------------------------

    @Override
    @GuardedBy("this")
    public void setCoordinator(RecoveredBufferStoreCoordinator coordinator) {
        assert Thread.holdsLock(this);
        this.coordinator = coordinator;
    }

    /**
     * {@inheritDoc}
     *
     * <p>The notification listener fires when a buffer is added to a previously empty ready queue,
     * waking up the Task thread waiting for data.
     */
    @Override
    @GuardedBy("this")
    public void setDataAvailableListener(DataAvailableListener listener) {
        assert Thread.holdsLock(this);
        this.dataAvailableListener = listener;
    }

    // ---------------------------------------------------------------------------
    // Internal methods (Recovery thread, called by FilteredBufferDispatcher)
    // ---------------------------------------------------------------------------

    /**
     * Adds a recovered buffer to the ready queue. If the queue was previously empty, the
     * notification listener is invoked to wake up the Task thread.
     *
     * <p>The listener is invoked <em>outside</em> the store monitor: the listener path goes through
     * {@code SingleInputGate.queueChannel}, which acquires the gate's {@code inputChannelsWithData}
     * monitor. A task thread holding that monitor while peeking the store would otherwise form an
     * AB-BA deadlock with this method. Lock-order matches the two-phase pattern used by
     * {@link #checkpoint}, {@link #releaseAll} and {@link #notifyCheckpointStopped}.
     *
     * <p>Callers that wrap this method in their own {@code synchronized(store)} block must instead
     * use {@link #addBufferAndCaptureListener(Buffer)} and fire the returned listener after exiting
     * that outer block — otherwise the listener still runs while the outer lock is held and the
     * AB-BA risk reappears.
     */
    public void addBuffer(Buffer buffer) {
        DataAvailableListener listenerToFire = addBufferAndCaptureListener(buffer);
        if (listenerToFire != null) {
            listenerToFire.onDataAvailable();
        }
    }

    /**
     * Variant of {@link #addBuffer(Buffer)} that captures the data-available listener inside the
     * store monitor and returns it instead of firing it. Callers must invoke {@code
     * onDataAvailable()} on the returned listener (if non-null) <em>after</em> releasing any outer
     * lock that orders before the gate's {@code inputChannelsWithData} monitor — otherwise the
     * listener would run while that outer lock is held and re-introduce the AB-BA deadlock with
     * the task thread (gate lock → store lock).
     *
     * <p>If {@link #pendingCount} is non-zero when this method is called, the buffer being added
     * is necessarily a drained spill entry (the only producer path into a non-idle store goes
     * through the dispatcher's drain), so {@code pendingCount} is decremented here. When that
     * decrement reaches zero, any {@link #deferredBuffers} are promoted into {@link #readyBuffers}
     * in the same critical section.
     *
     * @return the listener to fire, or {@code null} if no notification is needed (queue was already
     *     non-empty, no listener registered, or the store has been released)
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
     * Increments the pending spill entry count. Called when FilteredBufferDispatcher spills data to
     * disk; the matching decrement happens implicitly inside {@link #addBufferAndCaptureListener}
     * when the spill entry is drained back into a buffer.
     */
    @GuardedBy("this")
    public void incrementPending() {
        assert Thread.holdsLock(this);
        pendingCount++;
    }

    /**
     * Adds a buffer that must only become consumer-visible after all on-disk entries have been
     * drained. If no spill entries are pending, the buffer is delivered immediately as a normal
     * ready buffer. Otherwise it is held in {@link #deferredBuffers} and atomically promoted by
     * {@link #addBufferAndCaptureListener(Buffer)} when the count reaches zero.
     *
     * <p>Used by {@code RecoveredInputChannel#finishReadRecoveredState} to publish the per-channel
     * {@code EndOfInputChannelStateEvent} so the event always lands strictly after the last
     * recovered data buffer for the channel — preserving the contract "all data has been read"
     * without needing the event to traverse the dispatcher's spill path.
     *
     * <p>The listener is invoked <em>outside</em> the store monitor for the same lock-order
     * reasons documented on {@link #addBuffer}.
     *
     * <p>Callers that wrap this method in their own {@code synchronized(store)} block must instead
     * use {@link #addBufferAfterDiskAndCaptureListener(Buffer)} and fire the returned listener
     * after exiting that outer block.
     */
    public void addBufferAfterDisk(Buffer buffer) {
        DataAvailableListener listenerToFire = addBufferAfterDiskAndCaptureListener(buffer);
        if (listenerToFire != null) {
            listenerToFire.onDataAvailable();
        }
    }

    /**
     * Variant of {@link #addBufferAfterDisk(Buffer)} that captures the data-available listener
     * inside the store monitor and returns it instead of firing it. Same lock-order constraints as
     * {@link #addBufferAndCaptureListener(Buffer)} apply to the caller.
     */
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
