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

import org.apache.flink.runtime.checkpoint.CheckpointException;
import org.apache.flink.runtime.checkpoint.CheckpointFailureReason;
import org.apache.flink.runtime.checkpoint.channel.ChannelStateWriter;
import org.apache.flink.runtime.checkpoint.channel.InputChannelInfo;
import org.apache.flink.runtime.event.AbstractEvent;
import org.apache.flink.runtime.io.network.api.CheckpointBarrier;
import org.apache.flink.runtime.io.network.api.EventAnnouncement;
import org.apache.flink.runtime.io.network.api.serialization.EventSerializer;
import org.apache.flink.runtime.io.network.buffer.Buffer;
import org.apache.flink.util.CloseableIterator;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Nullable;
import javax.annotation.concurrent.GuardedBy;

import java.io.IOException;
import java.util.List;
import java.util.OptionalLong;

import static org.apache.flink.util.Preconditions.checkNotNull;
import static org.apache.flink.util.Preconditions.checkState;

/**
 * Helper class for persisting channel state via {@link ChannelStateWriter}.
 *
 * <h3>Locking contract</h3>
 *
 * <p>The bound {@link RecoveredBufferStore}'s intrinsic monitor IS the channel-private lock.
 * Every state-touching method on this class — {@link #startPersisting}, {@link #stopPersisting},
 * {@link #maybePersist}, {@link #checkForBarrier}, {@link #hasBarrierReceived} — requires the
 * caller to hold {@code synchronized(store)}. The methods enforce this with an internal {@code
 * assert Thread.holdsLock(store)} that fires under {@code -ea}.
 *
 * <p>This class deliberately holds <b>no lock of its own</b>. Concurrent access between the task
 * thread (start/stop, getNext) and the network thread (onBuffer) is serialized by the same outer
 * {@code synchronized(store)} on each call site, mirroring the master-branch wide-lock pattern.
 *
 * <p>The constraint that callers may safely cross {@link #startPersisting} (which can call back
 * into {@link RecoveredBufferStore#checkpoint}, firing the dispatcher coordinator) holds because
 * the dispatcher coordinator is itself lock-free — see
 * {@code FilteredBufferDispatcherImpl}'s class-level invariants.
 */
public final class ChannelStatePersister {
    private static final Logger LOG = LoggerFactory.getLogger(ChannelStatePersister.class);

    private final InputChannelInfo channelInfo;

    private enum CheckpointStatus {
        COMPLETED,
        BARRIER_PENDING,
        BARRIER_RECEIVED
    }

    @GuardedBy("store")
    private CheckpointStatus checkpointStatus = CheckpointStatus.COMPLETED;

    @GuardedBy("store")
    private long lastSeenBarrier = -1L;

    private final ChannelStateWriter channelStateWriter;

    /**
     * Per-channel store bound at construction. Its intrinsic monitor IS the channel-private lock
     * that callers must hold for every state-touching method on this class.
     */
    private final RecoveredBufferStore store;

    ChannelStatePersister(
            ChannelStateWriter channelStateWriter,
            InputChannelInfo channelInfo,
            RecoveredBufferStore store) {
        this.channelStateWriter = checkNotNull(channelStateWriter);
        this.channelInfo = checkNotNull(channelInfo);
        this.store = checkNotNull(store);
    }

    /**
     * Asserts the disk-network exclusivity invariant ({@code store.isEmpty() ||
     * knownBuffers.isEmpty()}), then either snapshots the recovered store or writes network
     * inflight buffers via {@link ChannelStateWriter#addInputData}.
     *
     * @param knownBuffers network inflight buffers (empty for LocalInputChannel)
     */
    @GuardedBy("store")
    protected void startPersisting(long barrierId, List<Buffer> knownBuffers)
            throws CheckpointException {
        assert Thread.holdsLock(store);
        logEvent("startPersisting", barrierId);
        if (checkpointStatus == CheckpointStatus.BARRIER_RECEIVED && lastSeenBarrier > barrierId) {
            throw new CheckpointException(
                    String.format(
                            "Barrier for newer checkpoint %d has already been received compared to the requested checkpoint %d",
                            lastSeenBarrier, barrierId),
                    CheckpointFailureReason
                            .CHECKPOINT_SUBSUMED); // currently, at most one active unaligned
        }
        if (lastSeenBarrier < barrierId) {
            // Regardless of the current checkpointStatus, if we are notified about a more recent
            // checkpoint then we have seen so far, always mark that this more recent barrier is
            // pending. BARRIER_RECEIVED status can happen if we have seen an older barrier, that
            // probably has not yet been processed by the task, but task is now notifying us that
            // checkpoint has started for even newer checkpoint. We should spill the knownBuffers
            // and mark that we are waiting for that newer barrier to arrive.
            checkpointStatus = CheckpointStatus.BARRIER_PENDING;
            lastSeenBarrier = barrierId;
        }

        final boolean storeEmpty = store.isEmpty();
        final int storeSize = store.size();
        checkState(
                storeEmpty || knownBuffers.isEmpty(),
                "Invariant violated: store has data (size=%s) AND knownBuffers non-empty (size=%s) at barrier %s. "
                        + "Requires UNALIGNED_RECOVER_OUTPUT_ON_DOWNSTREAM=true so upstream does not "
                        + "replay output state into receivedBuffers while the recovered store is still draining.",
                storeSize,
                knownBuffers.size(),
                barrierId);

        if (!storeEmpty) {
            try {
                store.checkpoint(channelStateWriter, barrierId);
            } catch (IOException e) {
                throw new CheckpointException(
                        "Failed to checkpoint recovered store",
                        CheckpointFailureReason.IO_EXCEPTION,
                        e);
            }
        } else if (!knownBuffers.isEmpty()) {
            channelStateWriter.addInputData(
                    barrierId,
                    channelInfo,
                    ChannelStateWriter.SEQUENCE_NUMBER_UNKNOWN,
                    CloseableIterator.fromList(knownBuffers, Buffer::recycleBuffer));
        }
    }

    /**
     * Marks {@code id} concluded on this channel and notifies the store so the coordinator drops
     * any wait-set still tied to it (otherwise an aborted checkpoint's wait-set could linger and a
     * later release/late callback would trigger a phase-2 drain into a concluded checkpoint).
     */
    @GuardedBy("store")
    protected void stopPersisting(long id) {
        assert Thread.holdsLock(store);
        logEvent("stopPersisting", id);
        if (id >= lastSeenBarrier) {
            checkpointStatus = CheckpointStatus.COMPLETED;
            lastSeenBarrier = id;
        }
        store.notifyCheckpointStopped(id);
    }

    @GuardedBy("store")
    protected void maybePersist(Buffer buffer) {
        assert Thread.holdsLock(store);
        if (checkpointStatus == CheckpointStatus.BARRIER_PENDING && buffer.isBuffer()) {
            channelStateWriter.addInputData(
                    lastSeenBarrier,
                    channelInfo,
                    ChannelStateWriter.SEQUENCE_NUMBER_UNKNOWN,
                    CloseableIterator.ofElement(buffer.retainBuffer(), Buffer::recycleBuffer));
        }
    }

    @GuardedBy("store")
    protected OptionalLong checkForBarrier(Buffer buffer) throws IOException {
        assert Thread.holdsLock(store);
        AbstractEvent event = parseEvent(buffer);
        if (event instanceof CheckpointBarrier) {
            long barrierId = ((CheckpointBarrier) event).getId();
            long expectedBarrierId =
                    checkpointStatus == CheckpointStatus.COMPLETED
                            ? lastSeenBarrier + 1
                            : lastSeenBarrier;
            if (barrierId >= expectedBarrierId) {
                logEvent("found barrier", barrierId);
                checkpointStatus = CheckpointStatus.BARRIER_RECEIVED;
                lastSeenBarrier = barrierId;
                return OptionalLong.of(lastSeenBarrier);
            } else {
                logEvent("ignoring barrier", barrierId);
            }
        }
        if (event instanceof EventAnnouncement) { // NOTE: only remote channels
            EventAnnouncement announcement = (EventAnnouncement) event;
            if (announcement.getAnnouncedEvent() instanceof CheckpointBarrier) {
                long barrierId = ((CheckpointBarrier) announcement.getAnnouncedEvent()).getId();
                logEvent("found announcement for barrier", barrierId);
                return OptionalLong.of(barrierId);
            }
        }
        return OptionalLong.empty();
    }

    private void logEvent(String event, long barrierId) {
        if (LOG.isDebugEnabled()) {
            LOG.debug(
                    "{} {}, lastSeenBarrier = {} ({}) @ {}",
                    event,
                    barrierId,
                    lastSeenBarrier,
                    checkpointStatus,
                    channelInfo);
        }
    }

    /**
     * Parses the buffer as an event and returns the {@link CheckpointBarrier} if the event is
     * indeed a barrier or returns null in all other cases.
     */
    @Nullable
    protected AbstractEvent parseEvent(Buffer buffer) throws IOException {
        if (buffer.isBuffer()) {
            return null;
        } else {
            AbstractEvent event = EventSerializer.fromBuffer(buffer, getClass().getClassLoader());
            // reset the buffer because it would be deserialized again in SingleInputGate while
            // getting next buffer.
            // we can further improve to avoid double deserialization in the future.
            buffer.setReaderIndex(0);
            return event;
        }
    }

    @GuardedBy("store")
    protected boolean hasBarrierReceived() {
        assert Thread.holdsLock(store);
        return checkpointStatus == CheckpointStatus.BARRIER_RECEIVED;
    }

    @Override
    public String toString() {
        // Best-effort debug snapshot. Reads are unsynchronized on purpose: the logger must not
        // deadlock against callers that already hold (or are about to acquire) the store lock.
        return "ChannelStatePersister(lastSeenBarrier="
                + lastSeenBarrier
                + " ("
                + checkpointStatus
                + ")}";
    }
}
