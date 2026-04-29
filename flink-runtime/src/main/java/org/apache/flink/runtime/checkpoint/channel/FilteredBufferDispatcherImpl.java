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
import org.apache.flink.runtime.io.network.buffer.Buffer;
import org.apache.flink.runtime.io.network.partition.consumer.RecoveredBufferStore;
import org.apache.flink.runtime.io.network.partition.consumer.RecoveredBufferStoreImpl;
import org.apache.flink.util.CloseableIterator;
import org.apache.flink.util.Preconditions;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Deque;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.NoSuchElementException;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;

import static org.apache.flink.util.IOUtils.closeQuietly;

/**
 * {@link FilteredBufferDispatcher} implementation managing three data paths:
 *
 * <ul>
 *   <li><b>P1</b>: write directly to a network buffer and deliver to the target store
 *   <li><b>P2</b>: when no buffer is available, write to a spill file
 *   <li><b>P3</b>: when buffers become available later, eagerly replay spilled entries
 * </ul>
 *
 * <p>A byte[] cache accumulates payload bytes for the active channel. On channel change or cache
 * full, {@link #flushCache()} commits the bytes via P1 if the spill writer is idle and a buffer is
 * available, otherwise via P2. After {@link #flush()} seals all Readers, {@link
 * #drainPendingSpill()} drains the remainder; {@link #close()} releases resources.
 *
 * <h3>Locking discipline (post-iter_6)</h3>
 *
 * <p>This class holds <b>no monitor of its own</b>. The previous {@code synchronized} methods
 * have been replaced with lock-free primitives so the per-channel store lock (held by task
 * threads inside {@code ChannelStatePersister}) can call back into the coordinator without
 * forming an AB-BA cycle.
 *
 * <ul>
 *   <li>Recovery-thread state ({@code cache}, {@code cachePosition}, {@code cacheChannel},
 *       {@code spillFile}, {@code flushed}, {@code closed}): single-writer; cross-thread reads
 *       use {@code volatile}.
 *   <li>Per-checkpoint state ({@link CheckpointWindow}) is bundled into one immutable object
 *       and published via {@link AtomicReference}. Channel-arrival uses {@link
 *       ConcurrentHashMap}-backed sets and an {@link AtomicInteger} counter; the channel that
 *       drives the counter to zero fires {@link #drainSpillEntriesToCheckpoint(CheckpointWindow)}
 *       at most once via an {@link AtomicBoolean} guard.
 *   <li>{@link AtomicLong} {@code lastStoppedCheckpointId} advances monotonically via CAS.
 *   <li>{@code drainHead} stays {@code volatile} as before; cross-channel Step-1 readers use
 *       its publication to observe drain progress without taking any dispatcher-level lock.
 * </ul>
 */
@Internal
public class FilteredBufferDispatcherImpl
        implements FilteredBufferDispatcher, RecoveredBufferStoreCoordinator {

    /**
     * Typed as {@link RecoveredBufferStoreImpl} (not the interface) because the producer-side
     * mutators ({@code addBuffer}, {@code incrementPending}) are deliberately not on the public
     * interface — only this dispatcher calls them.
     */
    private final Map<InputChannelInfo, RecoveredBufferStoreImpl> storesByChannel;

    private final ChannelStateWriter channelStateWriter;
    private final String[] spillDirs;
    private final int memorySegmentSize;
    private final BufferRequester bufferRequester;

    /** Recovery-thread private. */
    private final byte[] cache;

    private int cachePosition;
    private InputChannelInfo cacheChannel;

    /**
     * Lazily initialized by recovery thread inside {@link #writeToSpillFile}. Volatile so task
     * threads observing it after {@link #flushed} see a fully-constructed instance.
     */
    private volatile FilteredSpillFile spillFile;

    /**
     * Highest stopped checkpoint id seen so far. CAS-advanced from
     * {@link #onChannelCheckpointStopped}; read by {@link #onChannelCheckpointStarted} to short-
     * circuit late callbacks for already-finalized checkpoints.
     */
    private final AtomicLong lastStoppedCheckpointId = new AtomicLong(-1L);

    /**
     * Per-checkpoint window installed by the first {@link #onChannelCheckpointStarted} for a new
     * id. The reference is replaced (or cleared) via CAS so the bundled snapshots/wait-set/start-
     * pos all become visible together with no torn intermediate state.
     */
    private final AtomicReference<CheckpointWindow> currentWindow = new AtomicReference<>();

    /**
     * Position of the next spill entry the drain bundle will pop from the global FIFO. Volatile
     * publication provides cross-channel visibility: any other channel's Step 1 read under its own
     * store lock observes drain progress without needing a dispatcher monitor.
     */
    private volatile EntryPosition drainHead;

    private volatile boolean flushed;
    private volatile boolean closed;

    public FilteredBufferDispatcherImpl(
            Map<InputChannelInfo, RecoveredBufferStoreImpl> storesByChannel,
            ChannelStateWriter channelStateWriter,
            String[] spillDirs,
            int memorySegmentSize,
            BufferRequester bufferRequester)
            throws IOException {
        if (spillDirs.length == 0) {
            throw new IOException("Spill directories must not be empty");
        }
        this.storesByChannel = storesByChannel;
        this.channelStateWriter = channelStateWriter;
        this.spillDirs = spillDirs;
        this.memorySegmentSize = memorySegmentSize;
        this.bufferRequester = bufferRequester;
        this.cache = new byte[memorySegmentSize];
        this.cachePosition = 0;

        for (RecoveredBufferStoreImpl store : storesByChannel.values()) {
            synchronized (store) {
                store.setCoordinator(this);
            }
        }
    }

    @Override
    public void write(byte[] data, int length, InputChannelInfo channelInfo)
            throws IOException, InterruptedException {
        if (flushed || closed) {
            throw new IllegalStateException("Cannot write after " + (closed ? "close" : "flush"));
        }

        eagerDrain();

        if (cacheChannel != null && !cacheChannel.equals(channelInfo) && cachePosition > 0) {
            flushCache();
        }
        cacheChannel = channelInfo;

        int pos = 0;
        while (pos < length) {
            int space = memorySegmentSize - cachePosition;
            int toCopy = Math.min(space, length - pos);
            System.arraycopy(data, pos, cache, cachePosition, toCopy);
            cachePosition += toCopy;
            pos += toCopy;

            if (cachePosition == memorySegmentSize) {
                flushCache();
                cacheChannel = channelInfo;
            }
        }
    }

    @Override
    public void flush() throws IOException {
        if (flushed || closed) {
            return;
        }
        flushCache();
        if (spillFile != null) {
            spillFile.finish();
            // Initial value Step 1 of any channel will observe before the first drainPendingSpill
            // bundle commits — never publish an unset drainHead during the live checkpoint window.
            drainHead = computeDrainHeadFrom(0);
        }
        flushed = true;
    }

    @Override
    public void drainPendingSpill() throws IOException, InterruptedException {
        Preconditions.checkState(flushed, "drainPendingSpill requires flush() to be called first");
        if (closed) {
            return;
        }
        if (spillFile == null) {
            return;
        }
        List<FilteredSpillFile.Reader> readers = spillFile.getReaders();
        for (int i = 0; i < readers.size(); i++) {
            FilteredSpillFile.Reader reader = readers.get(i);
            while (true) {
                // Lock-free peek: the original Reader is mutated only by this Recovery thread.
                FilteredSpillFile.Reader.Entry entry = reader.peekNextEntry();
                if (entry == null) {
                    break;
                }
                InputChannelInfo ch = entry.getChannelInfo();
                long entryOffset = entry.getOffset();
                int entryLength = entry.getLength();

                // Lock-free I/O: buffer allocation may block on the pool and disk reads may miss
                // the page cache; keep both outside any lock so channel checkpoints are not
                // serialised behind them.
                Buffer buffer = bufferRequester.requestBufferBlocking(ch);
                byte[] data = new byte[entryLength];
                reader.readBytesAt(entryOffset, entryLength, data, 0);

                // Commit under store lock then fire listener outside. drainHead must be the last
                // write so its volatile publication signals "drainHead crossed e ⇒ e is in
                // store_C.readyBuffers" to cross-channel Step 1 readers. Listener is captured
                // inside the lock and fired after to avoid AB-BA with the task thread (gate →
                // store).
                RecoveredBufferStoreImpl store =
                        Preconditions.checkNotNull(
                                storesByChannel.get(ch), "No store for channel %s", ch);
                RecoveredBufferStore.DataAvailableListener listener;
                synchronized (store) {
                    reader.skipNextEntry();
                    writeChunkToBuffer(buffer, data, entryLength);
                    listener = store.addBufferAndCaptureListener(buffer);
                    drainHead = computeDrainHeadFrom(i);
                }
                if (listener != null) {
                    listener.onDataAvailable();
                }
            }
        }
    }

    @Override
    public void close() throws IOException {
        if (closed) {
            return;
        }
        closed = true;
        // Defensive: caller should have called flush() before close(); seal lingering data so
        // spillFile.close() cleans up consistently.
        if (!flushed) {
            flushCache();
            if (spillFile != null) {
                spillFile.finish();
            }
            flushed = true;
        }
        if (spillFile != null) {
            spillFile.close();
            spillFile = null;
        }
        // Symmetric tear-down: returns each channel's exclusive segments to the global pool now
        // that drain is no longer reading from them.
        bufferRequester.releaseExclusiveBuffers();
        // Drop any still-pinned phase-2 snapshots so their fds are released even if the writer
        // never advanced the iterator. The drainTriggered CAS keeps this from racing a final
        // in-flight maybeTriggerDrain — whichever side wins owns and closes the snapshots.
        CheckpointWindow w = currentWindow.getAndSet(null);
        if (w != null && w.drainTriggered.compareAndSet(false, true)) {
            closeSnapshots(w.snapshots);
        }
    }

    @Override
    public EntryPosition getCurrentDrainHead() {
        EntryPosition head = drainHead;
        return head == null ? EntryPosition.END : head;
    }

    /**
     * Called by each per-channel store when that channel's ready buffers have been snapshotted
     * into the {@link ChannelStateWriter}. The first arrival for a new checkpoint id installs an
     * immutable {@link CheckpointWindow} via CAS (pinning frozen Reader snapshots and seeding the
     * wait-set with the pending channels); subsequent arrivals only mark themselves done and the
     * channel that drives {@code remaining} to zero triggers
     * {@link #drainSpillEntriesToCheckpoint(CheckpointWindow)}.
     *
     * <p>{@code startPos} is the per-channel drain-head captured atomically with the ready-buffer
     * snapshot; phase-2 uses it to skip entries already covered by that channel's Step 1.
     */
    @Override
    public void onChannelCheckpointStarted(
            long checkpointId, InputChannelInfo channelInfo, EntryPosition startPos) {
        if (checkpointId <= lastStoppedCheckpointId.get()) {
            // ChannelStateWriter for this id is gone; phase-2 drain into it would rely on
            // writer.isDone() to silently swallow the data.
            return;
        }
        CheckpointWindow window = ensureCheckpointWindow(checkpointId);
        if (window == null) {
            return; // a newer checkpoint already advanced past us
        }
        window.startPos.put(channelInfo, startPos);
        if (window.waitSet.remove(channelInfo)) {
            maybeTriggerDrain(window);
        }
    }

    /**
     * Drops the wait-set tied to a finished/aborted checkpoint and bumps {@code
     * lastStoppedCheckpointId} so a late {@link #onChannelCheckpointStarted} for the same id is
     * short-circuited as stale. Closes any pinned phase-2 snapshot Readers — otherwise every
     * aborted checkpoint leaks one fd per spill file.
     */
    @Override
    public void onChannelCheckpointStopped(long checkpointId, InputChannelInfo channelInfo) {
        long prev;
        do {
            prev = lastStoppedCheckpointId.get();
            if (checkpointId <= prev) {
                break;
            }
        } while (!lastStoppedCheckpointId.compareAndSet(prev, checkpointId));

        CheckpointWindow w = currentWindow.get();
        if (w != null && w.checkpointId == checkpointId) {
            // Snapshot ownership is claimed via the drainTriggered flag: a concurrent
            // maybeTriggerDrain that has already CAS'd drainTriggered to true now owns the
            // snapshots and will hand them to the channel-state writer's iterator. Detach the
            // window unconditionally so a duplicate stop does not double-close, but only the
            // first claimant of drainTriggered closes the snapshots here.
            currentWindow.compareAndSet(w, null);
            if (w.drainTriggered.compareAndSet(false, true)) {
                closeSnapshots(w.snapshots);
            }
        }
    }

    /**
     * Drops every pending spill entry belonging to {@code channelInfo} from all Readers. Phase-2
     * snapshots are intentionally not mutated: the filtering iterator drops snapshot entries whose
     * channel has no recorded startPos. Mutating the live snapshot would race the executor thread
     * already iterating it.
     */
    @Override
    public void onChannelReleased(InputChannelInfo channelInfo) {
        FilteredSpillFile sf = spillFile;
        if (sf != null) {
            for (FilteredSpillFile.Reader reader : sf.getReaders()) {
                reader.removeEntriesForChannel(channelInfo);
            }
        }
        CheckpointWindow w = currentWindow.get();
        if (w != null && w.waitSet.remove(channelInfo)) {
            maybeTriggerDrain(w);
        }
    }

    /**
     * CAS-installs (or reuses) the {@link CheckpointWindow} for {@code checkpointId}. When this
     * call is the first to advance to a higher id, it pins phase-2 Reader snapshots and seeds the
     * wait-set; if it loses the CAS race against a concurrent caller, the speculative snapshots
     * it created are closed before retrying so we never leak fds.
     *
     * @return the published window for {@code checkpointId}, or {@code null} if a strictly newer
     *     checkpoint has already advanced past {@code checkpointId} and this caller is now stale.
     */
    private CheckpointWindow ensureCheckpointWindow(long checkpointId) {
        while (true) {
            // Re-check on each spin: a concurrent stop may have advanced lastStoppedCheckpointId
            // past us while we were preparing snapshots, in which case we must abort.
            if (checkpointId <= lastStoppedCheckpointId.get()) {
                return null;
            }
            CheckpointWindow current = currentWindow.get();
            if (current != null && current.checkpointId > checkpointId) {
                return null;
            }
            if (current != null && current.checkpointId == checkpointId) {
                return current;
            }
            // current is null or has a stale id — try to install a new window for checkpointId.
            CheckpointWindow candidate = createCheckpointWindow(checkpointId);
            if (currentWindow.compareAndSet(current, candidate)) {
                if (current != null && current.drainTriggered.compareAndSet(false, true)) {
                    closeSnapshots(current.snapshots);
                }
                return candidate;
            }
            // Lost the CAS race; close the speculative snapshots we just pinned and retry. We
            // own them exclusively at this point — they have not been published to any other
            // thread — so no drainTriggered claim is needed.
            closeSnapshots(candidate.snapshots);
        }
    }

    private CheckpointWindow createCheckpointWindow(long checkpointId) {
        FilteredSpillFile sf = spillFile;
        List<FilteredSpillFile.Reader> snapshots = new ArrayList<>();
        Set<InputChannelInfo> pendingChannels = new HashSet<>();
        if (sf != null) {
            try {
                for (FilteredSpillFile.Reader reader : sf.getReaders()) {
                    Preconditions.checkState(
                            reader.isFrozen(),
                            "Reader must be frozen when checkpoint starts; writer.finish() "
                                    + "must be called before checkpoint trigger.");
                    FilteredSpillFile.Reader snap = reader.snapshot();
                    snapshots.add(snap);
                    pendingChannels.addAll(snap.getPendingChannels());
                }
            } catch (IOException e) {
                closeSnapshots(snapshots);
                throw new RuntimeException(
                        "Failed to snapshot spill readers for checkpoint", e);
            }
        }
        return new CheckpointWindow(checkpointId, snapshots, pendingChannels);
    }

    /**
     * Hands pinned snapshot Readers off to the {@link ChannelStateWriter}. Ownership of the
     * snapshot Readers transfers to the iterator's {@link FilteringDrainChunkIterator#close()},
     * which releases the FileChannels even if the writer never advances the iterator (e.g. on
     * abort).
     *
     * <p>The {@link AtomicBoolean} guard inside {@link CheckpointWindow} ensures this fires at
     * most once even if {@code remove + decrementAndGet} races between
     * {@link #onChannelCheckpointStarted} and {@link #onChannelReleased}.
     */
    private void maybeTriggerDrain(CheckpointWindow window) {
        if (window.remaining.decrementAndGet() != 0) {
            return;
        }
        if (!window.drainTriggered.compareAndSet(false, true)) {
            return;
        }
        // Detach the window so a stop callback after drain doesn't double-close snapshots; the
        // iterator now owns them.
        currentWindow.compareAndSet(window, null);
        if (window.snapshots.isEmpty()) {
            return;
        }
        channelStateWriter.addInputDataFromSpill(
                window.checkpointId,
                new FilteringDrainChunkIterator(window.snapshots, window.startPos));
    }

    private static void closeSnapshots(List<FilteredSpillFile.Reader> snapshots) {
        for (FilteredSpillFile.Reader snap : snapshots) {
            closeQuietly(snap);
        }
    }

    /**
     * {@code fromListIndex} is a list cursor, distinct from {@link
     * FilteredSpillFile.Reader#getFileIndex()} which is the globally monotonic file-id.
     */
    private EntryPosition computeDrainHeadFrom(int fromListIndex) {
        if (spillFile == null) {
            return EntryPosition.END;
        }
        List<FilteredSpillFile.Reader> readers = spillFile.getReaders();
        for (int i = fromListIndex; i < readers.size(); i++) {
            FilteredSpillFile.Reader r = readers.get(i);
            FilteredSpillFile.Reader.Entry next = r.peekNextEntry();
            if (next != null) {
                return new EntryPosition(r.getFileIndex(), next.getOffset());
            }
        }
        return EntryPosition.END;
    }

    /**
     * Commits the cache via P1 (direct buffer) or P2 (spill). P1 requires the spill writer idle AND
     * a non-blocking buffer available; otherwise spill, which preserves FIFO ordering — once
     * anything has been spilled, all subsequent data must also spill.
     */
    private void flushCache() throws IOException {
        if (cachePosition == 0) {
            cacheChannel = null;
            return;
        }

        InputChannelInfo channelInfo = cacheChannel;
        int bytesToFlush = cachePosition;
        cachePosition = 0;
        cacheChannel = null;

        if (isSpillIdle()) {
            Buffer buffer = bufferRequester.requestBuffer(channelInfo);
            if (buffer != null) {
                writeChunkToBuffer(buffer, cache, bytesToFlush);
                RecoveredBufferStoreImpl store =
                        Preconditions.checkNotNull(
                                storesByChannel.get(channelInfo),
                                "No store for channel %s",
                                channelInfo);
                store.addBuffer(buffer);
                return;
            }
        }

        writeToSpillFile(cache, bytesToFlush, channelInfo);
    }

    private static void writeChunkToBuffer(Buffer buffer, byte[] data, int length) {
        Preconditions.checkState(
                buffer.getMaxCapacity() >= length,
                "Buffer capacity %s is smaller than chunk length %s",
                buffer.getMaxCapacity(),
                length);
        buffer.asByteBuf().writeBytes(data, 0, length);
    }

    private void writeToSpillFile(byte[] data, int length, InputChannelInfo channelInfo)
            throws IOException {
        if (spillFile == null) {
            spillFile = new FilteredSpillFile(spillDirs, memorySegmentSize);
        }
        spillFile.writeEntry(data, length, channelInfo);
        RecoveredBufferStoreImpl store =
                Preconditions.checkNotNull(
                        storesByChannel.get(channelInfo), "No store for channel %s", channelInfo);
        synchronized (store) {
            store.incrementPending();
        }
    }

    /**
     * Eagerly replays spill entries while non-blocking buffers are available. Runs only on the
     * {@link #write} path before {@link #flush}, so by construction it cannot race {@link
     * #onChannelCheckpointStarted}: physical channels (and thus checkpoint triggers) only exist
     * after recovery's {@code finishRecovery()}, which runs after flush. Does not maintain {@code
     * drainHead} — that field is initialised at flush time.
     */
    private void eagerDrain() throws IOException {
        if (spillFile == null) {
            return;
        }
        for (FilteredSpillFile.Reader reader : spillFile.getReaders()) {
            while (reader.hasEntries()) {
                InputChannelInfo ch = reader.peekNextChannel();
                Buffer buffer = bufferRequester.requestBuffer(ch);
                if (buffer == null) {
                    return;
                }
                FilteredSpillFile.Chunk chunk = reader.readNext();
                if (chunk == null) {
                    buffer.recycleBuffer();
                    return;
                }
                writeChunkToBuffer(buffer, chunk.getData(), chunk.getLength());
                RecoveredBufferStoreImpl store =
                        Preconditions.checkNotNull(
                                storesByChannel.get(ch), "No store for channel %s", ch);
                store.addBuffer(buffer);
            }
        }
    }

    private boolean isSpillIdle() {
        return spillFile == null || spillFile.isIdle();
    }

    /**
     * Immutable bundle of per-checkpoint coordination state. Replaces the old triple of nullable
     * fields ({@code checkpointSnapshots} / {@code waitSet} / {@code checkpointStartPos}) with a
     * single {@link AtomicReference}-published object so the snapshots and the wait-set become
     * visible together with no torn intermediate state.
     */
    private static final class CheckpointWindow {

        final long checkpointId;
        final List<FilteredSpillFile.Reader> snapshots;
        final Map<InputChannelInfo, EntryPosition> startPos = new ConcurrentHashMap<>();
        final Set<InputChannelInfo> waitSet;
        final AtomicInteger remaining;
        final AtomicBoolean drainTriggered = new AtomicBoolean(false);

        CheckpointWindow(
                long checkpointId,
                List<FilteredSpillFile.Reader> snapshots,
                Set<InputChannelInfo> pendingChannels) {
            this.checkpointId = checkpointId;
            this.snapshots = Collections.unmodifiableList(new ArrayList<>(snapshots));
            // ConcurrentHashMap-backed set: O(1) thread-safe remove + a plug-in source for the
            // remaining counter without holding a lock.
            this.waitSet = ConcurrentHashMap.newKeySet();
            this.waitSet.addAll(pendingChannels);
            // Counter mirrors waitSet size at construction so each successful remove drives one
            // decrement.
            this.remaining = new AtomicInteger(this.waitSet.size());
        }
    }

    /**
     * Iterates chunks from snapshot Readers, skipping entries below each channel's recorded {@code
     * startPos} cutoff (those are covered by Step 1). Each Reader is closed eagerly when exhausted;
     * {@link #close()} closes whatever Readers remain.
     */
    private static final class FilteringDrainChunkIterator
            implements CloseableIterator<FilteredSpillFile.Chunk> {

        private final Deque<FilteredSpillFile.Reader> remaining;
        private final Map<InputChannelInfo, EntryPosition> startPos;

        FilteringDrainChunkIterator(
                List<FilteredSpillFile.Reader> snapshots,
                Map<InputChannelInfo, EntryPosition> startPos) {
            this.remaining = new ArrayDeque<>(snapshots);
            this.startPos = startPos;
        }

        @Override
        public boolean hasNext() {
            advanceToIncluded();
            return !remaining.isEmpty();
        }

        @Override
        public FilteredSpillFile.Chunk next() {
            advanceToIncluded();
            if (remaining.isEmpty()) {
                throw new NoSuchElementException();
            }
            try {
                return remaining.peekFirst().readNext();
            } catch (IOException e) {
                throw new UncheckedIOException("Failed to read spill chunk", e);
            }
        }

        /**
         * Skips entries whose channel either has no recorded startPos (released before checkpoint
         * trigger — drop everything for that channel) or whose position is strictly below the
         * channel's startPos. Closes empty readers eagerly so FileChannel fds are released even if
         * the writer never finishes draining the iterator.
         */
        private void advanceToIncluded() {
            while (!remaining.isEmpty()) {
                FilteredSpillFile.Reader r = remaining.peekFirst();
                if (!r.hasEntries()) {
                    closeQuietly(remaining.pollFirst());
                    continue;
                }
                FilteredSpillFile.Reader.Entry e = r.peekNextEntry();
                EntryPosition cutoff = startPos.get(e.getChannelInfo());
                EntryPosition entryPos = new EntryPosition(r.getFileIndex(), e.getOffset());
                if (cutoff == null || entryPos.compareTo(cutoff) < 0) {
                    r.skipNextEntry();
                } else {
                    return;
                }
            }
        }

        @Override
        public void close() {
            while (!remaining.isEmpty()) {
                closeQuietly(remaining.pollFirst());
            }
        }
    }
}
