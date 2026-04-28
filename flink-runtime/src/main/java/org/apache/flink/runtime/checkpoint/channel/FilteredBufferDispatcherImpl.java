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
import java.util.Deque;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.NoSuchElementException;
import java.util.Set;

import static org.apache.flink.util.IOUtils.closeQuietly;

/**
 * Implementation of {@link FilteredBufferDispatcher} that manages three data paths:
 *
 * <ul>
 *   <li><b>P1 (buffer)</b>: Data is written directly to a network buffer and delivered to the
 *       target store.
 *   <li><b>P2 (spill to disk)</b>: When no buffer is available, data is written to a spill file via
 *       {@link FilteredSpillFile#writeEntry}.
 *   <li><b>P3 (eager drain)</b>: When buffers become available later, spilled entries are eagerly
 *       replayed from disk into buffers and delivered to stores.
 * </ul>
 *
 * <p>A byte[] memory cache accumulates payload bytes for the active channel. On channel change or
 * cache full, {@link #flushCache()} is invoked: if the spill writer is idle and a buffer is
 * available the cached bytes go directly to a network buffer (P1); otherwise they are written to
 * the spill file (P2). After {@link #flush()} seals all Readers, {@link #drainPendingSpill()}
 * drains any remaining spill entries via {@link BufferRequester#requestBufferBlocking(InputChannelInfo)}.
 * {@link #close()} then releases spill file resources without performing any drain.
 */
@Internal
public class FilteredBufferDispatcherImpl
        implements FilteredBufferDispatcher, RecoveredBufferStoreCoordinator {

    /**
     * Per-channel stores used by this dispatcher. Typed as the concrete {@link
     * RecoveredBufferStoreImpl} rather than {@link
     * org.apache.flink.runtime.io.network.partition.consumer.RecoveredBufferStore} because the
     * producer-side methods (addBuffer, incrementPending) are intentionally not part of the public
     * interface — they are only called by FilteredBufferDispatcher, which is the sole producer of
     * buffers for the stores.
     */
    private final Map<InputChannelInfo, RecoveredBufferStoreImpl> storesByChannel;

    private final ChannelStateWriter channelStateWriter;
    private final String[] spillDirs;
    private final int memorySegmentSize;
    private final BufferRequester bufferRequester;

    // Memory cache state: accumulates bytes for the active channel before committing to P1 or P2
    private final byte[] cache;
    private int cachePosition;
    private InputChannelInfo cacheChannel;

    // Spill infrastructure (P2/P3 paths)
    private FilteredSpillFile spillFile;

    // Checkpoint wait-set state machine
    private long currentCheckpointId = -1L;
    private long lastStoppedCheckpointId = -1L;
    private Set<InputChannelInfo> waitSet;

    /**
     * Phase-2 snapshot Readers captured at the first {@link #onChannelCheckpointStarted} call for
     * the in-flight checkpoint. Held until the wait-set converges and the iterator is handed off
     * to the {@link ChannelStateWriter}, or until the checkpoint is stopped early. {@code null}
     * when no checkpoint is in progress.
     */
    private List<FilteredSpillFile.Reader> checkpointSnapshots;

    /**
     * Per-channel drain head captured atomically with each channel's Step 1 ready snapshot. Used
     * by phase-2 to filter snapshot entries: entries strictly before {@code startPos[X]} were
     * already in {@code store_X.readyBuffers} when X's barrier passed (and were therefore captured
     * by Step 1), so phase 2 must skip them; entries at or after {@code startPos[X]} were still on
     * disk at X's barrier and must be written to X's channel state. {@code null} when no
     * checkpoint is in progress.
     */
    private Map<InputChannelInfo, EntryPosition> checkpointStartPos;

    /**
     * Position of the next spill entry the drain bundle will pop from the global FIFO across all
     * sealed Readers. The drain bundle commits addBuffer (which folds in the matching pending
     * decrement) and then advances this field as its last action under {@code synchronized(store_X)};
     * the volatile semantics provide cross-channel visibility for {@code Step 1} of any other
     * channel reading the field under its own store lock. {@code null} until the first spill entry
     * is seen.
     */
    private volatile EntryPosition drainHead;

    // Lifecycle flags
    private boolean flushed;
    private boolean closed;

    /**
     * Creates a new FilteredBufferDispatcherImpl.
     *
     * @param storesByChannel per-channel stores for delivering recovered buffers
     * @param channelStateWriter writer used during phase2 to stream spill chunks to checkpoint
     *     storage without allocating network buffers
     * @param spillDirs directories for spill files
     * @param memorySegmentSize the size of a memory segment / network buffer
     * @param bufferRequester per-channel buffer source. The non-blocking variant is used for the
     *     fast path (P1) and eager replay (P3); the blocking variant is used by drainPendingSpill()
     * @throws IOException if spillDirs is empty
     */
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

        // Register this dispatcher as the coordinator on every per-channel store. The store calls
        // back into the coordinator from checkpoint() and releaseAll() so the dispatcher can
        // maintain its wait-set and drop still-pending on-disk spill entries the moment a channel
        // is released, instead of holding the disk resources until dispatcher close().
        for (RecoveredBufferStoreImpl store : storesByChannel.values()) {
            synchronized (store) {
                store.setCoordinator(this);
            }
        }
    }

    @Override
    public synchronized void write(byte[] data, int length, InputChannelInfo channelInfo)
            throws IOException, InterruptedException {
        if (flushed || closed) {
            throw new IllegalStateException("Cannot write after " + (closed ? "close" : "flush"));
        }

        // P3: eagerly replay any pending spill entries while non-blocking buffers are available
        eagerDrain();

        // Channel change: flush cached bytes for the previous channel before accumulating new data
        if (cacheChannel != null && !cacheChannel.equals(channelInfo) && cachePosition > 0) {
            flushCache();
        }
        cacheChannel = channelInfo;

        // Copy bytes into cache; flush whenever the cache fills up
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
    public synchronized void flush() throws IOException {
        if (flushed || closed) {
            return;
        }
        flushCache();
        if (spillFile != null) {
            spillFile.finish();
            // Initialise drainHead to the first remaining entry across all sealed Readers (or END
            // if eagerDrain consumed everything). This is the value Step 1 of any channel will
            // observe before the first drainPendingSpill bundle commits — so we never publish an
            // unset / null drainHead during the live checkpoint window.
            drainHead = computeDrainHeadFrom(0);
        }
        flushed = true;
    }

    @Override
    public void drainPendingSpill() throws IOException, InterruptedException {
        Preconditions.checkState(flushed, "drainPendingSpill requires flush() to be called first");
        if (closed) {
            return; // already cleaned up; nothing to drain
        }
        if (spillFile == null) {
            return;
        }
        List<FilteredSpillFile.Reader> readers = spillFile.getReaders();
        for (int i = 0; i < readers.size(); i++) {
            FilteredSpillFile.Reader reader = readers.get(i);
            while (true) {
                // Section 1 (lock-free peek): inspect the head entry of the current Reader to
                // decide which channel's buffer pool to pull from. The original Reader is mutated
                // only by this Recovery thread, so peek does not race with another drain consumer.
                FilteredSpillFile.Reader.Entry entry = reader.peekNextEntry();
                if (entry == null) {
                    break;
                }
                InputChannelInfo ch = entry.getChannelInfo();
                long entryOffset = entry.getOffset();
                int entryLength = entry.getLength();

                // Section 2 (lock-free I/O): allocate the network buffer (may block on the pool)
                // and copy the spilled bytes from disk into it. Both operations are kept outside
                // any lock to avoid serialising channel checkpoints behind buffer-pool waits or
                // page-cache misses.
                Buffer buffer = bufferRequester.requestBufferBlocking(ch);
                byte[] data = new byte[entryLength];
                reader.readBytesAt(entryOffset, entryLength, data, 0);

                // Section 3 (commit under store lock + fire listener outside): pop the entry from
                // the Reader, hand the populated buffer to the store (which folds in the matching
                // pending decrement), and advance drainHead. The pop-then-add ordering keeps
                // Step 1 from observing an entry popped from disk but not yet in readyBuffers.
                // drainHead must be the last write so its volatile publication signals "drainHead
                // crossed e ⇒ e is in store_C.readyBuffers" to cross-channel readers. The
                // data-available listener is captured inside the store lock and fired afterwards
                // to avoid an AB-BA deadlock with the task thread (gate lock → store lock).
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
    public synchronized void close() throws IOException {
        if (closed) {
            return;
        }
        closed = true;
        // Defensive: caller should have called flush() before close(); honour the historical
        // fallback to seal lingering data so spillFile.close() can clean up consistently.
        if (!flushed) {
            flushCache();
            if (spillFile != null) {
                spillFile.finish();
            }
            flushed = true;
        }
        // Resource release only. drainSpillThroughBuffers is the responsibility of
        // drainPendingSpill().
        if (spillFile != null) {
            spillFile.close();
            spillFile = null;
        }
        // Hand off the symmetric tear-down: the BufferRequester returns each RecoveredInputChannel's
        // exclusive segments to the global pool now that drain is no longer reading from them.
        // Doing this here (post-drain, post-flush) guarantees no spill entry was abandoned mid-load
        // and lets task threads still consuming buffers from the recovered store recycle them
        // straight to the global pool (BufferManager.recycle's released-channel shortcut).
        bufferRequester.releaseExclusiveBuffers();
    }

    @Override
    public EntryPosition getCurrentDrainHead() {
        EntryPosition head = drainHead;
        return head == null ? EntryPosition.END : head;
    }

    /**
     * Called by each per-channel store when that channel's ready buffers have been snapshotted into
     * the {@link ChannelStateWriter}.
     *
     * <p>On the first callback for a given checkpointId we capture an immutable phase-2 view of the
     * disk side: each sealed Reader has {@link FilteredSpillFile.Reader#snapshot()} called and the
     * results are pinned in {@code checkpointSnapshots}, so subsequent {@link #drainPendingSpill}
     * pops on the original Readers cannot drop entries out of the in-flight checkpoint. The
     * wait-set is populated from those snapshots' pending channels. Subsequent callbacks remove
     * their channel from the wait-set; when the set becomes empty, all channels with disk data
     * have reported in and {@link #drainSpillEntriesToCheckpoint} is triggered.
     *
     * <p>{@code startPos} is the per-channel drain-head value the calling store captured atomically
     * with its ready-buffer snapshot; phase 2 uses it to filter the captured snapshot entries
     * (skip entries below the channel's startPos — they were already in readyBuffers and are
     * covered by Step 1).
     *
     * <p>Called from the Task thread; synchronized on {@code this} to be mutually exclusive with
     * the Recovery thread's {@link #write} / {@link #flush} / {@link #close}.
     */
    public synchronized void onChannelCheckpointStarted(
            long checkpointId, InputChannelInfo channelInfo, EntryPosition startPos) {
        if (checkpointId < currentCheckpointId) {
            // Stale callback from a superseded checkpoint: we have already moved on to a newer
            // one. Ignoring it keeps the current wait-set intact so the in-flight checkpoint
            // converges correctly.
            return;
        }
        if (checkpointId <= lastStoppedCheckpointId) {
            // Stale callback for a checkpoint the task has already stopped (finished or aborted).
            // The ChannelStateWriter for this id is gone; phase-2 drain into it would be wasted
            // work and would require relying on writer.isDone() to silently swallow the data.
            return;
        }
        if (checkpointId > currentCheckpointId) {
            // New checkpoint: pin a snapshot of every sealed Reader before any further drain pops
            // can mutate their entry deques, then build the wait-set from the snapshot's pending
            // channels. Invariant: checkpoint only starts after recovery ends (writer.finish());
            // all Readers are sealed at this point.
            currentCheckpointId = checkpointId;
            checkpointStartPos = new HashMap<>();
            checkpointSnapshots = new ArrayList<>();
            waitSet = new HashSet<>();
            if (spillFile != null) {
                List<FilteredSpillFile.Reader> snapshots = new ArrayList<>();
                try {
                    for (FilteredSpillFile.Reader reader : spillFile.getReaders()) {
                        Preconditions.checkState(
                                reader.isSealed(),
                                "Reader must be sealed when checkpoint starts; writer.finish() "
                                        + "must be called before checkpoint trigger.");
                        snapshots.add(reader.snapshot());
                    }
                } catch (IOException e) {
                    for (FilteredSpillFile.Reader snap : snapshots) {
                        closeQuietly(snap);
                    }
                    throw new RuntimeException(
                            "Failed to snapshot spill readers for checkpoint", e);
                }
                checkpointSnapshots = snapshots;
                for (FilteredSpillFile.Reader snap : snapshots) {
                    waitSet.addAll(snap.getPendingChannels());
                }
            }
        }
        // checkpointId == currentCheckpointId: accumulate toward the same wait-set.
        if (checkpointStartPos != null) {
            checkpointStartPos.put(channelInfo, startPos);
        }
        if (waitSet != null) {
            waitSet.remove(channelInfo);
            if (waitSet.isEmpty()) {
                drainSpillEntriesToCheckpoint(checkpointId);
            }
        }
    }

    /**
     * Called by a per-channel store from {@link RecoveredBufferStore#notifyCheckpointStopped} when
     * the owning channel has finished or aborted the given checkpoint. Drops the wait-set tied to
     * that checkpoint so a later {@link #onChannelReleased} cannot drain spill entries into a
     * checkpoint the task has already concluded; also bumps {@code lastStoppedCheckpointId} so a
     * late {@link #onChannelCheckpointStarted} for the same id is short-circuited as stale.
     *
     * <p>Closes any pinned phase-2 snapshot Readers and clears {@code checkpointStartPos} /
     * {@code checkpointSnapshots} so the next checkpoint starts from a clean slate and the per-
     * snapshot {@code FileChannel}s are released promptly (otherwise every aborted checkpoint
     * leaks one fd per spill file).
     *
     * <p>Called from the Task thread; synchronized on {@code this} to be mutually exclusive with
     * other coordinator callbacks and the Recovery thread's
     * {@link #write} / {@link #flush} / {@link #close}.
     */
    public synchronized void onChannelCheckpointStopped(
            long checkpointId, InputChannelInfo channelInfo) {
        if (checkpointId > lastStoppedCheckpointId) {
            lastStoppedCheckpointId = checkpointId;
        }
        if (currentCheckpointId == checkpointId) {
            // The wait-set we were collecting belongs to the now-stopped checkpoint; releasing
            // any remaining channel must not retroactively trigger phase-2 drain into it.
            resetCheckpointState();
        }
    }

    /**
     * Called by a per-channel store from {@link RecoveredBufferStoreImpl#releaseAll()}. Drops every
     * pending spill entry belonging to {@code channelInfo} from all Readers so the disk-side
     * bookkeeping is freed immediately; also removes the channel from an in-flight checkpoint
     * wait-set so the wait-set can still converge after the channel goes away.
     *
     * <p>Phase-2 snapshots intentionally are <em>not</em> mutated here: an entry left in the
     * snapshot whose channel has been released will be dropped by the filtering iterator because
     * {@code checkpointStartPos.get(channelInfo)} returns null (treated as "channel gone, skip
     * everything"). Mutating the live snapshot would require coordinating with the executor thread
     * already iterating it for an in-flight phase-2 drain.
     *
     * <p>Called from the Task thread; synchronized on {@code this} to be mutually exclusive with
     * the Recovery thread's {@link #write} / {@link #flush} / {@link #close} and with {@link
     * #onChannelCheckpointStarted}.
     */
    public synchronized void onChannelReleased(InputChannelInfo channelInfo) {
        if (spillFile != null) {
            for (FilteredSpillFile.Reader reader : spillFile.getReaders()) {
                reader.removeEntriesForChannel(channelInfo);
            }
        }
        if (waitSet != null && waitSet.remove(channelInfo) && waitSet.isEmpty()) {
            drainSpillEntriesToCheckpoint(currentCheckpointId);
        }
    }

    /**
     * Hands the previously pinned snapshot Readers (with the captured per-channel startPos cutoffs)
     * off to the {@link ChannelStateWriter} via a filtering iterator. Ownership of the snapshot
     * Readers transfers to the iterator's {@link FilteringDrainChunkIterator#close()}.
     */
    private void drainSpillEntriesToCheckpoint(long checkpointId) {
        if (checkpointSnapshots == null || checkpointSnapshots.isEmpty()) {
            resetCheckpointState();
            return;
        }
        List<FilteredSpillFile.Reader> snapshots = checkpointSnapshots;
        Map<InputChannelInfo, EntryPosition> startPos = checkpointStartPos;
        // Hand the snapshot Readers to the iterator so its close() releases the FileChannels even
        // if the writer never advances the iterator (e.g. on abort). Clear local state so a later
        // onChannelCheckpointStopped for the same id does not double-close.
        checkpointSnapshots = null;
        checkpointStartPos = null;
        waitSet = null;
        channelStateWriter.addInputDataFromSpill(
                checkpointId, new FilteringDrainChunkIterator(snapshots, startPos));
    }

    private void resetCheckpointState() {
        if (checkpointSnapshots != null) {
            for (FilteredSpillFile.Reader snap : checkpointSnapshots) {
                closeQuietly(snap);
            }
            checkpointSnapshots = null;
        }
        checkpointStartPos = null;
        waitSet = null;
    }

    /**
     * Next-to-pop {@link EntryPosition}, scanning readers from list position {@code fromListIndex}.
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
     * Flushes the cache for the current channel via P1 (buffer) or P2 (spill). After calling, the
     * cache is empty and cacheChannel is null.
     *
     * <p>P1 is chosen when the spill writer is idle (no pending disk entries) AND a non-blocking
     * buffer can be obtained. Otherwise the bytes are spilled (P2). This ensures FIFO ordering:
     * once any data has been spilled, all subsequent data must also spill to preserve order.
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

        // P1: spill writer idle and a buffer is available — write directly to network buffer
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

        // P2: spill writer not idle or no buffer available — write to spill file
        writeToSpillFile(cache, bytesToFlush, channelInfo);
    }

    /**
     * Copies {@code length} bytes from {@code data} into the given network buffer. Assumes the
     * buffer is freshly acquired (writerIndex == 0); after this call, {@code buffer.getSize() ==
     * length}.
     */
    private static void writeChunkToBuffer(Buffer buffer, byte[] data, int length) {
        Preconditions.checkState(
                buffer.getMaxCapacity() >= length,
                "Buffer capacity %s is smaller than chunk length %s",
                buffer.getMaxCapacity(),
                length);
        buffer.asByteBuf().writeBytes(data, 0, length);
    }

    /** Writes bytes to the spill file, creating the Writer lazily if needed. */
    private void writeToSpillFile(byte[] data, int length, InputChannelInfo channelInfo)
            throws IOException {
        if (spillFile == null) {
            spillFile = new FilteredSpillFile(spillDirs, memorySegmentSize);
        }
        spillFile.writeEntry(data, length, channelInfo);
        // Increment pending count so store.isEmpty() correctly reflects outstanding data
        RecoveredBufferStoreImpl store =
                Preconditions.checkNotNull(
                        storesByChannel.get(channelInfo),
                        "No store for channel %s",
                        channelInfo);
        synchronized (store) {
            store.incrementPending();
        }
    }

    /**
     * Eagerly replays spill entries while non-blocking buffers are available.
     *
     * <p>Runs only on the {@link #write} path before {@link #flush}, so by construction it cannot
     * race with {@link #onChannelCheckpointStarted}: physical {@code InputChannel}s exist only
     * after recovery's {@code finishRecovery()} (which itself runs after flush), and checkpoint
     * triggers can only fire on those physical channels. {@code drainHead} is initialised at
     * {@link #flush} time from whatever entries this method left behind, so eagerDrain does not
     * need to maintain it.
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

    /** Returns true if no spill entries have been written yet (P1 is safe). */
    private boolean isSpillIdle() {
        return spillFile == null || spillFile.isIdle();
    }

    // -------------------------------------------------------------------------
    // FilteringDrainChunkIterator — CloseableIterator over snapshot Readers with per-channel cutoff
    // -------------------------------------------------------------------------

    /**
     * Iterates over chunks from a sequence of snapshot {@link FilteredSpillFile.Reader}s, skipping
     * entries that fall below the channel's recorded {@code startPos} cutoff (those entries were
     * already in the channel's Step 1 ready snapshot and are covered there). Each Reader is
     * drained in order; once exhausted it is popped and closed immediately. {@link #close()} closes
     * whatever Readers remain (i.e. those not yet consumed by the iterator).
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
         * Skips entries whose channel either has no recorded startPos (channel was released before
         * triggering checkpoint, drop everything for that channel) or whose position is strictly
         * below the channel's startPos (entry was already covered by Step 1). Pops empty readers
         * along the way and closes them eagerly so FileChannel fds are released even if the writer
         * never finishes draining the iterator.
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
