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
import org.apache.flink.runtime.io.network.partition.consumer.RecoveredBufferStoreImpl;
import org.apache.flink.util.CloseableIterator;
import org.apache.flink.util.Preconditions;

import javax.annotation.concurrent.GuardedBy;

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
 * <h3>Lock ordering</h3>
 *
 * <p>Two locks meet on the recovery → checkpoint hand-off: each per-channel store's intrinsic
 * monitor (<i>SMALL</i>) and this dispatcher's {@link #dispatcherLock} (<i>BIG</i>). They must
 * always be acquired in the order <b>SMALL → BIG</b>. Code holding BIG must never reach back to any
 * SMALL — neither directly nor through a callee — otherwise the AB-BA cycle returns. Forbidden
 * callees from inside a {@code synchronized(dispatcherLock)} block: {@link
 * RecoveredBufferStoreImpl#addBuffer}, {@link RecoveredBufferStoreImpl#addBufferAfterDisk}, {@link
 * RecoveredBufferStoreImpl#incrementPending}, any {@code synchronized(store)} block, and any {@link
 * org.apache.flink.runtime.io.network.partition.consumer.ChannelStatePersister} entrypoint.
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

    /**
     * Explicit lock object instead of {@code synchronized} methods so every callsite that takes BIG
     * is grep-visible.
     */
    private final Object dispatcherLock = new Object();

    private final byte[] cache;
    private int cachePosition;
    private InputChannelInfo cacheChannel;

    /**
     * Lazily initialized by the recovery thread inside {@link #writeToSpillFile}; volatile so task
     * threads observing it after {@link #flushed} see a fully-constructed instance.
     */
    private volatile FilteredSpillFile spillFile;

    @GuardedBy("dispatcherLock")
    private long currentCheckpointId = -1L;

    @GuardedBy("dispatcherLock")
    private long lastStoppedCheckpointId = -1L;

    @GuardedBy("dispatcherLock")
    private Set<InputChannelInfo> waitSet;

    /**
     * Phase-2 snapshot Readers pinned at the first {@link #onChannelCheckpointStarted} for the
     * in-flight checkpoint. {@code null} when no checkpoint is in progress.
     */
    @GuardedBy("dispatcherLock")
    private List<FilteredSpillFile.Reader> checkpointSnapshots;

    /**
     * Per-channel drain-head captured atomically with each channel's Step 1 ready snapshot. Phase-2
     * skips entries strictly below {@code startPos[X]} for channel X (already covered by Step 1)
     * and writes entries at or after as that channel's checkpoint state.
     */
    @GuardedBy("dispatcherLock")
    private Map<InputChannelInfo, EntryPosition> checkpointStartPos;

    /**
     * Position of the next spill entry the drain bundle will pop from the global FIFO. Volatile
     * publication provides cross-channel visibility: any other channel's Step 1 read under its own
     * SMALL observes drain progress without acquiring BIG.
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
                FilteredSpillFile.Reader.Entry entry = reader.peekNextEntry();
                if (entry == null) {
                    break;
                }
                InputChannelInfo ch = entry.getChannelInfo();
                long entryOffset = entry.getOffset();
                int entryLength = entry.getLength();

                // Buffer allocation may block on the pool and disk reads may miss the page
                // cache; keep both outside the store lock so channel checkpoints are not
                // serialised behind them.
                Buffer buffer = bufferRequester.requestBufferBlocking(ch);
                byte[] data = new byte[entryLength];
                reader.readBytesAt(entryOffset, entryLength, data, 0);

                RecoveredBufferStoreImpl store =
                        Preconditions.checkNotNull(
                                storesByChannel.get(ch), "No store for channel %s", ch);
                synchronized (store.getGateLock()) {
                    reader.skipNextEntry();
                    writeChunkToBuffer(buffer, data, entryLength);
                    store.addBuffer(buffer);
                    // drainHead's volatile write happens-after addBuffer's store-lock release —
                    // preserves "drainHead crossed e ⇒ e is in store_C.readyBuffers" for
                    // cross-channel readers.
                    drainHead = computeDrainHeadFrom(i);
                }
            }
        }
    }

    @Override
    public void close() throws IOException {
        if (closed) {
            return;
        }

        // Phase 1 (abort path only): caller skipped flush(). flushCache reaches store.addBuffer,
        // which acquires SMALL — must run outside dispatcherLock to keep the lock-order rule.
        // The happy path enters with flushed=true and skips this block.
        if (!flushed) {
            flushCache();
            if (spillFile != null) {
                spillFile.finish();
            }
            flushed = true;
        }

        // Setting closed=true before deleting the spill file is what closes the
        // close-vs-snapshot race: a concurrent onChannelCheckpointStarted either pinned its
        // FileChannels before this block ran (POSIX keeps the file alive after unlink) or
        // observes closed and returns before opening the file.
        synchronized (dispatcherLock) {
            closed = true;
            if (spillFile != null) {
                spillFile.close();
                spillFile = null;
            }
            resetCheckpointState();
        }

        bufferRequester.releaseExclusiveBuffers();
    }

    @Override
    public EntryPosition getCurrentDrainHead() {
        EntryPosition head = drainHead;
        return head == null ? EntryPosition.END : head;
    }

    /**
     * On the first callback for a checkpoint id, pins an immutable phase-2 view of every frozen
     * Reader and seeds the wait-set with their pending channels. Subsequent callbacks remove their
     * channel; the empty wait-set triggers {@link #drainSpillEntriesToCheckpoint}.
     *
     * <p>{@code startPos} is the per-channel drain-head captured atomically with the ready-buffer
     * snapshot; phase-2 uses it to skip entries already covered by that channel's Step 1.
     */
    @Override
    public void onChannelCheckpointStarted(
            long checkpointId, InputChannelInfo channelInfo, EntryPosition startPos) {
        synchronized (dispatcherLock) {
            if (closed) {
                return;
            }
            if (checkpointId < currentCheckpointId) {
                return;
            }
            if (checkpointId <= lastStoppedCheckpointId) {
                // ChannelStateWriter for this id is gone; phase-2 drain into it would rely on
                // writer.isDone() to silently swallow the data.
                return;
            }
            if (checkpointId > currentCheckpointId) {
                currentCheckpointId = checkpointId;
                checkpointStartPos = new HashMap<>();
                checkpointSnapshots = new ArrayList<>();
                waitSet = new HashSet<>();
                if (spillFile != null) {
                    pinSpillSnapshots();
                }
            }
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
    }

    /**
     * Drops the wait-set tied to a finished/aborted checkpoint and bumps {@code
     * lastStoppedCheckpointId} so a late {@link #onChannelCheckpointStarted} for the same id is
     * short-circuited as stale. Closes any pinned phase-2 snapshot Readers — otherwise every
     * aborted checkpoint leaks one fd per spill file.
     */
    @Override
    public void onChannelCheckpointStopped(long checkpointId, InputChannelInfo channelInfo) {
        synchronized (dispatcherLock) {
            if (closed) {
                return;
            }
            if (checkpointId > lastStoppedCheckpointId) {
                lastStoppedCheckpointId = checkpointId;
            }
            if (currentCheckpointId == checkpointId) {
                resetCheckpointState();
            }
        }
    }

    /**
     * Drops every pending spill entry belonging to {@code channelInfo} from all live Readers.
     * Phase-2 snapshots are intentionally not mutated: the filtering iterator drops snapshot
     * entries whose channel has no recorded startPos. Mutating the live snapshot would race the
     * executor thread already iterating it.
     */
    @Override
    public void onChannelReleased(InputChannelInfo channelInfo) {
        synchronized (dispatcherLock) {
            if (closed) {
                return;
            }
            if (spillFile != null) {
                // reader.removeEntriesForChannel mutates the same Reader.entries deque that
                // drainPendingSpill (holds SMALL_C, not BIG) pops from, so the deque must be
                // a ConcurrentLinkedDeque — that is its load-bearing role, not a decoration.
                for (FilteredSpillFile.Reader reader : spillFile.getReaders()) {
                    reader.removeEntriesForChannel(channelInfo);
                }
            }
            if (waitSet != null && waitSet.remove(channelInfo) && waitSet.isEmpty()) {
                drainSpillEntriesToCheckpoint(currentCheckpointId);
            }
        }
    }

    @GuardedBy("dispatcherLock")
    private void pinSpillSnapshots() {
        List<FilteredSpillFile.Reader> snapshots = new ArrayList<>();
        try {
            for (FilteredSpillFile.Reader reader : spillFile.getReaders()) {
                Preconditions.checkState(
                        reader.isFrozen(),
                        "Reader must be frozen when checkpoint starts; writer.finish() "
                                + "must be called before checkpoint trigger.");
                snapshots.add(reader.snapshot());
            }
        } catch (IOException e) {
            for (FilteredSpillFile.Reader snap : snapshots) {
                closeQuietly(snap);
            }
            throw new RuntimeException("Failed to snapshot spill readers for checkpoint", e);
        }
        checkpointSnapshots = snapshots;
        for (FilteredSpillFile.Reader snap : snapshots) {
            waitSet.addAll(snap.getPendingChannels());
        }
    }

    /**
     * Hands pinned snapshot Readers off to the {@link ChannelStateWriter}. Ownership transfers to
     * the iterator's {@link FilteringDrainChunkIterator#close()} so the FileChannels are released
     * even if the writer never advances the iterator (e.g. on abort).
     */
    @GuardedBy("dispatcherLock")
    private void drainSpillEntriesToCheckpoint(long checkpointId) {
        if (checkpointSnapshots == null || checkpointSnapshots.isEmpty()) {
            resetCheckpointState();
            return;
        }
        List<FilteredSpillFile.Reader> snapshots = checkpointSnapshots;
        Map<InputChannelInfo, EntryPosition> startPos = checkpointStartPos;
        checkpointSnapshots = null;
        checkpointStartPos = null;
        waitSet = null;
        // addInputDataFromSpill submits to an async writer thread and does not reach back into
        // any store SMALL — required for any callee invoked while holding BIG.
        channelStateWriter.addInputDataFromSpill(
                checkpointId, new FilteringDrainChunkIterator(snapshots, startPos));
    }

    @GuardedBy("dispatcherLock")
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
                synchronized (store.getGateLock()) {
                    store.addBuffer(buffer);
                }
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
                synchronized (store.getGateLock()) {
                    store.addBuffer(buffer);
                }
            }
        }
    }

    private boolean isSpillIdle() {
        return spillFile == null || spillFile.isIdle();
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
                // No startPos means the channel was released before checkpoint trigger — drop
                // every snapshot entry for it. Below-cutoff entries are already covered by
                // Step 1 in store_C.readyBuffers.
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
