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
import org.apache.flink.util.FileUtils;

import java.io.Closeable;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.nio.file.StandardOpenOption;
import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Deque;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Set;
import java.util.UUID;

import static org.apache.flink.util.Preconditions.checkArgument;
import static org.apache.flink.util.Preconditions.checkState;

/**
 * Spill file for the {@code filterAndRewrite} recovery path. Appends raw bytes to one or more
 * physical files on disk; each logical entry is tracked in the corresponding {@link Reader}'s entry
 * deque. Readers support both replay ({@link Reader#readNext}) and checkpoint snapshot ({@link
 * Reader#snapshot}).
 *
 * <p>Rotates to a new file when the current one exceeds 64 MB; each rotation seals the outgoing
 * Reader and opens a new one. All Readers are sealed on {@link #finish()}. Files are created lazily
 * on the first {@link #writeEntry} call.
 */
@Internal
public class FilteredSpillFile implements Closeable {

    private static final long FILE_ROTATION_THRESHOLD = 64L * 1024 * 1024; // 64 MB

    private final String[] spillDirs;
    private final int memorySegmentSize;
    private int currentDirIndex;
    private FileChannel currentChannel;
    private long currentFileOffset;
    private final List<Reader> readers;
    /** Monotonic file-id counter; never reused even if a finished Reader is removed from the list. */
    private int nextFileIndex;
    private boolean finished;

    /**
     * Creates a new spill file (lazy: the first physical file is opened on the first {@link
     * #writeEntry} call). Arguments are assumed pre-validated by the caller (non-empty spillDirs,
     * positive memorySegmentSize).
     *
     * @param spillDirs directories for writing spill files
     * @param memorySegmentSize max bytes per spill entry. Each entry is 1:1 aligned with a network
     *     buffer of this size; longer payloads must be split upstream.
     */
    public FilteredSpillFile(String[] spillDirs, int memorySegmentSize) {
        this.spillDirs = spillDirs;
        this.memorySegmentSize = memorySegmentSize;
        this.currentDirIndex = 0;
        this.currentFileOffset = 0;
        this.readers = new ArrayList<>();
        this.nextFileIndex = 0;
        this.finished = false;
    }

    /**
     * Appends {@code len} bytes from {@code data[0..len)} to the current spill file, registering an
     * entry for {@code channelInfo} in the current Reader. Lazily opens the first file; rotates
     * when the current file exceeds {@link #FILE_ROTATION_THRESHOLD}.
     *
     * <p>Entries must fit within {@code memorySegmentSize} bytes (1:1 alignment with network
     * buffers on replay). Oversize entries fail fast via {@link IllegalArgumentException}.
     */
    public void writeEntry(byte[] data, int len, InputChannelInfo channelInfo) throws IOException {
        checkState(!finished, "writeEntry after finish");
        checkArgument(
                len <= memorySegmentSize,
                "Entry length %s exceeds memorySegmentSize %s",
                len,
                memorySegmentSize);
        if (currentChannel == null) {
            openNewFile();
        } else if (currentFileOffset > FILE_ROTATION_THRESHOLD) {
            rotateFile();
        }
        long entryOffset = currentFileOffset;
        FileUtils.writeCompletely(currentChannel, ByteBuffer.wrap(data, 0, len));
        currentFileOffset += len;
        currentReader().addEntry(channelInfo, entryOffset, len);
    }

    /** Seals the last Reader. After finish, no more writeEntry calls are accepted. */
    public void finish() {
        if (finished) {
            return;
        }
        finished = true;
        if (!readers.isEmpty()) {
            currentReader().seal();
        }
    }

    /**
     * Finishes (if not already done), closes the write channel, chain-closes all Readers, and
     * deletes all spill files on disk. After close() returns, no physical spill files remain and
     * this spill file cannot be reused.
     */
    @Override
    public void close() throws IOException {
        finish();
        try {
            if (currentChannel != null) {
                currentChannel.close();
                currentChannel = null;
            }
        } finally {
            for (Reader r : readers) {
                r.close();
            }
            // Best-effort cleanup of all spill files. Called unconditionally so callers do not
            // need a separate delete step.
            for (Reader r : readers) {
                try {
                    Files.deleteIfExists(r.filePath);
                } catch (IOException ignored) {
                    // best-effort cleanup
                }
            }
        }
    }

    /** Returns true after {@link #finish()} has been called. */
    public boolean isFinished() {
        return finished;
    }

    /**
     * Returns true when no entry is currently pending on disk — either no file was ever opened, or
     * every entry previously written has already been consumed (via {@link Reader#readNext()}) back
     * into memory. While idle, the dispatcher prefers P1 (direct buffer) over P2 (spill); FIFO
     * ordering is still preserved because there are no on-disk entries to jump ahead of.
     */
    public boolean isIdle() {
        for (Reader r : readers) {
            if (r.hasEntries()) {
                return false;
            }
        }
        return true;
    }

    /** Returns an unmodifiable view of all Readers created so far. */
    public List<Reader> getReaders() {
        return Collections.unmodifiableList(readers);
    }

    private Reader currentReader() {
        return readers.get(readers.size() - 1);
    }

    private void openNewFile() throws IOException {
        String dir = spillDirs[currentDirIndex];
        currentDirIndex = (currentDirIndex + 1) % spillDirs.length;
        Path dirPath = Paths.get(dir);
        Files.createDirectories(dirPath);
        Path currentFilePath = dirPath.resolve("spill-" + UUID.randomUUID() + ".bin");
        currentChannel =
                FileChannel.open(
                        currentFilePath,
                        StandardOpenOption.CREATE_NEW,
                        StandardOpenOption.WRITE);
        currentFileOffset = 0;
        readers.add(new Reader(currentFilePath, memorySegmentSize, nextFileIndex++));
    }

    private void rotateFile() throws IOException {
        // Seal the current Reader before opening a new file.
        currentReader().seal();
        currentChannel.close();
        currentChannel = null;
        openNewFile();
    }

    // -------------------------------------------------------------------------
    // Chunk — the payload unit returned by Reader.readNext()
    // -------------------------------------------------------------------------

    /**
     * A single spilled-data chunk returned by {@link Reader#readNext()}. The {@code data} array is
     * reused between calls on the same Reader; callers must consume bytes before the next readNext.
     */
    public static final class Chunk {

        private final InputChannelInfo channelInfo;
        private final byte[] data;
        private final int length;

        public Chunk(InputChannelInfo channelInfo, byte[] data, int length) {
            this.channelInfo = channelInfo;
            this.data = data;
            this.length = length;
        }

        public InputChannelInfo getChannelInfo() {
            return channelInfo;
        }

        /** Returns the internal data buffer; valid bytes are {@code [0, length)}. */
        public byte[] getData() {
            return data;
        }

        /** Number of valid bytes at the start of {@link #getData()}. */
        public int getLength() {
            return length;
        }
    }

    // -------------------------------------------------------------------------
    // Reader — per-physical-file reader with entry deque and sealed state
    // -------------------------------------------------------------------------

    /**
     * Reads entries from a single spill file. Each instance is owned by exactly one consumer
     * thread: the original Reader by the replay path, a snapshot Reader by a checkpoint drain.
     *
     * <p>The internal buffer is reused across {@link #readNext()} calls; callers must consume each
     * Chunk before calling readNext again.
     */
    public static class Reader implements Closeable {

        private final FileChannel channel;
        final Path filePath; // accessed by FilteredSpillFile.close() to delete spill files
        private final int memorySegmentSize;
        private final int fileIndex;
        private final Deque<Entry> entries = new ArrayDeque<>();
        private volatile boolean sealed = false;
        private final byte[] buf;

        Reader(Path filePath, int memorySegmentSize, int fileIndex) throws IOException {
            this.filePath = filePath;
            this.channel = FileChannel.open(filePath, StandardOpenOption.READ);
            this.memorySegmentSize = memorySegmentSize;
            this.fileIndex = fileIndex;
            // Pre-allocated to memorySegmentSize; every entry is guaranteed to fit because
            // FilteredSpillFile#writeEntry rejects oversized payloads at write time.
            this.buf = new byte[memorySegmentSize];
        }

        /** Index of this Reader's file in the spill file's {@code readers} list (0-based). */
        public int getFileIndex() {
            return fileIndex;
        }

        // ---- Write side (called by FilteredSpillFile) ----

        /** Registers an entry at {@code offset} with {@code length} bytes for {@code channelInfo}. */
        void addEntry(InputChannelInfo channelInfo, long offset, int length) {
            checkState(!sealed, "addEntry after seal");
            entries.addLast(new Entry(channelInfo, offset, length));
        }

        /** Returns the head pending entry without consuming it, or null if empty. */
        public Entry peekNextEntry() {
            return entries.peekFirst();
        }

        /**
         * Removes the head pending entry without performing any disk I/O. Returns the dropped entry
         * for callers that still need its metadata, or null if the deque was already empty. Use this
         * when the caller has already read the entry's bytes via {@link #readBytesAt} or has chosen
         * to discard it (e.g. phase-2 filter skipping an entry whose channel has already snapshotted
         * it via Step 1).
         */
        public Entry skipNextEntry() {
            return entries.pollFirst();
        }

        /**
         * Reads {@code length} bytes starting at absolute {@code offset} from this file into
         * {@code dest} starting at {@code destOffset}. Throws {@link IOException} on truncation or
         * an underlying read failure. Unlike {@link #readNext()} this method does <em>not</em>
         * mutate the entry deque, so callers can safely perform the disk I/O outside any lock that
         * also protects the deque.
         */
        public void readBytesAt(long offset, int length, byte[] dest, int destOffset)
                throws IOException {
            ByteBuffer bb = ByteBuffer.wrap(dest, destOffset, length);
            long position = offset;
            while (bb.hasRemaining()) {
                int n = channel.read(bb, position);
                if (n < 0) {
                    throw new IOException(
                            "Truncated spill file: "
                                    + length
                                    + " bytes @"
                                    + offset
                                    + " in "
                                    + filePath);
                }
                position += n;
            }
        }

        /** Seals this Reader; no more addEntry calls are allowed after this point. */
        void seal() {
            sealed = true;
        }

        public boolean isSealed() {
            return sealed;
        }

        // ---- Consume side (replay or checkpoint drain) ----

        /** Returns true if there are pending entries to consume. */
        public boolean hasEntries() {
            return !entries.isEmpty();
        }

        /**
         * Returns the channel of the next pending entry without consuming it, or null if empty.
         */
        public InputChannelInfo peekNextChannel() {
            Entry e = entries.peekFirst();
            return e != null ? e.channelInfo : null;
        }

        /**
         * Reads and returns the next pending entry as a {@link Chunk}. The Chunk's data array is
         * the Reader's internal buffer; it is overwritten by the next readNext call. Returns null
         * when there are no more entries.
         */
        public Chunk readNext() throws IOException {
            Entry entry = entries.pollFirst();
            if (entry == null) {
                return null;
            }
            // writeEntry enforces entry.length <= memorySegmentSize, so buf always fits.
            ByteBuffer bb = ByteBuffer.wrap(buf, 0, entry.length);
            long position = entry.offset;
            while (bb.hasRemaining()) {
                int n = channel.read(bb, position);
                if (n < 0) {
                    throw new IOException(
                            "Truncated spill file: "
                                    + entry.length
                                    + " bytes @"
                                    + entry.offset
                                    + " in "
                                    + filePath);
                }
                position += n;
            }
            return new Chunk(entry.channelInfo, buf, entry.length);
        }

        /**
         * Returns an independent Reader over the same file with a shallow copy of the current
         * entries. The snapshot is pre-sealed. Must be called only after this Reader is sealed.
         * The caller owns and must close the returned Reader.
         */
        public Reader snapshot() throws IOException {
            checkState(sealed, "snapshot requires sealed Reader");
            Reader snap = new Reader(filePath, memorySegmentSize, fileIndex);
            snap.entries.addAll(this.entries);
            snap.sealed = true;
            return snap;
        }

        /** Returns the set of channels that still have pending entries. */
        public Set<InputChannelInfo> getPendingChannels() {
            Set<InputChannelInfo> channels = new HashSet<>();
            for (Entry e : entries) {
                channels.add(e.channelInfo);
            }
            return channels;
        }

        /**
         * Removes all pending entries belonging to {@code channelInfo} and returns how many were
         * dropped. Used when a per-channel store is released so that the dispatcher can free the
         * channel's disk-side bookkeeping eagerly instead of leaving it to {@link
         * FilteredSpillFile#close()}.
         */
        public int removeEntriesForChannel(InputChannelInfo channelInfo) {
            int removed = 0;
            Iterator<Entry> it = entries.iterator();
            while (it.hasNext()) {
                if (it.next().channelInfo.equals(channelInfo)) {
                    it.remove();
                    removed++;
                }
            }
            return removed;
        }

        @Override
        public void close() throws IOException {
            channel.close();
        }

        // ---- Entry metadata ----

        /**
         * Immutable metadata for a single spilled entry: the target channel, the byte offset in
         * the owning file, and the payload length. Exposed publicly so the dispatcher can inspect
         * the next entry (channel + offset + length) and perform disk I/O outside the deque-mutating
         * commit section without re-implementing the metadata accessor surface.
         */
        public static final class Entry {
            private final InputChannelInfo channelInfo;
            private final long offset;
            private final int length;

            Entry(InputChannelInfo channelInfo, long offset, int length) {
                this.channelInfo = channelInfo;
                this.offset = offset;
                this.length = length;
            }

            public InputChannelInfo getChannelInfo() {
                return channelInfo;
            }

            public long getOffset() {
                return offset;
            }

            public int getLength() {
                return length;
            }
        }
    }
}
