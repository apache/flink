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
import java.util.ArrayList;
import java.util.Collections;
import java.util.Deque;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.ConcurrentLinkedDeque;

import static org.apache.flink.util.Preconditions.checkArgument;
import static org.apache.flink.util.Preconditions.checkState;

/**
 * Spill file for the {@code filterAndRewrite} recovery path. Appends raw bytes to one or more
 * physical files; each logical entry is tracked in the corresponding {@link Reader}'s entry deque.
 * Readers support replay ({@link Reader#readNext}) and checkpoint snapshot ({@link
 * Reader#snapshot}). Rotates to a new file when the current one exceeds 64 MB; each rotation
 * freezes the outgoing Reader. Files are created lazily on the first {@link #writeEntry}.
 */
@Internal
public class FilteredSpillFile implements Closeable {

    private static final long FILE_ROTATION_THRESHOLD = 64L * 1024 * 1024;

    private final String[] spillDirs;
    private final int memorySegmentSize;
    private int currentDirIndex;
    private FileChannel currentChannel;
    private long currentFileOffset;
    private final List<Reader> readers;

    /** Monotonic file-id counter; never reused. */
    private int nextFileIndex;

    private boolean finished;

    /**
     * @param memorySegmentSize max bytes per spill entry — each entry is 1:1 aligned with a network
     *     buffer of this size, so longer payloads must be split upstream.
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
     * Appends {@code len} bytes for {@code channelInfo}, lazily opening the first file and rotating
     * when the current file exceeds {@link #FILE_ROTATION_THRESHOLD}. Oversize entries (> {@code
     * memorySegmentSize}) fail fast.
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

    /** Freezes the last Reader. After finish, no more writeEntry calls are accepted. */
    public void finish() {
        if (finished) {
            return;
        }
        finished = true;
        if (!readers.isEmpty()) {
            currentReader().freeze();
        }
    }

    /** Closes all Readers and deletes the underlying spill files. */
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
            for (Reader r : readers) {
                try {
                    Files.deleteIfExists(r.filePath);
                } catch (IOException ignored) {
                    // best-effort cleanup
                }
            }
        }
    }

    public boolean isFinished() {
        return finished;
    }

    /**
     * True when no entry is pending on disk. While idle, the dispatcher prefers P1; FIFO ordering
     * is preserved because there are no on-disk entries to jump ahead of.
     */
    public boolean isIdle() {
        for (Reader r : readers) {
            if (r.hasEntries()) {
                return false;
            }
        }
        return true;
    }

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
                        currentFilePath, StandardOpenOption.CREATE_NEW, StandardOpenOption.WRITE);
        currentFileOffset = 0;
        readers.add(new Reader(currentFilePath, memorySegmentSize, nextFileIndex++));
    }

    private void rotateFile() throws IOException {
        currentReader().freeze();
        currentChannel.close();
        currentChannel = null;
        openNewFile();
    }

    /**
     * Spilled-data chunk returned by {@link Reader#readNext()}. The {@code data} array is the
     * Reader's internal buffer, reused between calls; callers must consume bytes before the next
     * readNext.
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

        /** Internal data buffer; valid bytes are {@code [0, length)}. */
        public byte[] getData() {
            return data;
        }

        public int getLength() {
            return length;
        }
    }

    /**
     * Reads entries from a single spill file. The original Reader is mutated by the recovery
     * thread (write path, replay drain) and concurrently by task threads via {@link
     * #removeEntriesForChannel} on channel release. To avoid undefined behavior on the entry
     * deque, the backing storage is a {@link ConcurrentLinkedDeque} — its weakly consistent
     * iterator and atomic poll/peek tolerate the writer-vs-release race that the post-iter_6
     * dispatcher no longer wraps in any monitor. The internal byte buffer is reused across
     * {@link #readNext()} calls; callers must consume each {@link Chunk} before calling readNext
     * again.
     */
    public static class Reader implements Closeable {

        private final FileChannel channel;
        final Path filePath; // accessed by FilteredSpillFile.close() to delete spill files
        private final int memorySegmentSize;
        private final int fileIndex;
        private final Deque<Entry> entries = new ConcurrentLinkedDeque<>();
        private volatile boolean frozen = false;
        private final byte[] buf;

        Reader(Path filePath, int memorySegmentSize, int fileIndex) throws IOException {
            this.filePath = filePath;
            this.channel = FileChannel.open(filePath, StandardOpenOption.READ);
            this.memorySegmentSize = memorySegmentSize;
            this.fileIndex = fileIndex;
            // writeEntry rejects oversized payloads, so every entry is guaranteed to fit.
            this.buf = new byte[memorySegmentSize];
        }

        public int getFileIndex() {
            return fileIndex;
        }

        void addEntry(InputChannelInfo channelInfo, long offset, int length) {
            checkState(!frozen, "addEntry after freeze");
            entries.addLast(new Entry(channelInfo, offset, length));
        }

        /** Head entry without consuming it; null if empty. */
        public Entry peekNextEntry() {
            return entries.peekFirst();
        }

        /**
         * Removes the head entry without disk I/O. Use after reading via {@link #readBytesAt} or to
         * discard (e.g. phase-2 filter skipping an entry already covered by Step 1).
         */
        public Entry skipNextEntry() {
            return entries.pollFirst();
        }

        /**
         * Reads {@code length} bytes from absolute {@code offset} into {@code dest}. Unlike {@link
         * #readNext()} this does not mutate the entry deque, so callers can perform the I/O outside
         * any lock that protects the deque.
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

        void freeze() {
            frozen = true;
        }

        public boolean isFrozen() {
            return frozen;
        }

        public boolean hasEntries() {
            return !entries.isEmpty();
        }

        /** Channel of the next pending entry; null if empty. */
        public InputChannelInfo peekNextChannel() {
            Entry e = entries.peekFirst();
            return e != null ? e.channelInfo : null;
        }

        /**
         * Reads the next pending entry as a {@link Chunk}; null when there are no more entries. The
         * Chunk's data array is the Reader's internal buffer and is overwritten by the next
         * readNext.
         */
        public Chunk readNext() throws IOException {
            Entry entry = entries.pollFirst();
            if (entry == null) {
                return null;
            }
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
         * Independent Reader over the same file with a shallow copy of the current entries,
         * pre-frozen. The caller owns and must close the returned Reader.
         */
        public Reader snapshot() throws IOException {
            checkState(frozen, "snapshot requires frozen Reader");
            Reader snap = new Reader(filePath, memorySegmentSize, fileIndex);
            snap.entries.addAll(this.entries);
            snap.frozen = true;
            return snap;
        }

        public Set<InputChannelInfo> getPendingChannels() {
            Set<InputChannelInfo> channels = new HashSet<>();
            for (Entry e : entries) {
                channels.add(e.channelInfo);
            }
            return channels;
        }

        /**
         * Drops all pending entries for {@code channelInfo}; returns the count. Used when a store
         * is released so disk-side bookkeeping is freed eagerly.
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

        /** Immutable metadata for a single spilled entry: target channel, offset, length. */
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
