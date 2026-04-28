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

import org.apache.flink.runtime.memory.MemoryManager;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.io.IOException;
import java.io.RandomAccessFile;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;
import java.util.Random;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * Tests for {@link FilteredSpillFile}, {@link FilteredSpillFile.Reader}, and {@link
 * FilteredSpillFile.Chunk}.
 */
class FilteredSpillFileTest {

    @TempDir private Path temporaryFolder;

    private static final int MEMORY_SEGMENT_SIZE = MemoryManager.DEFAULT_PAGE_SIZE;

    private static final InputChannelInfo CHANNEL_0 = new InputChannelInfo(0, 0);
    private static final InputChannelInfo CHANNEL_1 = new InputChannelInfo(0, 1);

    /** Write a single entry and read it back via readNext; verify bytes match. */
    @Test
    void testSingleEntryRoundTrip() throws Exception {
        String[] spillDirs = {temporaryFolder.toString()};
        Random random = new Random(42);
        byte[] data = new byte[1024];
        random.nextBytes(data);

        try (FilteredSpillFile writer = new FilteredSpillFile(spillDirs, MEMORY_SEGMENT_SIZE)) {
            writer.writeEntry(data, data.length, CHANNEL_0);
            writer.finish();

            FilteredSpillFile.Reader reader = writer.getReaders().get(0);
            assertThat(reader.hasEntries()).isTrue();
            FilteredSpillFile.Chunk chunk = reader.readNext();
            assertThat(chunk).isNotNull();
            assertThat(chunk.getChannelInfo()).isEqualTo(CHANNEL_0);
            assertThat(chunk.getLength()).isEqualTo(data.length);
            assertThat(chunk.getData()).startsWith(data);
            assertThat(reader.hasEntries()).isFalse();
        }
    }

    /** Write multiple entries across channels; verify readNext returns them in order. */
    @Test
    void testMultipleEntriesInOrder() throws Exception {
        String[] spillDirs = {temporaryFolder.toString()};
        byte[] d0 = new byte[] {1, 2, 3, 4};
        byte[] d1 = new byte[] {5, 6, 7, 8};

        try (FilteredSpillFile writer = new FilteredSpillFile(spillDirs, MEMORY_SEGMENT_SIZE)) {
            writer.writeEntry(d0, d0.length, CHANNEL_0);
            writer.writeEntry(d1, d1.length, CHANNEL_1);
            writer.finish();

            FilteredSpillFile.Reader reader = writer.getReaders().get(0);
            FilteredSpillFile.Chunk c0 = reader.readNext();
            assertThat(c0.getChannelInfo()).isEqualTo(CHANNEL_0);
            assertThat(c0.getLength()).isEqualTo(d0.length);
            byte[] actual0 = new byte[d0.length];
            System.arraycopy(c0.getData(), 0, actual0, 0, d0.length);
            assertThat(actual0).isEqualTo(d0);

            FilteredSpillFile.Chunk c1 = reader.readNext();
            assertThat(c1.getChannelInfo()).isEqualTo(CHANNEL_1);
            assertThat(c1.getLength()).isEqualTo(d1.length);
            byte[] actual1 = new byte[d1.length];
            System.arraycopy(c1.getData(), 0, actual1, 0, d1.length);
            assertThat(actual1).isEqualTo(d1);

            assertThat(reader.readNext()).isNull();
        }
    }

    /**
     * Write more than 64MB to trigger file rotation; verify multiple Readers are created and data
     * is correct across files.
     */
    @Test
    void testFileRotation() throws Exception {
        Path dir1 = Files.createDirectory(temporaryFolder.resolve("dir1"));
        Path dir2 = Files.createDirectory(temporaryFolder.resolve("dir2"));
        String[] spillDirs = {dir1.toString(), dir2.toString()};

        // 64MB / DEFAULT_PAGE_SIZE + extra to force at least one rotation
        int numEntries = (int) (64L * 1024 * 1024 / MEMORY_SEGMENT_SIZE) + 10;
        byte[][] chunks = new byte[numEntries][MEMORY_SEGMENT_SIZE];
        Random random = new Random(42);
        for (byte[] chunk : chunks) {
            random.nextBytes(chunk);
        }

        try (FilteredSpillFile writer = new FilteredSpillFile(spillDirs, MEMORY_SEGMENT_SIZE)) {
            for (int i = 0; i < numEntries; i++) {
                writer.writeEntry(chunks[i], MEMORY_SEGMENT_SIZE, CHANNEL_0);
            }
            writer.finish();

            assertThat(writer.getReaders().size()).isGreaterThan(1);

            int idx = 0;
            for (FilteredSpillFile.Reader reader : writer.getReaders()) {
                while (reader.hasEntries()) {
                    FilteredSpillFile.Chunk chunk = reader.readNext();
                    byte[] actual = new byte[chunk.getLength()];
                    System.arraycopy(chunk.getData(), 0, actual, 0, chunk.getLength());
                    assertThat(actual).isEqualTo(chunks[idx++]);
                }
            }
            assertThat(idx).isEqualTo(numEntries);
        }
    }

    /** Writer.close() finishes and releases resources; writeEntry after close throws. */
    @Test
    void testCloseReleasesResources() throws Exception {
        String[] spillDirs = {temporaryFolder.toString()};
        FilteredSpillFile writer = new FilteredSpillFile(spillDirs, MEMORY_SEGMENT_SIZE);
        writer.writeEntry(new byte[] {1, 2, 3}, 3, CHANNEL_0);
        writer.close();

        assertThatThrownBy(() -> writer.writeEntry(new byte[] {4}, 1, CHANNEL_0))
                .isInstanceOf(IllegalStateException.class);
    }

    /** Truncated file causes readNext to throw IOException. */
    @Test
    void testTruncatedFileThrows() throws Exception {
        String[] spillDirs = {temporaryFolder.toString()};
        byte[] data = new byte[1024];
        new Random(42).nextBytes(data);

        try (FilteredSpillFile writer = new FilteredSpillFile(spillDirs, MEMORY_SEGMENT_SIZE)) {
            writer.writeEntry(data, data.length, CHANNEL_0);
            writer.finish();

            // Truncate the spill file to half
            Path filePath = writer.getReaders().get(0).filePath;
            try (RandomAccessFile raf = new RandomAccessFile(filePath.toFile(), "rw")) {
                raf.setLength(data.length / 2);
            }

            assertThatThrownBy(() -> writer.getReaders().get(0).readNext())
                    .isInstanceOf(IOException.class);
        }
    }

    /** snapshot() creates an independent Reader with the same entries; pre-sealed. */
    @Test
    void testSnapshot() throws Exception {
        String[] spillDirs = {temporaryFolder.toString()};
        byte[] data = new byte[256];
        for (int i = 0; i < data.length; i++) {
            data[i] = (byte) i;
        }

        try (FilteredSpillFile writer = new FilteredSpillFile(spillDirs, MEMORY_SEGMENT_SIZE)) {
            writer.writeEntry(data, data.length, CHANNEL_0);
            writer.finish();

            FilteredSpillFile.Reader original = writer.getReaders().get(0);
            assertThat(original.isSealed()).isTrue();

            FilteredSpillFile.Reader snap = original.snapshot();
            try {
                assertThat(snap.isSealed()).isTrue();
                assertThat(snap.hasEntries()).isTrue();

                FilteredSpillFile.Chunk chunk = snap.readNext();
                assertThat(chunk.getLength()).isEqualTo(data.length);
                byte[] actual = new byte[data.length];
                System.arraycopy(chunk.getData(), 0, actual, 0, data.length);
                assertThat(actual).isEqualTo(data);

                // Original still has entries (snapshot is independent)
                assertThat(original.hasEntries()).isTrue();
            } finally {
                snap.close();
            }
        }
    }

    /** addEntry after seal throws IllegalStateException. */
    @Test
    void testAddEntryAfterSealThrows() throws Exception {
        String[] spillDirs = {temporaryFolder.toString()};
        try (FilteredSpillFile writer = new FilteredSpillFile(spillDirs, MEMORY_SEGMENT_SIZE)) {
            writer.writeEntry(new byte[] {1}, 1, CHANNEL_0);
            writer.finish();
            // Reader is sealed by finish(); addEntry via a new writeEntry after finish should throw
            assertThatThrownBy(() -> writer.writeEntry(new byte[] {2}, 1, CHANNEL_0))
                    .isInstanceOf(IllegalStateException.class);
        }
    }

    /** peekNextChannel returns the channel of the next entry without consuming it. */
    @Test
    void testPeekNextChannel() throws Exception {
        String[] spillDirs = {temporaryFolder.toString()};
        try (FilteredSpillFile writer = new FilteredSpillFile(spillDirs, MEMORY_SEGMENT_SIZE)) {
            writer.writeEntry(new byte[] {1, 2}, 2, CHANNEL_0);
            writer.writeEntry(new byte[] {3, 4}, 2, CHANNEL_1);
            writer.finish();

            FilteredSpillFile.Reader reader = writer.getReaders().get(0);
            assertThat(reader.peekNextChannel()).isEqualTo(CHANNEL_0);
            reader.readNext();
            assertThat(reader.peekNextChannel()).isEqualTo(CHANNEL_1);
            reader.readNext();
            assertThat(reader.peekNextChannel()).isNull();
        }
    }

    /** getPendingChannels returns all channels with pending entries. */
    @Test
    void testGetPendingChannels() throws Exception {
        String[] spillDirs = {temporaryFolder.toString()};
        try (FilteredSpillFile writer = new FilteredSpillFile(spillDirs, MEMORY_SEGMENT_SIZE)) {
            writer.writeEntry(new byte[] {1}, 1, CHANNEL_0);
            writer.writeEntry(new byte[] {2}, 1, CHANNEL_1);
            writer.finish();

            FilteredSpillFile.Reader reader = writer.getReaders().get(0);
            assertThat(reader.getPendingChannels()).containsExactlyInAnyOrder(CHANNEL_0, CHANNEL_1);

            reader.readNext(); // consume CHANNEL_0
            assertThat(reader.getPendingChannels()).containsExactly(CHANNEL_1);

            reader.readNext(); // consume CHANNEL_1
            assertThat(reader.getPendingChannels()).isEmpty();
        }
    }

    /** isIdle() returns true before any writeEntry call, false after. */
    @Test
    void testIsIdle() throws Exception {
        String[] spillDirs = {temporaryFolder.toString()};
        try (FilteredSpillFile writer = new FilteredSpillFile(spillDirs, MEMORY_SEGMENT_SIZE)) {
            assertThat(writer.isIdle()).isTrue();
            writer.writeEntry(new byte[] {1}, 1, CHANNEL_0);
            assertThat(writer.isIdle()).isFalse();
        }
    }

    /**
     * isIdle() flips dynamically with the reader entry count: empty at start, non-idle after any
     * write, stays non-idle while entries remain even if some are partially drained, idle again
     * only after all entries have been consumed. Must behave consistently across multiple write /
     * drain rounds.
     */
    @Test
    void testIsIdleFlipsAcrossWriteDrainRounds() throws Exception {
        String[] spillDirs = {temporaryFolder.toString()};
        try (FilteredSpillFile writer = new FilteredSpillFile(spillDirs, MEMORY_SEGMENT_SIZE)) {
            assertThat(writer.isIdle()).isTrue();

            writer.writeEntry(new byte[] {1}, 1, CHANNEL_0);
            writer.writeEntry(new byte[] {2}, 1, CHANNEL_1);
            writer.writeEntry(new byte[] {3}, 1, CHANNEL_0);
            assertThat(writer.isIdle()).isFalse();

            FilteredSpillFile.Reader reader = writer.getReaders().get(0);

            reader.readNext();
            assertThat(writer.isIdle()).isFalse();

            reader.readNext();
            assertThat(writer.isIdle()).isFalse();

            reader.readNext();
            assertThat(reader.hasEntries()).isFalse();
            assertThat(writer.isIdle()).isTrue();

            writer.writeEntry(new byte[] {4}, 1, CHANNEL_1);
            writer.writeEntry(new byte[] {5}, 1, CHANNEL_0);
            assertThat(writer.isIdle()).isFalse();

            reader.readNext();
            assertThat(writer.isIdle()).isFalse();

            reader.readNext();
            assertThat(reader.hasEntries()).isFalse();
            assertThat(writer.isIdle()).isTrue();
        }
    }

    /** close() deletes all spill files on disk. */
    @Test
    void testCloseDeletesAllFiles() throws Exception {
        String[] spillDirs = {temporaryFolder.toString()};
        FilteredSpillFile writer = new FilteredSpillFile(spillDirs, MEMORY_SEGMENT_SIZE);
        writer.writeEntry(new byte[64], 64, CHANNEL_0);
        List<Path> filePaths = new ArrayList<>();
        for (FilteredSpillFile.Reader r : writer.getReaders()) {
            filePaths.add(r.filePath);
        }
        for (Path p : filePaths) {
            assertThat(Files.exists(p)).isTrue();
        }

        writer.close();

        for (Path p : filePaths) {
            assertThat(Files.exists(p)).isFalse();
        }
    }
}
