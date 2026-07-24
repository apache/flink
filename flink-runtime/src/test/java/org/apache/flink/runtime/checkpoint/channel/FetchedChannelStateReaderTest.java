/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.flink.runtime.checkpoint.channel;

import org.apache.flink.runtime.checkpoint.channel.FetchedChannelStateReader.SpillSegment;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.io.EOFException;
import java.io.IOException;
import java.io.InputStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Optional;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * Unit tests for {@link FetchedChannelStateReader}: sequential segment scanning, body boundedness,
 * cross-file transparency, snapshot derivation, and fail-loud on truncated segments.
 *
 * <p>Segment boundaries are self-described in disk headers; no in-memory segment locator table is
 * used.
 */
class FetchedChannelStateReaderTest {

    @TempDir Path tempDir;

    // -------------------------------------------------------------------------------------------
    // Segment iteration
    // -------------------------------------------------------------------------------------------

    @Test
    void testIteratorEmptyWhenNoDataWritten() throws Exception {
        // A writer that never spills produces no state; an empty state has no segments.
        FetchedChannelState state = new FetchedChannelState(Collections.emptyList());
        try (FetchedChannelStateReader reader = state.reader()) {
            assertThat(reader.nextSegment()).isEmpty();
        }
    }

    @Test
    void testMultipleIteratorIteratedInOrder() throws Exception {
        InputChannelInfo c0 = new InputChannelInfo(0, 0);
        InputChannelInfo c1 = new InputChannelInfo(0, 1);

        FetchedChannelState state;
        try (TestSpillWriter writer = new TestSpillWriter(tempDir)) {
            writer.writeRecord(c0, bytes(10), 1);
            writer.writeRecord(c1, bytes(20), 1);
            writer.writeRecord(c0, bytes(30), 1);
            state = writer.getChannelState();
        }

        List<InputChannelInfo> channels = new ArrayList<>();
        try (FetchedChannelStateReader reader = state.reader()) {
            Optional<SpillSegment> next;
            while ((next = reader.nextSegment()).isPresent()) {
                SpillSegment seg = next.get();
                channels.add(seg.channelInfo());
                readAll(seg.bodyStream());
            }
        }

        // Segments are produced at channel switches: c0, c1, c0
        assertThat(channels).containsExactly(c0, c1, c0);
    }

    // -------------------------------------------------------------------------------------------
    // Body boundedness: body() stops exactly at segment end
    // -------------------------------------------------------------------------------------------

    @Test
    void testBodyReturnsMinus1AtSegmentEnd() throws Exception {
        InputChannelInfo ch = new InputChannelInfo(0, 0);

        FetchedChannelState state;
        try (TestSpillWriter writer = new TestSpillWriter(tempDir)) {
            writer.writeRecord(ch, bytes(1, 2), 2);
            state = writer.getChannelState();
        }

        try (FetchedChannelStateReader reader = state.reader()) {
            SpillSegment seg = reader.nextSegment().orElseThrow(AssertionError::new);
            InputStream body = seg.bodyStream();
            // Read exactly length bytes
            byte[] data = new byte[seg.length()];
            int totalRead = 0;
            while (totalRead < data.length) {
                int n = body.read(data, totalRead, data.length - totalRead);
                assertThat(n).isGreaterThan(0);
                totalRead += n;
            }
            // Next read must return EOF
            assertThat(body.read()).isEqualTo(-1);
        }
    }

    // -------------------------------------------------------------------------------------------
    // Cross-file transparency
    // -------------------------------------------------------------------------------------------

    @Test
    void testCrossFileTransparencyWhenRotationOccurs() throws Exception {
        InputChannelInfo c0 = new InputChannelInfo(0, 0);
        InputChannelInfo c1 = new InputChannelInfo(0, 1);

        // Use tiny rotation threshold so first segment triggers a file rotation.
        FetchedChannelState state;
        try (TestSpillWriter writer = new TestSpillWriter(tempDir, 1L /* 1 byte threshold */)) {
            writer.writeRecord(c0, bytes(10, 11, 12), 3);
            writer.writeRecord(c1, bytes(20, 21), 2);
            state = writer.getChannelState();
        }

        // Two segments in different files.
        assertThat(state.files()).hasSize(2);

        List<InputChannelInfo> channels = new ArrayList<>();
        try (FetchedChannelStateReader reader = state.reader()) {
            Optional<SpillSegment> next;
            while ((next = reader.nextSegment()).isPresent()) {
                SpillSegment seg = next.get();
                channels.add(seg.channelInfo());
                // Body read must not throw even if the segment is in a different file.
                readAll(seg.bodyStream());
            }
        }

        assertThat(channels).containsExactly(c0, c1);
    }

    // -------------------------------------------------------------------------------------------
    // Snapshot: independent reader with correct start position
    // -------------------------------------------------------------------------------------------

    @Test
    void testSnapshotCoversAllIteratorWhenNothingConsumed() throws Exception {
        InputChannelInfo c0 = new InputChannelInfo(0, 0);
        InputChannelInfo c1 = new InputChannelInfo(0, 1);

        FetchedChannelState state;
        try (TestSpillWriter writer = new TestSpillWriter(tempDir)) {
            writer.writeRecord(c0, bytes(1), 1);
            writer.writeRecord(c1, bytes(2), 1);
            state = writer.getChannelState();
        }

        try (FetchedChannelStateReader root = state.reader()) {
            // Snapshot before consuming anything
            try (FetchedChannelStateReader snap = root.snapshot().reader()) {
                List<InputChannelInfo> channels = new ArrayList<>();
                Optional<SpillSegment> next;
                while ((next = snap.nextSegment()).isPresent()) {
                    SpillSegment seg = next.get();
                    channels.add(seg.channelInfo());
                    readAll(seg.bodyStream());
                }
                assertThat(channels).containsExactly(c0, c1);
            }
        }
    }

    @Test
    void testSnapshotAfterFullSegmentConsumedSkipsThatSegment() throws Exception {
        InputChannelInfo c0 = new InputChannelInfo(0, 0);
        InputChannelInfo c1 = new InputChannelInfo(0, 1);

        FetchedChannelState state;
        try (TestSpillWriter writer = new TestSpillWriter(tempDir)) {
            writer.writeRecord(c0, bytes(1), 1);
            writer.writeRecord(c1, bytes(2), 1);
            state = writer.getChannelState();
        }

        try (FetchedChannelStateReader root = state.reader()) {
            // Consume and commit first segment
            SpillSegment first = root.nextSegment().orElseThrow(AssertionError::new);
            readAll(first.bodyStream());
            first.commit();

            // Snapshot must start from second segment
            try (FetchedChannelStateReader snap = root.snapshot().reader()) {
                List<InputChannelInfo> channels = new ArrayList<>();
                Optional<SpillSegment> next;
                while ((next = snap.nextSegment()).isPresent()) {
                    SpillSegment seg = next.get();
                    channels.add(seg.channelInfo());
                    readAll(seg.bodyStream());
                }
                assertThat(channels).containsExactly(c1);
            }
        }
    }

    @Test
    void testSnapshotFromMidSegmentStartsAtCommittedByteOffset() throws Exception {
        InputChannelInfo ch = new InputChannelInfo(0, 0);

        FetchedChannelState state;
        try (TestSpillWriter writer = new TestSpillWriter(tempDir)) {
            // Two records in the same channel -> one segment
            writer.writeRecord(ch, bytes(10, 11), 2);
            state = writer.getChannelState();
        }
        // Verify: one file with one segment
        assertThat(state.files()).hasSize(1);

        try (FetchedChannelStateReader root = state.reader()) {
            SpillSegment seg = root.nextSegment().orElseThrow(AssertionError::new);
            int fullLength = seg.length();
            InputStream body = seg.bodyStream();

            // Read only 1 byte without committing, then snapshot — snapshot should start from 0
            // (no bytes committed yet).
            body.read();

            try (FetchedChannelStateReader snapBeforeCommit = root.snapshot().reader()) {
                SpillSegment snapSeg =
                        snapBeforeCommit.nextSegment().orElseThrow(AssertionError::new);
                assertThat(snapSeg.length()).isEqualTo(fullLength);
                readAll(snapSeg.bodyStream());
            }

            // Read rest of body and commit
            readAll(body);
            seg.commit();

            // After commit the snapshot must be empty
            try (FetchedChannelStateReader snapAfterCommit = root.snapshot().reader()) {
                assertThat(snapAfterCommit.nextSegment()).isEmpty();
            }
        }
    }

    @Test
    void testSnapshotAfterPartialCommitReadsRemainingBodyTail() throws Exception {
        InputChannelInfo ch = new InputChannelInfo(0, 0);

        FetchedChannelState state;
        try (TestSpillWriter writer = new TestSpillWriter(tempDir)) {
            // One segment whose body is a single pass-through blob of known bytes.
            writer.writePassThrough(ch, bytes(1, 2, 3, 4, 5, 6, 7, 8), 0, 8);
            state = writer.getChannelState();
        }
        assertThat(state.files()).hasSize(1);

        try (FetchedChannelStateReader root = state.reader()) {
            SpillSegment seg = root.nextSegment().orElseThrow(AssertionError::new);
            int fullLength = seg.length();
            InputStream body = seg.bodyStream();

            // Read and commit only a 3-byte prefix.
            byte[] prefix = new byte[3];
            assertThat(body.read(prefix)).isEqualTo(3);
            seg.commit();

            // Snapshot must resume mid-segment and yield exactly the remaining tail bytes.
            try (FetchedChannelStateReader snap = root.snapshot().reader()) {
                SpillSegment snapSeg = snap.nextSegment().orElseThrow(AssertionError::new);
                assertThat(snapSeg.channelInfo()).isEqualTo(ch);
                assertThat(snapSeg.length()).isEqualTo(fullLength - 3);
                byte[] tail = readAll(snapSeg.bodyStream());
                assertThat(tail).isEqualTo(bytes(4, 5, 6, 7, 8));
                assertThat(snap.nextSegment()).isEmpty();
            }
        }
    }

    @Test
    void testSnapshotResumesPartialSegmentAcrossFileBoundary() throws Exception {
        InputChannelInfo c0 = new InputChannelInfo(0, 0);
        InputChannelInfo c1 = new InputChannelInfo(0, 1);

        // Tiny rotation threshold so the two segments land in separate files.
        FetchedChannelState state;
        try (TestSpillWriter writer = new TestSpillWriter(tempDir, 1L)) {
            writer.writePassThrough(c0, bytes(1, 2, 3, 4), 0, 4);
            writer.writePassThrough(c1, bytes(5, 6, 7), 0, 3);
            state = writer.getChannelState();
        }
        assertThat(state.files()).hasSize(2);

        try (FetchedChannelStateReader root = state.reader()) {
            SpillSegment first = root.nextSegment().orElseThrow(AssertionError::new);
            // Commit a 1-byte prefix of the file-0 segment.
            first.bodyStream().read(new byte[1]);
            first.commit();

            try (FetchedChannelStateReader snap = root.snapshot().reader()) {
                SpillSegment resumed = snap.nextSegment().orElseThrow(AssertionError::new);
                assertThat(resumed.channelInfo()).isEqualTo(c0);
                assertThat(readAll(resumed.bodyStream())).isEqualTo(bytes(2, 3, 4));

                // Crossing into file 1 must reset the skip to 0.
                SpillSegment following = snap.nextSegment().orElseThrow(AssertionError::new);
                assertThat(following.channelInfo()).isEqualTo(c1);
                assertThat(readAll(following.bodyStream())).isEqualTo(bytes(5, 6, 7));

                assertThat(snap.nextSegment()).isEmpty();
            }
        }
    }

    @Test
    void testRootDrainViaRepeatedCommitsTerminatesAndFinalSnapshotEmpty() throws Exception {
        InputChannelInfo c0 = new InputChannelInfo(0, 0);
        InputChannelInfo c1 = new InputChannelInfo(0, 1);

        FetchedChannelState state;
        try (TestSpillWriter writer = new TestSpillWriter(tempDir)) {
            writer.writePassThrough(c0, bytes(1, 2), 0, 2);
            writer.writePassThrough(c1, bytes(3, 4, 5), 0, 3);
            state = writer.getChannelState();
        }

        try (FetchedChannelStateReader root = state.reader()) {
            int count = 0;
            Optional<SpillSegment> next;
            while ((next = root.nextSegment()).isPresent()) {
                SpillSegment seg = next.get();
                readAll(seg.bodyStream());
                seg.commit();
                count++;
            }
            assertThat(count).isEqualTo(2);

            // After draining everything, a snapshot must have nothing left.
            try (FetchedChannelStateReader snap = root.snapshot().reader()) {
                assertThat(snap.nextSegment()).isEmpty();
            }
        }
    }

    // -------------------------------------------------------------------------------------------
    // Fail-loud on truncated segment
    // -------------------------------------------------------------------------------------------

    @Test
    void testBodyThrowsEOFExceptionOnTruncatedFile() throws Exception {
        InputChannelInfo ch = new InputChannelInfo(0, 0);

        FetchedChannelState state;
        try (TestSpillWriter writer = new TestSpillWriter(tempDir)) {
            writer.writeRecord(ch, bytes(1, 2, 3, 4, 5, 6, 7, 8), 8);
            state = writer.getChannelState();
        }

        // Truncate the spill file to just the header (12 bytes) so the body is missing.
        Path spill = state.files().get(0);
        byte[] headerOnly = Files.readAllBytes(spill);
        // Keep only the 12-byte header, discard body.
        Files.write(
                spill,
                java.util.Arrays.copyOf(headerOnly, AbstractSpillingHandler.SEGMENT_HEADER_BYTES),
                StandardOpenOption.TRUNCATE_EXISTING);

        try (FetchedChannelStateReader reader = state.reader()) {
            SpillSegment seg = reader.nextSegment().orElseThrow(AssertionError::new);
            // bufferLength from header says > 0 bytes, but file has nothing after the header.
            assertThatThrownBy(() -> readAll(seg.bodyStream())).isInstanceOf(EOFException.class);
        }
    }

    // -------------------------------------------------------------------------------------------
    // Reference counting: acquire/release via reader lifecycle
    // -------------------------------------------------------------------------------------------

    @Test
    void testReaderAcquiresAndReleasesRefCount() throws Exception {
        InputChannelInfo ch = new InputChannelInfo(0, 0);

        FetchedChannelState state;
        try (TestSpillWriter writer = new TestSpillWriter(tempDir)) {
            writer.writeRecord(ch, bytes(1), 1);
            state = writer.getChannelState();
        }

        Path spill = state.files().get(0);

        FetchedChannelStateReader reader = state.reader();
        // Drop the handoff grant so the reader's grant is the only one outstanding.
        state.release();
        assertThat(java.nio.file.Files.exists(spill)).isTrue();

        reader.close();

        // After closing the only reader, the file is cleaned up.
        assertThat(java.nio.file.Files.exists(spill)).isFalse();
    }

    @Test
    void testSnapshotKeepsFilesAliveUntilSnapshotClosed() throws Exception {
        InputChannelInfo ch = new InputChannelInfo(0, 0);

        FetchedChannelState state;
        try (TestSpillWriter writer = new TestSpillWriter(tempDir)) {
            writer.writeRecord(ch, bytes(1), 1);
            state = writer.getChannelState();
        }

        Path spill = state.files().get(0);

        FetchedChannelStateReader root = state.reader();
        FetchedChannelStateReader snap = root.snapshot().reader();
        // Drop the handoff grant so only the two reader grants remain outstanding.
        state.release();

        root.close(); // One grant released; file must still exist because snap holds another.
        assertThat(java.nio.file.Files.exists(spill)).isTrue();

        snap.close(); // Last grant released; file must be deleted.
        assertThat(java.nio.file.Files.exists(spill)).isFalse();
    }

    // -------------------------------------------------------------------------------------------
    // New behaviour: first-call exemption, fail-loud on body not consumed, empty reader
    // -------------------------------------------------------------------------------------------

    @Test
    void testFirstNextSegmentCallDoesNotRequirePreviousBodyConsumed() throws Exception {
        // The "previous body must be fully consumed" rule must not fire on the very first call
        // because there is no previous segment.
        FetchedChannelState state;
        try (TestSpillWriter writer = new TestSpillWriter(tempDir)) {
            writer.writeRecord(new InputChannelInfo(0, 0), bytes(1, 2), 2);
            state = writer.getChannelState();
        }

        try (FetchedChannelStateReader reader = state.reader()) {
            // Must not throw on the first call even though no body has been consumed before.
            Optional<SpillSegment> seg = reader.nextSegment();
            assertThat(seg).isPresent();
        }
    }

    @Test
    void testNextSegmentThrowsWhenPreviousBodyNotFullyConsumed() throws Exception {
        InputChannelInfo ch = new InputChannelInfo(0, 0);
        InputChannelInfo ch2 = new InputChannelInfo(0, 1);

        FetchedChannelState state;
        try (TestSpillWriter writer = new TestSpillWriter(tempDir)) {
            writer.writeRecord(ch, bytes(1, 2, 3, 4), 4);
            writer.writeRecord(ch2, bytes(5, 6), 2);
            state = writer.getChannelState();
        }

        try (FetchedChannelStateReader reader = state.reader()) {
            SpillSegment seg = reader.nextSegment().orElseThrow(AssertionError::new);
            // Read only part of the body — do not exhaust it.
            seg.bodyStream().read();

            // Advancing to the next segment while the previous body is not fully consumed must fail
            // loud.
            assertThatThrownBy(reader::nextSegment)
                    .isInstanceOf(IllegalStateException.class)
                    .hasMessageContaining("Previous segment body not fully consumed");
        }
    }

    @Test
    void testEmptyReaderNextSegmentReturnsEmptyAndCloseIsClean() throws Exception {
        FetchedChannelState state = new FetchedChannelState(Collections.emptyList());
        try (FetchedChannelStateReader reader = state.reader()) {
            // First call on an empty reader must return empty without throwing.
            assertThat(reader.nextSegment()).isEmpty();
            // Closing must not throw.
        }
    }

    @Test
    void testEmptyReaderHandsOutIndependentInstancesSoCloseDoesNotLeak() throws Exception {
        // emptyReader() is obtained and closed once per checkpoint. close() is single-use (it flips
        // the closed flag permanently), so each call must yield a fresh instance; otherwise the
        // first consumer's close would make every later consumer's nextSegment() fail loud.
        FetchedChannelStateReader first = FetchedChannelStateReader.emptyReader();
        assertThat(first.nextSegment()).isEmpty();
        first.close();

        FetchedChannelStateReader second = FetchedChannelStateReader.emptyReader();
        assertThat(second).isNotSameAs(first);
        // Must still work after the previously obtained empty reader was closed.
        assertThat(second.nextSegment()).isEmpty();
        second.close();
    }

    // -------------------------------------------------------------------------------------------
    // Helpers
    // -------------------------------------------------------------------------------------------

    private static byte[] readAll(InputStream in) throws IOException {
        java.io.ByteArrayOutputStream out = new java.io.ByteArrayOutputStream();
        byte[] buf = new byte[256];
        int n;
        while ((n = in.read(buf)) != -1) {
            out.write(buf, 0, n);
        }
        return out.toByteArray();
    }

    private static byte[] bytes(int... values) {
        byte[] arr = new byte[values.length];
        for (int i = 0; i < values.length; i++) {
            arr[i] = (byte) values[i];
        }
        return arr;
    }
}
