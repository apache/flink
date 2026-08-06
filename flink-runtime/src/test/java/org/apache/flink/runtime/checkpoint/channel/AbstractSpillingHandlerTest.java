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

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.io.DataInputStream;
import java.io.FileInputStream;
import java.nio.file.Path;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Tests the on-disk spill format produced by {@link AbstractSpillingHandler}: the 12-byte segment
 * header with backfilled buffer length, channel switching, file rotation, and empty-segment
 * dropping. Records are appended through {@link TestSpillWriter}, mirroring how the filtering and
 * pass-through subclasses feed the segment buffer.
 */
class AbstractSpillingHandlerTest {

    @TempDir Path tempDir;

    @Test
    void testChannelSwitchProducesTwoSegmentsInOneFile() throws Exception {
        try (TestSpillWriter writer = new TestSpillWriter(tempDir)) {
            writer.writeRecord(new InputChannelInfo(0, 0), bytes(0xAA, 0xBB), 2);
            writer.writeRecord(new InputChannelInfo(0, 1), bytes(0xCC, 0xDD, 0xEE), 3);
            FetchedChannelState state = writer.getChannelState();

            assertThat(state.files()).hasSize(1);
            int seg0Body = Integer.BYTES + 2;
            int seg1Body = Integer.BYTES + 3;
            assertThat(state.files().get(0).toFile().length())
                    .isEqualTo(
                            (long) (AbstractSpillingHandler.SEGMENT_HEADER_BYTES + seg0Body)
                                    + (AbstractSpillingHandler.SEGMENT_HEADER_BYTES + seg1Body));
        }
    }

    @Test
    void testSameChannelContinuousRecordsMergeIntoOneSegment() throws Exception {
        InputChannelInfo ch = new InputChannelInfo(0, 0);
        try (TestSpillWriter writer = new TestSpillWriter(tempDir)) {
            writer.writeRecord(ch, bytes(1), 1);
            writer.writeRecord(ch, bytes(2, 3), 2);
            writer.writeRecord(ch, bytes(4, 5, 6), 3);
            FetchedChannelState state = writer.getChannelState();

            long expectedBody = 3L * Integer.BYTES + (1 + 2 + 3);
            assertThat(state.files()).hasSize(1);
            assertThat(state.files().get(0).toFile().length())
                    .isEqualTo(AbstractSpillingHandler.SEGMENT_HEADER_BYTES + expectedBody);
        }
    }

    @Test
    void testPassThroughBytesAreWrittenVerbatim() throws Exception {
        InputChannelInfo ch = new InputChannelInfo(1, 2);
        byte[] data = bytes(0x01, 0x02, 0x03, 0x04, 0x05);
        try (TestSpillWriter writer = new TestSpillWriter(tempDir)) {
            writer.writePassThrough(ch, data, 0, data.length);
            FetchedChannelState state = writer.getChannelState();

            assertThat(state.files()).hasSize(1);
            assertThat(state.files().get(0).toFile().length())
                    .isEqualTo(AbstractSpillingHandler.SEGMENT_HEADER_BYTES + data.length);
        }
    }

    // -------------------------------------------------------------------------------------------
    // Empty segments: opening a segment without writing a body must not create a file
    // -------------------------------------------------------------------------------------------

    @Test
    void testOpenSegmentWithoutBodyProducesNoFile() throws Exception {
        try (TestSpillWriter writer = new TestSpillWriter(tempDir)) {
            writer.openSegment(new InputChannelInfo(0, 0));
            // No body was ever spilled, so no state (and therefore no file) is produced.
            assertThat(writer.getChannelState()).isNull();
        }
    }

    @Test
    void testEmptySegmentsAroundANonEmptyOneProduceExactlyTheNonEmptyFile() throws Exception {
        try (TestSpillWriter writer = new TestSpillWriter(tempDir)) {
            writer.openSegment(new InputChannelInfo(0, 0));
            writer.writeRecord(new InputChannelInfo(0, 1), bytes(7, 8, 9), 3);
            writer.openSegment(new InputChannelInfo(0, 2));
            FetchedChannelState state = writer.getChannelState();

            assertThat(state.files()).hasSize(1);
            assertThat(state.files().get(0).toFile().length())
                    .isEqualTo(AbstractSpillingHandler.SEGMENT_HEADER_BYTES + Integer.BYTES + 3);
        }
    }

    // -------------------------------------------------------------------------------------------
    // File rotation
    // -------------------------------------------------------------------------------------------

    @Test
    void testRotationProducesOneFilePerSegmentWhenBoundIsTiny() throws Exception {
        try (TestSpillWriter writer = new TestSpillWriter(tempDir, 1L)) {
            writer.writeRecord(new InputChannelInfo(0, 0), bytes(1), 1);
            writer.writeRecord(new InputChannelInfo(0, 1), bytes(2, 3), 2);
            writer.writeRecord(new InputChannelInfo(0, 2), bytes(4), 1);
            FetchedChannelState state = writer.getChannelState();

            assertThat(state.files()).hasSize(3);
            for (Path file : state.files()) {
                assertThat(file.toFile().length()).isGreaterThan(0);
            }
        }
    }

    // -------------------------------------------------------------------------------------------
    // Disk-format verification
    // -------------------------------------------------------------------------------------------

    @Test
    void testDiskFormatMatchesSpec() throws Exception {
        try (TestSpillWriter writer = new TestSpillWriter(tempDir)) {
            writer.writeRecord(new InputChannelInfo(7, 3), bytes(0xAB, 0xCD, 0xEF), 3);
            Path file = writer.getChannelState().files().get(0);
            try (DataInputStream in = new DataInputStream(new FileInputStream(file.toFile()))) {
                assertThat(in.readInt()).isEqualTo(7); // gateIdx
                assertThat(in.readInt()).isEqualTo(3); // channelIdx
                assertThat(in.readInt()).isEqualTo(Integer.BYTES + 3); // bufferLength
                assertThat(in.readInt()).isEqualTo(3); // record length prefix
                assertThat(in.read()).isEqualTo(0xAB);
                assertThat(in.read()).isEqualTo(0xCD);
                assertThat(in.read()).isEqualTo(0xEF);
                assertThat(in.read()).isEqualTo(-1); // EOF
            }
        }
    }

    private static byte[] bytes(int... values) {
        byte[] out = new byte[values.length];
        for (int i = 0; i < values.length; i++) {
            out[i] = (byte) values[i];
        }
        return out;
    }
}
