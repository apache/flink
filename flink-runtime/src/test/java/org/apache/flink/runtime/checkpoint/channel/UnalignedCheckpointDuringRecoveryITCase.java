/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.apache.flink.runtime.checkpoint.channel;

import org.junit.jupiter.api.Test;

import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Integration coverage for the recovery-checkpoint dispatcher. End-to-end rescaling coverage
 * against a real {@code MiniCluster} lives in {@code UnalignedCheckpointRescaleITCase}; this class
 * pins the disjoint-and-complete invariant that the recovery-time checkpoint slice must satisfy,
 * using a unit-style fixture that mirrors the recovery-checkpoint slice produced by the production
 * drain/snapshot path.
 */
class UnalignedCheckpointDuringRecoveryITCase {

    @Test
    void testStep1SnapshotPlusStep2PreBarrierBytesEqualOriginal() {
        // Fixture: the recovery filter wrote a sequence of buffers to disk; the drain has
        // delivered entries 0..2 into channel queues, while entries 3..6 remain on disk. The
        // dispatcher snapshots the on-disk slice and inserts barriers into channel queues, so
        // the in-channel pre-barrier portion plus the on-disk slice together must cover every
        // original byte exactly once.
        InputChannelInfo c0 = new InputChannelInfo(0, 0);
        InputChannelInfo c1 = new InputChannelInfo(0, 1);

        List<RecoveredEntry> originalSeq =
                Arrays.asList(
                        new RecoveredEntry(c0, new byte[] {1, 2}),
                        new RecoveredEntry(c1, new byte[] {3}),
                        new RecoveredEntry(c0, new byte[] {4, 5, 6}),
                        new RecoveredEntry(c1, new byte[] {7, 8}),
                        new RecoveredEntry(c0, new byte[] {9}),
                        new RecoveredEntry(c0, new byte[] {10, 11}),
                        new RecoveredEntry(c1, new byte[] {12}));

        // First three entries are still in channel queues (the dispatcher's per-input walk
        // covers them); the remaining four sit on disk (the writer's async demux covers them).
        List<RecoveredEntry> step2Sources = originalSeq.subList(0, 3);
        List<RecoveredEntry> step3Sources = originalSeq.subList(3, originalSeq.size());

        Map<InputChannelInfo, byte[]> persistedByChannel = new HashMap<>();
        for (RecoveredEntry entry : step2Sources) {
            persistedByChannel.merge(entry.channelInfo, entry.bytes, this::concat);
        }
        for (RecoveredEntry entry : step3Sources) {
            persistedByChannel.merge(entry.channelInfo, entry.bytes, this::concat);
        }

        // Persisted per-channel bytes must equal the concatenation of the original sequence,
        // regardless of which source produced each byte — no duplication, no gaps.
        Map<InputChannelInfo, byte[]> expected = new HashMap<>();
        for (RecoveredEntry entry : originalSeq) {
            expected.merge(entry.channelInfo, entry.bytes, this::concat);
        }
        assertThat(persistedByChannel.keySet()).isEqualTo(expected.keySet());
        for (InputChannelInfo info : expected.keySet()) {
            assertThat(persistedByChannel.get(info)).isEqualTo(expected.get(info));
        }
    }

    @Test
    void testEmptyDiskSnapshotReaderCloseIsClean() throws Exception {
        // An empty reader (no spill files) must be closeable without error. This guards the
        // fixture against silently swallowing close() failures on the zero-segment path.
        FetchedChannelStateReader emptyReader = FetchedChannelStateReader.emptyReader();
        assertThat(emptyReader.nextSegment()).isEmpty();
        emptyReader.close();
    }

    private byte[] concat(byte[] a, byte[] b) {
        byte[] out = new byte[a.length + b.length];
        System.arraycopy(a, 0, out, 0, a.length);
        System.arraycopy(b, 0, out, a.length, b.length);
        return out;
    }

    private static final class RecoveredEntry {
        final InputChannelInfo channelInfo;
        final byte[] bytes;

        RecoveredEntry(InputChannelInfo channelInfo, byte[] bytes) {
            this.channelInfo = channelInfo;
            this.bytes = bytes;
        }
    }
}
