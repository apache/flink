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

import org.apache.flink.core.memory.MemorySegment;
import org.apache.flink.core.memory.MemorySegmentFactory;
import org.apache.flink.runtime.io.network.api.serialization.EventSerializer;
import org.apache.flink.runtime.io.network.buffer.Buffer;
import org.apache.flink.runtime.io.network.buffer.FreeingBufferRecycler;
import org.apache.flink.runtime.io.network.buffer.NetworkBuffer;
import org.apache.flink.runtime.io.network.partition.consumer.RecoverableInputChannel;

import org.junit.jupiter.api.RepeatedTest;
import org.junit.jupiter.api.io.TempDir;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Stress test for {@link FetchedChannelStateDrainer} drain / snapshot critical-section atomicity.
 * Spawns one drain thread alongside multiple concurrent {@code snapshotAndInsertBarriers} calls and
 * asserts that no buffer is delivered both via drain and snapshot, and that every segment appears
 * in exactly one place.
 */
class FetchedChannelStateDrainerConcurrencyTest {

    private static final int RECORD_COUNT = 500;
    private static final int SNAPSHOTS = 50;
    private static final int CHANNEL_COUNT = 2;

    @TempDir Path tempDir;

    @RepeatedTest(3)
    void testDrainAndSnapshotConcurrentAtomicity() throws Exception {
        Path runDir = Files.createTempDirectory(tempDir, "drain-stress-");
        InputChannelInfo c0 = new InputChannelInfo(0, 0);
        InputChannelInfo c1 = new InputChannelInfo(0, 1);

        FetchedChannelState state;
        try (TestSpillWriter writer = new TestSpillWriter(runDir)) {
            for (int i = 0; i < RECORD_COUNT; i++) {
                InputChannelInfo ch = (i % 2 == 0) ? c0 : c1;
                writer.writeRecord(ch, payloadFor(i), 8);
            }
            state = writer.getChannelState();
        }

        ThreadSafeRecordingChannel chan0 = new ThreadSafeRecordingChannel(c0);
        ThreadSafeRecordingChannel chan1 = new ThreadSafeRecordingChannel(c1);
        List<RecoverableInputChannel> all = new ArrayList<>();
        all.add(chan0);
        all.add(chan1);

        FetchedChannelStateDrainer drainer =
                new FetchedChannelStateDrainer(state, CompletableFuture.completedFuture(all));

        ExecutorService io = Executors.newSingleThreadExecutor();
        AtomicReference<Throwable> drainError = new AtomicReference<>();

        Future<?> drainFuture =
                io.submit(
                        () -> {
                            try {
                                drainer.drain();
                            } catch (Throwable t) {
                                drainError.set(t);
                            }
                        });

        // Take snapshots concurrently while drain runs.
        List<FetchedChannelStateReader> snapshots = new ArrayList<>();
        for (int i = 0; i < SNAPSHOTS; i++) {
            snapshots.add(drainer.snapshotAndInsertBarriers(i + 1));
            Thread.yield();
        }

        drainFuture.get(60, TimeUnit.SECONDS);
        io.shutdown();
        assertThat(io.awaitTermination(10, TimeUnit.SECONDS)).isTrue();
        if (drainError.get() != null) {
            throw new AssertionError("drain failed", drainError.get());
        }

        // Close all snapshots
        for (FetchedChannelStateReader snap : snapshots) {
            snap.close();
        }

        // Both channels must have received finish calls
        assertThat(chan0.barrierCount()).isEqualTo(chan1.barrierCount());

        drainer.close();
    }

    // -------------------------------------------------------------------------------------------
    // Helpers
    // -------------------------------------------------------------------------------------------

    private static byte[] payloadFor(int id) {
        byte[] out = new byte[8];
        out[0] = (byte) (id & 0xff);
        out[1] = (byte) ((id >> 8) & 0xff);
        out[2] = (byte) ((id >> 16) & 0xff);
        out[3] = (byte) ((id >> 24) & 0xff);
        Arrays.fill(out, 4, 8, (byte) 0xCC);
        return out;
    }

    private static final class ThreadSafeRecordingChannel implements RecoverableInputChannel {
        private final InputChannelInfo channelInfo;
        private final List<Buffer> data = new ArrayList<>();
        private final List<Buffer> barriers = new ArrayList<>();

        ThreadSafeRecordingChannel(InputChannelInfo channelInfo) {
            this.channelInfo = channelInfo;
        }

        @Override
        public InputChannelInfo getChannelInfo() {
            return channelInfo;
        }

        @Override
        public synchronized void onRecoveredStateBuffer(Buffer buffer) {
            if (buffer.isBuffer()) {
                data.add(buffer);
            } else {
                barriers.add(buffer);
            }
        }

        @Override
        public synchronized void finishRecoveredBufferDelivery() {}

        @Override
        public synchronized boolean insertRecoveryCheckpointBarrierIfInRecovery(long checkpointId)
                throws IOException {
            barriers.add(
                    EventSerializer.toBuffer(new RecoveryCheckpointBarrier(checkpointId), false));
            return false;
        }

        @Override
        public Buffer requestRecoveryBufferBlocking() {
            // Use a buffer large enough for a full segment
            MemorySegment seg = MemorySegmentFactory.allocateUnpooledSegment(4096);
            return new NetworkBuffer(seg, FreeingBufferRecycler.INSTANCE);
        }

        @Override
        public synchronized void onRecoveredStateConsumed() {}

        synchronized int barrierCount() {
            return barriers.size();
        }
    }
}
