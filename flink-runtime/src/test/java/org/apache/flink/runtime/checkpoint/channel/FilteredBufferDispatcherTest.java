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

import org.apache.flink.core.memory.MemorySegmentFactory;
import org.apache.flink.runtime.io.network.buffer.Buffer;
import org.apache.flink.runtime.io.network.buffer.FreeingBufferRecycler;
import org.apache.flink.runtime.io.network.buffer.NetworkBuffer;
import org.apache.flink.runtime.io.network.partition.consumer.RecoveredBufferStoreImpl;
import org.apache.flink.util.CloseableIterator;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Queue;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.SynchronousQueue;
import java.util.concurrent.TimeUnit;
import java.util.stream.Stream;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;
import static org.junit.jupiter.api.Assertions.assertTimeoutPreemptively;

/** Tests for {@link FilteredBufferDispatcherImpl}. */
class FilteredBufferDispatcherTest {

    private static final int SEGMENT_SIZE = 64;

    @TempDir Path tempDir;

    private InputChannelInfo ch0;
    private InputChannelInfo ch1;
    private RecoveredBufferStoreImpl store0;
    private RecoveredBufferStoreImpl store1;
    private Map<InputChannelInfo, RecoveredBufferStoreImpl> stores;
    private String[] spillDirs;

    @BeforeEach
    void setUp() {
        ch0 = new InputChannelInfo(0, 0);
        ch1 = new InputChannelInfo(0, 1);
        store0 = new RecoveredBufferStoreImpl(ch0);
        store1 = new RecoveredBufferStoreImpl(ch1);
        stores = new HashMap<>();
        stores.put(ch0, store0);
        stores.put(ch1, store1);
        spillDirs = new String[] {tempDir.toString()};
    }

    @AfterEach
    void tearDown() {}

    /** Records setCoordinator calls without Mockito. */
    private static class TrackingBufferStore extends RecoveredBufferStoreImpl {
        private RecoveredBufferStoreCoordinator registeredCoordinator;
        private int setCoordinatorCount = 0;

        TrackingBufferStore(InputChannelInfo channelInfo) {
            super(channelInfo);
        }

        @Override
        public synchronized void setCoordinator(RecoveredBufferStoreCoordinator coordinator) {
            super.setCoordinator(coordinator);
            this.registeredCoordinator = coordinator;
            this.setCoordinatorCount++;
        }
    }

    private Buffer createBuffer() {
        return new NetworkBuffer(
                MemorySegmentFactory.allocateUnpooledSegment(SEGMENT_SIZE),
                FreeingBufferRecycler.INSTANCE);
    }

    private Queue<Buffer> createBufferPool(int count) {
        Queue<Buffer> pool = new LinkedList<>();
        for (int i = 0; i < count; i++) {
            pool.add(createBuffer());
        }
        return pool;
    }

    private byte[] createTestData(int length, byte fillValue) {
        byte[] data = new byte[length];
        Arrays.fill(data, fillValue);
        return data;
    }

    private List<byte[]> drainStore(RecoveredBufferStoreImpl store) {
        List<byte[]> result = new ArrayList<>();
        while (true) {
            Buffer buf;
            synchronized (store) {
                buf = store.tryTake();
            }
            if (buf == null) {
                break;
            }
            byte[] data = new byte[buf.getSize()];
            buf.getMemorySegment().get(0, data, 0, buf.getSize());
            buf.recycleBuffer();
            result.add(data);
        }
        return result;
    }

    private byte[] concat(List<byte[]> chunks) {
        int totalLen = chunks.stream().mapToInt(a -> a.length).sum();
        byte[] result = new byte[totalLen];
        int pos = 0;
        for (byte[] chunk : chunks) {
            System.arraycopy(chunk, 0, result, pos, chunk.length);
            pos += chunk.length;
        }
        return result;
    }

    /** Buffer always available, no disk. All data flows to stores via buffers. */
    @Test
    void testP1MemoryPath() throws Exception {
        Queue<Buffer> pool = createBufferPool(10);
        FilteredBufferDispatcherImpl writer =
                new FilteredBufferDispatcherImpl(
                        stores,
                        ChannelStateWriter.NO_OP,
                        spillDirs,
                        SEGMENT_SIZE,
                        new TestBufferPool(pool));

        byte[] data = createTestData(SEGMENT_SIZE, (byte) 0xAA);
        writer.write(data, data.length, ch0);
        writer.flush();
        writer.close();

        List<byte[]> buffers = drainStore(store0);
        byte[] actual = concat(buffers);
        assertThat(actual).isEqualTo(data);
        assertEmpty(store0);
    }

    /** Buffer supplier always returns null. Data goes to disk, replayed on close. */
    @Test
    void testP2SpillPath() throws Exception {
        Queue<Buffer> drainPool = createBufferPool(5);
        FilteredBufferDispatcherImpl writer =
                new FilteredBufferDispatcherImpl(
                        stores,
                        ChannelStateWriter.NO_OP,
                        spillDirs,
                        SEGMENT_SIZE,
                        TestBufferPool.drainOnly(drainPool));

        byte[] data = createTestData(SEGMENT_SIZE, (byte) 0xBB);
        writer.write(data, data.length, ch0);
        writer.flush();

        assertThat(tryTake(store0)).isNull();

        writer.drainPendingSpill();
        writer.close();

        List<byte[]> buffers = drainStore(store0);
        byte[] actual = concat(buffers);
        assertThat(actual).isEqualTo(data);
        assertEmpty(store0);
    }

    /** First write spills, then buffer becomes available. P3 replays from disk. */
    @Test
    void testP3ReplayPath() throws Exception {
        Queue<Buffer> pool = new LinkedList<>();
        FilteredBufferDispatcherImpl writer =
                new FilteredBufferDispatcherImpl(
                        stores,
                        ChannelStateWriter.NO_OP,
                        spillDirs,
                        SEGMENT_SIZE,
                        new TestBufferPool(pool));

        byte[] data1 = createTestData(SEGMENT_SIZE, (byte) 0x11);
        writer.write(data1, data1.length, ch0);

        // Buffers added — next write triggers P3 eager drain.
        pool.addAll(createBufferPool(5));

        byte[] data2 = createTestData(SEGMENT_SIZE, (byte) 0x22);
        writer.write(data2, data2.length, ch1);
        writer.flush();
        writer.drainPendingSpill();
        writer.close();

        List<byte[]> buf0 = drainStore(store0);
        assertThat(concat(buf0)).isEqualTo(data1);

        List<byte[]> buf1 = drainStore(store1);
        assertThat(concat(buf1)).isEqualTo(data2);
    }

    /**
     * Multiple channels' data goes to disk. Replay order matches FIFO write order across channels.
     */
    @Test
    void testP3FIFOOrdering() throws Exception {
        Queue<Buffer> drainPool = createBufferPool(10);
        FilteredBufferDispatcherImpl writer =
                new FilteredBufferDispatcherImpl(
                        stores,
                        ChannelStateWriter.NO_OP,
                        spillDirs,
                        SEGMENT_SIZE,
                        TestBufferPool.drainOnly(drainPool));

        byte[] d0 = createTestData(SEGMENT_SIZE, (byte) 0x10);
        byte[] d1 = createTestData(SEGMENT_SIZE, (byte) 0x20);
        writer.write(d0, d0.length, ch0);
        writer.write(d1, d1.length, ch1);
        writer.flush();
        writer.drainPendingSpill();
        writer.close();

        assertThat(concat(drainStore(store0))).isEqualTo(d0);
        assertThat(concat(drainStore(store1))).isEqualTo(d1);
    }

    /**
     * Multiple entries on disk, multiple buffers available. P3 loops until no buffer or disk empty.
     */
    @Test
    void testP3EagerDrain() throws Exception {
        Queue<Buffer> pool = new LinkedList<>();
        FilteredBufferDispatcherImpl writer =
                new FilteredBufferDispatcherImpl(
                        stores,
                        ChannelStateWriter.NO_OP,
                        spillDirs,
                        SEGMENT_SIZE,
                        new TestBufferPool(pool));

        for (int i = 0; i < 3; i++) {
            byte[] d = createTestData(SEGMENT_SIZE, (byte) (0x30 + i));
            writer.write(d, d.length, ch0);
        }

        pool.addAll(createBufferPool(10));

        byte[] d3 = createTestData(SEGMENT_SIZE, (byte) 0x40);
        writer.write(d3, d3.length, ch1);
        writer.flush();
        writer.drainPendingSpill();
        writer.close();

        List<byte[]> results = drainStore(store0);
        assertThat(results).hasSize(3);
        for (int i = 0; i < 3; i++) {
            assertThat(results.get(i)).isEqualTo(createTestData(SEGMENT_SIZE, (byte) (0x30 + i)));
        }

        assertThat(concat(drainStore(store1))).isEqualTo(d3);
    }

    /**
     * Start with buffer, buffer fills, no new buffer available. Remaining data goes to file. Cannot
     * upgrade back to buffer within one writeToBackend call.
     */
    @Test
    void testBackendDowngradeOnly() throws Exception {
        Queue<Buffer> pool = createBufferPool(1);
        Queue<Buffer> drainPool = createBufferPool(5);
        FilteredBufferDispatcherImpl writer =
                new FilteredBufferDispatcherImpl(
                        stores,
                        ChannelStateWriter.NO_OP,
                        spillDirs,
                        SEGMENT_SIZE,
                        new TestBufferPool(pool, drainPool));

        // First SEGMENT_SIZE goes to buffer, rest to disk.
        byte[] data = createTestData(SEGMENT_SIZE * 2, (byte) 0x44);
        writer.write(data, data.length, ch0);
        writer.flush();
        writer.drainPendingSpill();
        writer.close();

        List<byte[]> results = drainStore(store0);
        byte[] actual = concat(results);
        assertThat(actual).isEqualTo(data);
    }

    /** Data starts in buffer, spans to file when buffer full. */
    @Test
    void testCrossBackendRecordSpanning() throws Exception {
        Queue<Buffer> pool = createBufferPool(1);
        Queue<Buffer> drainPool = createBufferPool(5);
        FilteredBufferDispatcherImpl writer =
                new FilteredBufferDispatcherImpl(
                        stores,
                        ChannelStateWriter.NO_OP,
                        spillDirs,
                        SEGMENT_SIZE,
                        new TestBufferPool(pool, drainPool));

        byte[] part1 = createTestData(SEGMENT_SIZE / 2, (byte) 0x55);
        writer.write(part1, part1.length, ch0);

        byte[] part2 = createTestData(SEGMENT_SIZE, (byte) 0x66);
        writer.write(part2, part2.length, ch0);

        writer.flush();
        writer.drainPendingSpill();
        writer.close();

        List<byte[]> results = drainStore(store0);
        byte[] actual = concat(results);
        byte[] expected = new byte[part1.length + part2.length];
        System.arraycopy(part1, 0, expected, 0, part1.length);
        System.arraycopy(part2, 0, expected, part1.length, part2.length);
        assertThat(actual).isEqualTo(expected);
    }

    /** Write to channel A, then B. Verify flush between transitions. */
    @Test
    void testChannelChangeDetection() throws Exception {
        Queue<Buffer> pool = createBufferPool(10);
        FilteredBufferDispatcherImpl writer =
                new FilteredBufferDispatcherImpl(
                        stores,
                        ChannelStateWriter.NO_OP,
                        spillDirs,
                        SEGMENT_SIZE,
                        new TestBufferPool(pool));

        byte[] d0 = createTestData(SEGMENT_SIZE / 2, (byte) 0x77);
        writer.write(d0, d0.length, ch0);

        // Channel switch should flush ch0's partial buffer.
        byte[] d1 = createTestData(SEGMENT_SIZE / 2, (byte) 0x88);
        writer.write(d1, d1.length, ch1);

        writer.flush();
        writer.close();

        List<byte[]> results0 = drainStore(store0);
        assertThat(concat(results0)).isEqualTo(d0);

        List<byte[]> results1 = drainStore(store1);
        assertThat(concat(results1)).isEqualTo(d1);
    }

    /** Multiple channels share one spill file. */
    @Test
    void testSingleFilePerTask() throws Exception {
        Queue<Buffer> drainPool = createBufferPool(10);
        FilteredBufferDispatcherImpl writer =
                new FilteredBufferDispatcherImpl(
                        stores,
                        ChannelStateWriter.NO_OP,
                        spillDirs,
                        SEGMENT_SIZE,
                        TestBufferPool.drainOnly(drainPool));

        byte[] d0 = createTestData(SEGMENT_SIZE, (byte) 0x99);
        byte[] d1 = createTestData(SEGMENT_SIZE, (byte) 0xAA);
        writer.write(d0, d0.length, ch0);
        writer.write(d1, d1.length, ch1);
        writer.flush();
        writer.drainPendingSpill();
        writer.close();

        assertThat(concat(drainStore(store0))).isEqualTo(d0);
        assertThat(concat(drainStore(store1))).isEqualTo(d1);
    }

    /** Spill, partial replay, verify tracking state. */
    @Test
    void testCursorBasedTracking() throws Exception {
        Queue<Buffer> pool = new LinkedList<>();
        FilteredBufferDispatcherImpl writer =
                new FilteredBufferDispatcherImpl(
                        stores,
                        ChannelStateWriter.NO_OP,
                        spillDirs,
                        SEGMENT_SIZE,
                        new TestBufferPool(pool));

        byte[] d1 = createTestData(SEGMENT_SIZE, (byte) 0x01);
        byte[] d2 = createTestData(SEGMENT_SIZE, (byte) 0x02);
        writer.write(d1, d1.length, ch0);
        writer.write(d2, d2.length, ch0);

        // Only 1 buffer — partial replay: only 1 of 2 entries drains.
        pool.add(createBuffer());

        byte[] d3 = createTestData(SEGMENT_SIZE, (byte) 0x03);
        writer.write(d3, d3.length, ch1);

        pool.addAll(createBufferPool(5));
        writer.flush();
        writer.drainPendingSpill();
        writer.close();

        List<byte[]> results0 = drainStore(store0);
        assertThat(results0).hasSize(2);
        assertThat(results0.get(0)).isEqualTo(d1);
        assertThat(results0.get(1)).isEqualTo(d2);
    }

    /** drainPendingSpill() drains all disk data; close() is then a no-op resource release. */
    @Test
    void testDrainPendingSpillUntilEmpty() throws Exception {
        Queue<Buffer> drainPool = createBufferPool(10);
        FilteredBufferDispatcherImpl writer =
                new FilteredBufferDispatcherImpl(
                        stores,
                        ChannelStateWriter.NO_OP,
                        spillDirs,
                        SEGMENT_SIZE,
                        TestBufferPool.drainOnly(drainPool));

        for (int i = 0; i < 5; i++) {
            byte[] data = createTestData(SEGMENT_SIZE, (byte) i);
            writer.write(data, data.length, ch0);
        }
        writer.flush();

        assertThat(tryTake(store0)).isNull();

        writer.drainPendingSpill();

        List<byte[]> results = drainStore(store0);
        assertThat(results).hasSize(5);
        for (int i = 0; i < 5; i++) {
            assertThat(results.get(i)).isEqualTo(createTestData(SEGMENT_SIZE, (byte) i));
        }

        writer.close();
        assertThat(tryTake(store0)).isNull();
    }

    /** close() twice doesn't throw; drainPendingSpill() after close() is a no-op. */
    @Test
    void testCloseIdempotent() throws Exception {
        Queue<Buffer> pool = createBufferPool(5);
        FilteredBufferDispatcherImpl writer =
                new FilteredBufferDispatcherImpl(
                        stores,
                        ChannelStateWriter.NO_OP,
                        spillDirs,
                        SEGMENT_SIZE,
                        new TestBufferPool(pool));

        writer.flush();
        writer.drainPendingSpill();
        writer.close();
        writer.close();
    }

    /** After close(), spill files deleted. */
    @Test
    void testCloseCleanup() throws Exception {
        Queue<Buffer> drainPool = createBufferPool(10);
        FilteredBufferDispatcherImpl writer =
                new FilteredBufferDispatcherImpl(
                        stores,
                        ChannelStateWriter.NO_OP,
                        spillDirs,
                        SEGMENT_SIZE,
                        TestBufferPool.drainOnly(drainPool));

        byte[] data = createTestData(SEGMENT_SIZE * 3, (byte) 0xCC);
        writer.write(data, data.length, ch0);
        writer.flush();

        try (Stream<Path> files =
                Files.list(tempDir).filter(p -> p.getFileName().toString().startsWith("spill-"))) {
            assertThat(files.count()).isGreaterThan(0);
        }

        writer.drainPendingSpill();
        writer.close();

        try (Stream<Path> files =
                Files.list(tempDir).filter(p -> p.getFileName().toString().startsWith("spill-"))) {
            assertThat(files.count()).isEqualTo(0);
        }
    }

    /** write() after close() throws IllegalStateException. */
    @Test
    void testWriteAfterClose() throws Exception {
        Queue<Buffer> pool = createBufferPool(5);
        FilteredBufferDispatcherImpl writer =
                new FilteredBufferDispatcherImpl(
                        stores,
                        ChannelStateWriter.NO_OP,
                        spillDirs,
                        SEGMENT_SIZE,
                        new TestBufferPool(pool));

        writer.flush();
        writer.drainPendingSpill();
        writer.close();

        byte[] data = createTestData(10, (byte) 0xDD);
        assertThatThrownBy(() -> writer.write(data, data.length, ch0))
                .isInstanceOf(IllegalStateException.class);
    }

    /** write() after flush() throws IllegalStateException. */
    @Test
    void testWriteAfterFlush() throws Exception {
        Queue<Buffer> pool = createBufferPool(5);
        FilteredBufferDispatcherImpl writer =
                new FilteredBufferDispatcherImpl(
                        stores,
                        ChannelStateWriter.NO_OP,
                        spillDirs,
                        SEGMENT_SIZE,
                        new TestBufferPool(pool));

        writer.flush();

        byte[] data = createTestData(10, (byte) 0xEE);
        assertThatThrownBy(() -> writer.write(data, data.length, ch0))
                .isInstanceOf(IllegalStateException.class);
    }

    /** Empty spill dirs throws IOException. */
    @Test
    void testSpillDirectorySource() {
        assertThatThrownBy(
                        () ->
                                new FilteredBufferDispatcherImpl(
                                        stores,
                                        ChannelStateWriter.NO_OP,
                                        new String[0],
                                        SEGMENT_SIZE,
                                        TestBufferPool.empty()))
                .isInstanceOf(IOException.class);
    }

    /** Enough data for many spill entries. All replayed correctly. */
    @Test
    void testLargeDataMultiRotation() throws Exception {
        // FilteredSpillFile rotates at 64MB; truly testing rotation needs >192MB which is too
        // much for a unit test. Verify the mechanism with many entries instead.
        int entryCount = 100;
        int segmentSize = 256;
        Queue<Buffer> drainPool = new LinkedList<>();
        for (int i = 0; i < entryCount + 10; i++) {
            drainPool.add(
                    new NetworkBuffer(
                            MemorySegmentFactory.allocateUnpooledSegment(segmentSize),
                            FreeingBufferRecycler.INSTANCE));
        }

        String[] dirs = new String[] {tempDir.toString()};
        FilteredBufferDispatcherImpl writer =
                new FilteredBufferDispatcherImpl(
                        stores,
                        ChannelStateWriter.NO_OP,
                        dirs,
                        segmentSize,
                        TestBufferPool.drainOnly(drainPool));

        byte[][] expectedData = new byte[entryCount][];
        for (int i = 0; i < entryCount; i++) {
            expectedData[i] = new byte[segmentSize];
            Arrays.fill(expectedData[i], (byte) (i & 0xFF));
            writer.write(expectedData[i], expectedData[i].length, ch0);
        }
        writer.flush();
        writer.drainPendingSpill();
        writer.close();

        List<byte[]> results = drainStore(store0);
        assertThat(results).hasSize(entryCount);
        for (int i = 0; i < entryCount; i++) {
            assertThat(results.get(i)).isEqualTo(expectedData[i]);
        }
    }

    /** FilteredBufferDispatcher.write(data, length, channelInfo) is the unified write interface. */
    @Test
    void testUnifiedWriteInterface() throws Exception {
        Queue<Buffer> pool = createBufferPool(10);
        FilteredBufferDispatcherImpl writer =
                new FilteredBufferDispatcherImpl(
                        stores,
                        ChannelStateWriter.NO_OP,
                        spillDirs,
                        SEGMENT_SIZE,
                        new TestBufferPool(pool));

        byte[] d0 = createTestData(32, (byte) 0xF0);
        byte[] d1 = createTestData(32, (byte) 0xF1);

        writer.write(d0, d0.length, ch0);
        writer.write(d1, d1.length, ch1);
        writer.flush();
        writer.close();

        assertThat(concat(drainStore(store0))).isEqualTo(d0);
        assertThat(concat(drainStore(store1))).isEqualTo(d1);
    }

    /** SpillEntry aligns with memorySegmentSize, 1:1 with buffer. */
    @Test
    void testBufferAlignedEntryReplay() throws Exception {
        Queue<Buffer> drainPool = createBufferPool(5);
        FilteredBufferDispatcherImpl writer =
                new FilteredBufferDispatcherImpl(
                        stores,
                        ChannelStateWriter.NO_OP,
                        spillDirs,
                        SEGMENT_SIZE,
                        TestBufferPool.drainOnly(drainPool));

        // 3 * SEGMENT_SIZE bytes → exactly 3 spill entries, 1:1 with buffers.
        for (int i = 0; i < 3; i++) {
            byte[] data = createTestData(SEGMENT_SIZE, (byte) (0xA0 + i));
            writer.write(data, data.length, ch0);
        }
        writer.flush();
        writer.drainPendingSpill();
        writer.close();

        List<byte[]> results = drainStore(store0);
        assertThat(results).hasSize(3);
        for (int i = 0; i < 3; i++) {
            assertThat(results.get(i)).hasSize(SEGMENT_SIZE);
            assertThat(results.get(i)).isEqualTo(createTestData(SEGMENT_SIZE, (byte) (0xA0 + i)));
        }
    }

    /**
     * After construction, each store has its coordinator registered to the
     * FilteredBufferDispatcherImpl instance.
     */
    @Test
    void testCoordinatorRegisteredOnConstruction() throws Exception {
        TrackingBufferStore trackStore0 = new TrackingBufferStore(ch0);
        TrackingBufferStore trackStore1 = new TrackingBufferStore(ch1);
        Map<InputChannelInfo, RecoveredBufferStoreImpl> trackStores = new HashMap<>();
        trackStores.put(ch0, trackStore0);
        trackStores.put(ch1, trackStore1);

        Queue<Buffer> pool = createBufferPool(5);
        FilteredBufferDispatcherImpl writer =
                new FilteredBufferDispatcherImpl(
                        trackStores,
                        ChannelStateWriter.NO_OP,
                        spillDirs,
                        SEGMENT_SIZE,
                        new TestBufferPool(pool));

        assertThat(trackStore0.setCoordinatorCount).isEqualTo(1);
        assertThat(trackStore0.registeredCoordinator).isSameAs(writer);
        assertThat(trackStore1.setCoordinatorCount).isEqualTo(1);
        assertThat(trackStore1.registeredCoordinator).isSameAs(writer);
    }

    /**
     * First onChannelCheckpointStarted call for a checkpointId scans spillEntryQueue and builds the
     * correct wait-set; subsequent calls for the same checkpointId remove channels.
     */
    @Test
    void testWaitSetBuiltOnFirstCallback() throws Exception {
        // Spill one entry per channel so both appear in the wait-set.
        Queue<Buffer> drainPool = createBufferPool(10);
        FilteredBufferDispatcherImpl writer =
                new FilteredBufferDispatcherImpl(
                        stores,
                        ChannelStateWriter.NO_OP,
                        spillDirs,
                        SEGMENT_SIZE,
                        TestBufferPool.drainOnly(drainPool));

        byte[] d0 = createTestData(SEGMENT_SIZE, (byte) 0x10);
        byte[] d1 = createTestData(SEGMENT_SIZE, (byte) 0x20);
        writer.write(d0, d0.length, ch0);
        writer.write(d1, d1.length, ch1);
        writer.flush();

        // After flush: 2 spill entries in queue (ch0, ch1).
        // First callback for checkpoint 1: wait-set = {ch0, ch1}; then ch0 is removed.
        writer.onChannelCheckpointStarted(1L, ch0, writer.getCurrentDrainHead());
        // Second callback for same checkpoint: ch1 is removed → wait-set is now empty.
        writer.onChannelCheckpointStarted(1L, ch1, writer.getCurrentDrainHead());
        // No exception; wait-set reached empty — state machine operated correctly.

        writer.drainPendingSpill();
        writer.close();
    }

    /** New checkpointId causes wait-set to be rebuilt from current spillEntryQueue. */
    @Test
    void testWaitSetRebuiltOnNewCheckpointId() throws Exception {
        Queue<Buffer> drainPool = createBufferPool(10);
        FilteredBufferDispatcherImpl writer =
                new FilteredBufferDispatcherImpl(
                        stores,
                        ChannelStateWriter.NO_OP,
                        spillDirs,
                        SEGMENT_SIZE,
                        TestBufferPool.drainOnly(drainPool));

        // Spill entries for both channels.
        writer.write(createTestData(SEGMENT_SIZE, (byte) 0x30), SEGMENT_SIZE, ch0);
        writer.write(createTestData(SEGMENT_SIZE, (byte) 0x40), SEGMENT_SIZE, ch1);
        writer.flush();

        // Checkpoint 1: consume both callbacks (wait-set empties).
        writer.onChannelCheckpointStarted(1L, ch0, writer.getCurrentDrainHead());
        writer.onChannelCheckpointStarted(1L, ch1, writer.getCurrentDrainHead());

        // Checkpoint 2: new id → wait-set should be rebuilt from the *remaining* spillEntryQueue.
        // At this point the queue still holds the 2 entries (they are drained only on close).
        // Both channels should again appear in the wait-set.
        writer.onChannelCheckpointStarted(2L, ch0, writer.getCurrentDrainHead());
        writer.onChannelCheckpointStarted(2L, ch1, writer.getCurrentDrainHead());
        // No exception; both rebuilt and removed successfully.

        writer.drainPendingSpill();
        writer.close();
    }

    /**
     * Channel not present in spillEntryQueue is not in wait-set; removing it is a no-op. Uses a
     * fresh store map so only ch0 has a spill entry; ch1 callback is a no-op.
     */
    @Test
    void testCallbackForChannelWithNoPendingEntryIsNoOp() throws Exception {
        // Use a fresh store map to avoid interference from previous tests
        RecoveredBufferStoreImpl freshStore0 = new RecoveredBufferStoreImpl(ch0);
        RecoveredBufferStoreImpl freshStore1 = new RecoveredBufferStoreImpl(ch1);
        Map<InputChannelInfo, RecoveredBufferStoreImpl> freshStores = new HashMap<>();
        freshStores.put(ch0, freshStore0);
        freshStores.put(ch1, freshStore1);

        Queue<Buffer> drainPool = createBufferPool(5);
        FilteredBufferDispatcherImpl writer =
                new FilteredBufferDispatcherImpl(
                        freshStores,
                        ChannelStateWriter.NO_OP,
                        spillDirs,
                        SEGMENT_SIZE,
                        TestBufferPool.drainOnly(drainPool));

        // Only ch0 spills; ch1 has no entries in the queue.
        writer.write(createTestData(SEGMENT_SIZE, (byte) 0x50), SEGMENT_SIZE, ch0);
        writer.flush();

        // ch1 callback: not in wait-set → no-op remove, wait-set stays non-empty.
        writer.onChannelCheckpointStarted(42L, ch1, writer.getCurrentDrainHead());
        // ch0 callback: removed from wait-set → empty.
        writer.onChannelCheckpointStarted(42L, ch0, writer.getCurrentDrainHead());

        writer.drainPendingSpill();
        writer.close();
    }

    /**
     * Duplicate callback for the same channel in the same checkpoint is idempotent (Set.remove on
     * an already-absent element is a no-op).
     */
    @Test
    void testDuplicateCallbackForSameChannelIsIdempotent() throws Exception {
        Queue<Buffer> drainPool = createBufferPool(5);
        FilteredBufferDispatcherImpl writer =
                new FilteredBufferDispatcherImpl(
                        stores,
                        ChannelStateWriter.NO_OP,
                        spillDirs,
                        SEGMENT_SIZE,
                        TestBufferPool.drainOnly(drainPool));

        writer.write(createTestData(SEGMENT_SIZE, (byte) 0x70), SEGMENT_SIZE, ch0);
        writer.flush();

        // ch0 removed on first call; second call is a no-op (already absent from set).
        writer.onChannelCheckpointStarted(10L, ch0, writer.getCurrentDrainHead());
        writer.onChannelCheckpointStarted(
                10L, ch0, writer.getCurrentDrainHead()); // idempotent — no exception

        writer.drainPendingSpill();
        writer.close();
    }

    /**
     * A stale callback (checkpointId &lt; currentCheckpointId) must be ignored: it must not modify
     * the current checkpoint's wait-set and must not trigger phase 2. We verify this by observing
     * that after the stale callback arrives, the current checkpoint still converges normally when
     * its own callbacks arrive.
     */
    @Test
    void testStaleCheckpointCallbackIsIgnored() throws Exception {
        Queue<Buffer> drainPool = createBufferPool(5);
        RecordingChannelStateWriter recordingWriter = new RecordingChannelStateWriter();
        FilteredBufferDispatcherImpl writer =
                new FilteredBufferDispatcherImpl(
                        stores,
                        recordingWriter,
                        spillDirs,
                        SEGMENT_SIZE,
                        TestBufferPool.drainOnly(drainPool));

        // Two entries so both channels appear in the wait-set.
        writer.write(createTestData(SEGMENT_SIZE, (byte) 0xA1), SEGMENT_SIZE, ch0);
        writer.write(createTestData(SEGMENT_SIZE, (byte) 0xA2), SEGMENT_SIZE, ch1);
        writer.flush();

        // Move to a newer checkpoint (id=20) and deliver one of its two channel callbacks.
        writer.onChannelCheckpointStarted(20L, ch0, writer.getCurrentDrainHead());
        // wait-set still contains ch1; phase 2 not yet triggered.
        assertThat(recordingWriter.inputDataCalls).isEmpty();

        // A stale callback for an older checkpoint (id=10) arrives. It must be ignored — not
        // alter the wait-set for checkpoint 20, and must not trigger phase 2.
        writer.onChannelCheckpointStarted(10L, ch0, writer.getCurrentDrainHead());
        writer.onChannelCheckpointStarted(10L, ch1, writer.getCurrentDrainHead());
        assertThat(recordingWriter.inputDataCalls).isEmpty();

        // Now deliver the remaining callback for checkpoint 20 — wait-set empties and phase 2
        // snapshots the entries into the ChannelStateWriter.
        writer.onChannelCheckpointStarted(20L, ch1, writer.getCurrentDrainHead());
        assertThat(recordingWriter.inputDataCalls).hasSize(2);

        writer.drainPendingSpill();
        writer.close();
    }

    /**
     * wait-set reaching empty triggers phase2 {@code drainSpillEntriesToCheckpoint}: the sealed
     * readers are snapshotted and streamed to the ChannelStateWriter, but the original readers and
     * store state are left intact. drainPendingSpill() then still delivers every entry to the store
     * via network buffers.
     */
    @Test
    void testWaitSetEmptyTriggersPhase2SnapshotThenDrainDeliversBuffers() throws Exception {
        Queue<Buffer> drainPool = createBufferPool(5);
        FilteredBufferDispatcherImpl writer =
                new FilteredBufferDispatcherImpl(
                        stores,
                        ChannelStateWriter.NO_OP,
                        spillDirs,
                        SEGMENT_SIZE,
                        TestBufferPool.drainOnly(drainPool));

        byte[] payload = createTestData(SEGMENT_SIZE, (byte) 0x80);
        writer.write(payload, SEGMENT_SIZE, ch0);
        writer.flush();

        // phase2: snapshots sealed readers into the ChannelStateWriter (NO_OP here); the original
        // reader and store state are untouched.
        writer.onChannelCheckpointStarted(99L, ch0, writer.getCurrentDrainHead());

        // drainPendingSpill() consumes the original reader and delivers buffers to the store.
        writer.drainPendingSpill();
        writer.close();

        Buffer delivered = tryTake(store0);
        assertThat(delivered).isNotNull();
        byte[] actual = new byte[delivered.getSize()];
        delivered.getMemorySegment().get(0, actual, 0, delivered.getSize());
        delivered.recycleBuffer();
        assertThat(actual).isEqualTo(payload);
    }

    /**
     * Captures addInputDataFromSpill calls for assertions; drains the chunk iterator synchronously
     * so tests can assert on actual bytes and channel info.
     */
    private static class RecordingChannelStateWriter
            extends ChannelStateWriter.NoOpChannelStateWriter {

        static class Call {
            final long checkpointId;
            final InputChannelInfo info;
            final int dataLength;
            final byte[] capturedBytes;

            Call(long checkpointId, InputChannelInfo info, int dataLength, byte[] capturedBytes) {
                this.checkpointId = checkpointId;
                this.info = info;
                this.dataLength = dataLength;
                this.capturedBytes = capturedBytes;
            }
        }

        final List<Call> inputDataCalls = new ArrayList<>();

        @Override
        public void addInputDataFromSpill(
                long checkpointId, CloseableIterator<FilteredSpillFile.Chunk> chunks) {
            try {
                while (chunks.hasNext()) {
                    FilteredSpillFile.Chunk chunk = chunks.next();
                    byte[] bytes = new byte[chunk.getLength()];
                    System.arraycopy(chunk.getData(), 0, bytes, 0, chunk.getLength());
                    inputDataCalls.add(
                            new Call(
                                    checkpointId,
                                    chunk.getChannelInfo(),
                                    chunk.getLength(),
                                    bytes));
                }
                chunks.close();
            } catch (Exception e) {
                throw new RuntimeException(e);
            }
        }
    }

    /**
     * phase2 writes all spill entries to ChannelStateWriter via streaming addInputData. Verifies
     * checkpointId, channelInfo, seqNum=SEQUENCE_NUMBER_RESTORED, and byte content.
     */
    @Test
    void testPhase2WritesDiskDataThroughStreamingApi() throws Exception {
        RecordingChannelStateWriter recordingWriter = new RecordingChannelStateWriter();

        // drainOnly: no buffers for the write path (forces everything to spill), but the blocking
        // drain path gets buffers so close()'s drain loop can still deliver the snapshotted entries
        // to the stores. phase 2 is a backup — close() drain is the task-facing delivery path.
        Queue<Buffer> drainPool = createBufferPool(5);
        FilteredBufferDispatcherImpl writer =
                new FilteredBufferDispatcherImpl(
                        stores,
                        recordingWriter,
                        spillDirs,
                        SEGMENT_SIZE,
                        TestBufferPool.drainOnly(drainPool));

        byte[] d0 = createTestData(SEGMENT_SIZE, (byte) 0xA1);
        byte[] d1 = createTestData(SEGMENT_SIZE, (byte) 0xA2);
        writer.write(d0, d0.length, ch0);
        writer.write(d1, d1.length, ch1);
        writer.flush();

        // Trigger phase2: all channels report in, wait-set empties on second callback
        long checkpointId = 42L;
        writer.onChannelCheckpointStarted(checkpointId, ch0, writer.getCurrentDrainHead());
        writer.onChannelCheckpointStarted(checkpointId, ch1, writer.getCurrentDrainHead());

        // Two entries must have been streamed to ChannelStateWriter
        assertThat(recordingWriter.inputDataCalls).hasSize(2);

        RecordingChannelStateWriter.Call call0 = recordingWriter.inputDataCalls.get(0);
        assertThat(call0.checkpointId).isEqualTo(checkpointId);
        assertThat(call0.info).isEqualTo(ch0);
        assertThat(call0.dataLength).isEqualTo(SEGMENT_SIZE);
        assertThat(call0.capturedBytes).isEqualTo(d0);

        RecordingChannelStateWriter.Call call1 = recordingWriter.inputDataCalls.get(1);
        assertThat(call1.checkpointId).isEqualTo(checkpointId);
        assertThat(call1.info).isEqualTo(ch1);
        assertThat(call1.dataLength).isEqualTo(SEGMENT_SIZE);
        assertThat(call1.capturedBytes).isEqualTo(d1);

        writer.drainPendingSpill();
        writer.close();
    }

    /**
     * phase 2 is a snapshot-only backup: it must NOT decrement {@code store.pendingCount}. The
     * original reader and the store state are left untouched so close()'s drain loop still has
     * these entries to deliver to the task via network buffers.
     */
    @Test
    void testPhase2DoesNotTouchStorePendingCount() throws Exception {
        Queue<Buffer> drainPool = createBufferPool(3);
        FilteredBufferDispatcherImpl writer =
                new FilteredBufferDispatcherImpl(
                        stores,
                        ChannelStateWriter.NO_OP,
                        spillDirs,
                        SEGMENT_SIZE,
                        TestBufferPool.drainOnly(drainPool));

        // Spill 2 entries for ch0, 1 for ch1 — each exactly SEGMENT_SIZE so they auto-seal
        writer.write(createTestData(SEGMENT_SIZE, (byte) 0xB1), SEGMENT_SIZE, ch0);
        writer.write(createTestData(SEGMENT_SIZE, (byte) 0xB2), SEGMENT_SIZE, ch1);
        writer.write(createTestData(SEGMENT_SIZE, (byte) 0xB3), SEGMENT_SIZE, ch0);
        writer.flush();

        // Before phase 2: both stores non-empty
        assertNotEmpty(store0);
        assertNotEmpty(store1);

        // Phase 2: all callbacks arrive — entries are copied to checkpoint, but pendingCount stays
        long checkpointId = 7L;
        writer.onChannelCheckpointStarted(checkpointId, ch0, writer.getCurrentDrainHead());
        writer.onChannelCheckpointStarted(checkpointId, ch1, writer.getCurrentDrainHead());

        // pendingCount untouched — stores still report non-empty
        assertNotEmpty(store0);
        assertNotEmpty(store1);

        // drainPendingSpill() delivers all entries to stores; only then do the counts go to zero
        writer.drainPendingSpill();
        writer.close();
        // Drain the ready buffers so isEmpty() reflects pendingCount only
        drainReady(store0);
        drainReady(store1);
        assertEmpty(store0);
        assertEmpty(store1);
    }

    /**
     * After phase 2 snapshots entries into the checkpoint, drainPendingSpill() must still deliver
     * every entry to the stores (phase 2 is a backup, not an ownership transfer).
     */
    @Test
    void testDrainStillDeliversEntriesAfterPhase2() throws Exception {
        Queue<Buffer> drainPool = createBufferPool(1);
        FilteredBufferDispatcherImpl writer =
                new FilteredBufferDispatcherImpl(
                        stores,
                        ChannelStateWriter.NO_OP,
                        spillDirs,
                        SEGMENT_SIZE,
                        TestBufferPool.drainOnly(drainPool));

        byte[] payload = createTestData(SEGMENT_SIZE, (byte) 0xC1);
        writer.write(payload, SEGMENT_SIZE, ch0);
        writer.flush();

        writer.onChannelCheckpointStarted(55L, ch0, writer.getCurrentDrainHead());

        // drainPendingSpill() consumes the still-pending entry and delivers it to the store
        writer.drainPendingSpill();
        writer.close();

        Buffer delivered = tryTake(store0);
        assertThat(delivered).isNotNull();
        byte[] actual = new byte[delivered.getSize()];
        delivered.getMemorySegment().get(0, actual, 0, delivered.getSize());
        delivered.recycleBuffer();
        assertThat(actual).isEqualTo(payload);
    }

    /**
     * Two independent consumers: phase 2 writes every entry into the checkpoint via
     * ChannelStateWriter, and drainPendingSpill() additionally delivers every entry to the stores.
     * Both streams see the full data — the on-disk bytes are read twice via independent
     * FileChannels.
     */
    @Test
    void testPhase2AndDrainBothReceiveAllEntries() throws Exception {
        Queue<Buffer> drainPool = createBufferPool(2);
        RecordingChannelStateWriter recordingWriter = new RecordingChannelStateWriter();

        FilteredBufferDispatcherImpl writer =
                new FilteredBufferDispatcherImpl(
                        stores,
                        recordingWriter,
                        spillDirs,
                        SEGMENT_SIZE,
                        TestBufferPool.drainOnly(drainPool));

        byte[] payload0 = createTestData(SEGMENT_SIZE, (byte) 0xD1);
        byte[] payload1 = createTestData(SEGMENT_SIZE, (byte) 0xD2);
        writer.write(payload0, SEGMENT_SIZE, ch0);
        writer.write(payload1, SEGMENT_SIZE, ch1);
        writer.flush();

        // Phase 2: both entries captured into ChannelStateWriter (checkpoint backup)
        long checkpointId = 100L;
        writer.onChannelCheckpointStarted(checkpointId, ch0, writer.getCurrentDrainHead());
        writer.onChannelCheckpointStarted(checkpointId, ch1, writer.getCurrentDrainHead());
        assertThat(recordingWriter.inputDataCalls).hasSize(2);

        // drainPendingSpill(): both entries additionally delivered to the stores (task-facing
        // pipeline)
        writer.drainPendingSpill();
        writer.close();

        Buffer buf0 = tryTake(store0);
        Buffer buf1 = tryTake(store1);
        assertThat(buf0).isNotNull();
        assertThat(buf1).isNotNull();
        byte[] got0 = new byte[buf0.getSize()];
        byte[] got1 = new byte[buf1.getSize()];
        buf0.getMemorySegment().get(0, got0, 0, buf0.getSize());
        buf1.getMemorySegment().get(0, got1, 0, buf1.getSize());
        buf0.recycleBuffer();
        buf1.recycleBuffer();
        assertThat(got0).isEqualTo(payload0);
        assertThat(got1).isEqualTo(payload1);
    }

    /**
     * After a store is released, its pending disk entries must be dropped from every Reader so the
     * dispatcher's subsequent close() drain does not try to deliver bytes for the gone channel.
     */
    @Test
    void testReleaseAllRemovesChannelDiskEntriesEagerly() throws Exception {
        Queue<Buffer> drainPool = createBufferPool(5);
        FilteredBufferDispatcherImpl writer =
                new FilteredBufferDispatcherImpl(
                        stores,
                        ChannelStateWriter.NO_OP,
                        spillDirs,
                        SEGMENT_SIZE,
                        TestBufferPool.drainOnly(drainPool));

        byte[] d0 = createTestData(SEGMENT_SIZE, (byte) 0x51);
        byte[] d1 = createTestData(SEGMENT_SIZE, (byte) 0x52);
        writer.write(d0, d0.length, ch0);
        writer.write(d1, d1.length, ch1);
        writer.flush();

        // Release store0 before draining — this should propagate to the dispatcher, which drops
        // all ch0 entries from the Readers.
        store0.releaseAll();

        writer.drainPendingSpill();
        writer.close();

        // store1 still receives its data; store0 must stay empty since ch0 entries were dropped.
        assertThat(tryTake(store0)).isNull();
        assertThat(concat(drainStore(store1))).isEqualTo(d1);
    }

    /**
     * When a channel is released while an in-flight checkpoint wait-set still contains it, the
     * dispatcher must remove it from the wait-set so the wait-set can still converge to empty and
     * phase-2 drain is not blocked.
     */
    @Test
    void testReleaseAllConvergesInFlightCheckpointWaitSet() throws Exception {
        Queue<Buffer> drainPool = createBufferPool(5);
        RecordingChannelStateWriter recordingWriter = new RecordingChannelStateWriter();
        FilteredBufferDispatcherImpl writer =
                new FilteredBufferDispatcherImpl(
                        stores,
                        recordingWriter,
                        spillDirs,
                        SEGMENT_SIZE,
                        TestBufferPool.drainOnly(drainPool));

        writer.write(createTestData(SEGMENT_SIZE, (byte) 0x61), SEGMENT_SIZE, ch0);
        writer.write(createTestData(SEGMENT_SIZE, (byte) 0x62), SEGMENT_SIZE, ch1);
        writer.flush();

        // ch0 reports in. Wait-set still contains ch1 so phase 2 must not have fired yet.
        writer.onChannelCheckpointStarted(30L, ch0, writer.getCurrentDrainHead());
        assertThat(recordingWriter.inputDataCalls).isEmpty();

        // ch1 is released before its checkpoint callback ever arrives. The dispatcher removes it
        // from the wait-set, which now empties and triggers phase 2.
        store1.releaseAll();

        // Only ch0's entry made it into the checkpoint backup; ch1's entries were dropped on
        // release.
        assertThat(recordingWriter.inputDataCalls).hasSize(1);
        assertThat(recordingWriter.inputDataCalls.get(0).info).isEqualTo(ch0);

        writer.drainPendingSpill();
        writer.close();
    }

    /**
     * After a checkpoint is aborted (i.e. all stores called notifyCheckpointStopped), a subsequent
     * channel release must NOT trigger a phase-2 drain into the stopped checkpoint — the writer for
     * that id is gone and any drain would either be wasted work or rely on the writer's isDone()
     * guard to silently swallow the data.
     */
    @Test
    void testReleaseAfterStoppedCheckpointDoesNotDrainStoppedCheckpoint() throws Exception {
        Queue<Buffer> drainPool = createBufferPool(5);
        RecordingChannelStateWriter recordingWriter = new RecordingChannelStateWriter();
        FilteredBufferDispatcherImpl writer =
                new FilteredBufferDispatcherImpl(
                        stores,
                        recordingWriter,
                        spillDirs,
                        SEGMENT_SIZE,
                        TestBufferPool.drainOnly(drainPool));

        writer.write(createTestData(SEGMENT_SIZE, (byte) 0x71), SEGMENT_SIZE, ch0);
        writer.write(createTestData(SEGMENT_SIZE, (byte) 0x72), SEGMENT_SIZE, ch1);
        writer.flush();

        // Checkpoint 50 starts on ch0; wait-set still contains ch1.
        writer.onChannelCheckpointStarted(50L, ch0, writer.getCurrentDrainHead());
        assertThat(recordingWriter.inputDataCalls).isEmpty();

        // The task aborts checkpoint 50 — every channel's persister fires notifyCheckpointStopped.
        store0.notifyCheckpointStopped(50L);
        store1.notifyCheckpointStopped(50L);

        // Now ch1 is released. Without the stopped-checkpoint short-circuit, the wait-set would
        // empty and the dispatcher would drain to checkpoint 50; with the fix, no drain fires.
        store1.releaseAll();

        assertThat(recordingWriter.inputDataCalls).isEmpty();

        writer.drainPendingSpill();
        writer.close();
    }

    /**
     * A late {@code onChannelCheckpointStarted} for a checkpoint that has already been stopped must
     * be ignored as stale, even if a new checkpoint has not yet started.
     */
    @Test
    void testLateCheckpointStartedAfterStoppedIsIgnored() throws Exception {
        Queue<Buffer> drainPool = createBufferPool(5);
        RecordingChannelStateWriter recordingWriter = new RecordingChannelStateWriter();
        FilteredBufferDispatcherImpl writer =
                new FilteredBufferDispatcherImpl(
                        stores,
                        recordingWriter,
                        spillDirs,
                        SEGMENT_SIZE,
                        TestBufferPool.drainOnly(drainPool));

        writer.write(createTestData(SEGMENT_SIZE, (byte) 0x81), SEGMENT_SIZE, ch0);
        writer.flush();

        // Stop checkpoint 60 before anyone reports in.
        store0.notifyCheckpointStopped(60L);
        store1.notifyCheckpointStopped(60L);

        // A late onChannelCheckpointStarted(60, ...) shows up. It must be short-circuited.
        writer.onChannelCheckpointStarted(60L, ch0, writer.getCurrentDrainHead());
        writer.onChannelCheckpointStarted(60L, ch1, writer.getCurrentDrainHead());

        assertThat(recordingWriter.inputDataCalls).isEmpty();

        writer.drainPendingSpill();
        writer.close();
    }

    /**
     * A new checkpoint started AFTER a stop notification must still progress normally — the
     * stopped-id short-circuit only skips the exact stopped id, not all subsequent checkpoints.
     */
    @Test
    void testCheckpointAfterStoppedStillProgresses() throws Exception {
        Queue<Buffer> drainPool = createBufferPool(5);
        RecordingChannelStateWriter recordingWriter = new RecordingChannelStateWriter();
        FilteredBufferDispatcherImpl writer =
                new FilteredBufferDispatcherImpl(
                        stores,
                        recordingWriter,
                        spillDirs,
                        SEGMENT_SIZE,
                        TestBufferPool.drainOnly(drainPool));

        writer.write(createTestData(SEGMENT_SIZE, (byte) 0x91), SEGMENT_SIZE, ch0);
        writer.write(createTestData(SEGMENT_SIZE, (byte) 0x92), SEGMENT_SIZE, ch1);
        writer.flush();

        // Abort checkpoint 70.
        store0.notifyCheckpointStopped(70L);
        store1.notifyCheckpointStopped(70L);

        // Checkpoint 71 begins; both channels report in and phase-2 fires for 71.
        writer.onChannelCheckpointStarted(71L, ch0, writer.getCurrentDrainHead());
        writer.onChannelCheckpointStarted(71L, ch1, writer.getCurrentDrainHead());

        assertThat(recordingWriter.inputDataCalls).hasSize(2);
        assertThat(recordingWriter.inputDataCalls.get(0).checkpointId).isEqualTo(71L);
        assertThat(recordingWriter.inputDataCalls.get(1).checkpointId).isEqualTo(71L);

        writer.drainPendingSpill();
        writer.close();
    }

    /**
     * A BufferRequester whose blocking path parks indefinitely until interrupted. Used to verify
     * that callers that should NOT invoke requestBufferBlocking (e.g. close()) are not blocked.
     */
    private static final class BlockingForeverBufferRequester implements BufferRequester {

        /** Signals that a blocking request is in flight. */
        final CountDownLatch blockingStarted = new CountDownLatch(1);

        /** Unblocks the blocking requester when a real buffer should be delivered. */
        final SynchronousQueue<Buffer> releaseQueue = new SynchronousQueue<>();

        @Override
        public Buffer requestBuffer(InputChannelInfo channelInfo) {
            return null; // write path always spills
        }

        @Override
        public Buffer requestBufferBlocking(InputChannelInfo channelInfo)
                throws InterruptedException {
            blockingStarted.countDown();
            // Park until a buffer arrives through releaseQueue or the thread is interrupted.
            return releaseQueue.take();
        }

        @Override
        public void releaseExclusiveBuffers() {
            // No-op: this test fixture has no per-channel buffer manager to tear down.
        }
    }

    /**
     * AT-CABT: abort path — skip drainPendingSpill, call close() directly. close() must return
     * promptly (it must not invoke requestBufferBlocking) and delete all spill files.
     */
    @Test
    void testCloseWithoutDrainReleasesResources() throws Exception {
        // requestBufferBlocking parks indefinitely — if close() mistakenly calls it the test hangs.
        BlockingForeverBufferRequester requester = new BlockingForeverBufferRequester();

        FilteredBufferDispatcherImpl writer =
                new FilteredBufferDispatcherImpl(
                        stores, ChannelStateWriter.NO_OP, spillDirs, SEGMENT_SIZE, requester);

        byte[] data = createTestData(SEGMENT_SIZE, (byte) 0xAB);
        writer.write(data, data.length, ch0);
        writer.flush();

        // Verify spill file exists before close.
        try (Stream<Path> files =
                Files.list(tempDir).filter(p -> p.getFileName().toString().startsWith("spill-"))) {
            assertThat(files.count()).isGreaterThan(0);
        }

        // close() skips drainPendingSpill entirely: must complete within 5 s (not block on buffer).
        assertTimeoutPreemptively(
                Duration.ofSeconds(5),
                () -> assertDoesNotThrow(writer::close),
                "close() blocked — it incorrectly called requestBufferBlocking");

        // Spill files must be deleted on close.
        try (Stream<Path> files =
                Files.list(tempDir).filter(p -> p.getFileName().toString().startsWith("spill-"))) {
            assertThat(files.count()).isEqualTo(0);
        }

        // The written bytes were intentionally dropped (abort semantics). Store must remain empty.
        assertThat(tryTake(store0)).isNull();
    }

    /**
     * AT-INTR: drainPendingSpill() is interruptible. When the drain thread blocks on
     * requestBufferBlocking and the thread is interrupted, drainPendingSpill() must propagate
     * InterruptedException. A subsequent close() must still release resources.
     */
    @Test
    void testDrainPendingSpillInterruptible() throws Exception {
        BlockingForeverBufferRequester requester = new BlockingForeverBufferRequester();

        FilteredBufferDispatcherImpl writer =
                new FilteredBufferDispatcherImpl(
                        stores, ChannelStateWriter.NO_OP, spillDirs, SEGMENT_SIZE, requester);

        byte[] data = createTestData(SEGMENT_SIZE, (byte) 0xBC);
        writer.write(data, data.length, ch0);
        writer.flush();

        ExecutorService executor = Executors.newSingleThreadExecutor();
        Future<Exception> drainFuture =
                executor.submit(
                        () -> {
                            try {
                                writer.drainPendingSpill();
                                return null; // unexpected: should have thrown
                            } catch (InterruptedException e) {
                                // restore flag for good measure
                                Thread.currentThread().interrupt();
                                return e;
                            } catch (Exception e) {
                                return e;
                            }
                        });

        // Wait until drainPendingSpill() is actually blocked.
        assertThat(requester.blockingStarted.await(5, TimeUnit.SECONDS))
                .as("drain thread did not enter blocking state in time")
                .isTrue();

        // Interrupt the drain thread.
        executor.shutdownNow();

        Exception thrown = drainFuture.get(5, TimeUnit.SECONDS);
        assertThat(thrown)
                .as("drainPendingSpill() must throw InterruptedException on interrupt")
                .isInstanceOf(InterruptedException.class);

        // Even after an interrupted drain, close() must release resources without throwing.
        assertTimeoutPreemptively(
                Duration.ofSeconds(5),
                () -> assertDoesNotThrow(writer::close),
                "close() blocked after interrupted drain");

        // Spill files must be cleaned up.
        try (Stream<Path> files =
                Files.list(tempDir).filter(p -> p.getFileName().toString().startsWith("spill-"))) {
            assertThat(files.count()).isEqualTo(0);
        }
    }

    /**
     * Phase 2 must respect each channel's recorded {@code startPos}: entries with position strictly
     * less than the channel's startPos are skipped (their channel's Step 1 already captured them
     * via readyBuffers), while entries at or beyond startPos are emitted to the channel state. Two
     * channels with different startPos values let us verify the filter is applied per-channel and
     * not globally.
     */
    @Test
    void testPhase2FilterByPerChannelStartPos() throws Exception {
        RecordingChannelStateWriter recordingWriter = new RecordingChannelStateWriter();
        FilteredBufferDispatcherImpl writer =
                new FilteredBufferDispatcherImpl(
                        stores,
                        recordingWriter,
                        spillDirs,
                        SEGMENT_SIZE,
                        TestBufferPool.drainOnly(createBufferPool(0)));

        // Layout in the spill file: ch0 entries at offsets 0 and 2*SEGMENT_SIZE, ch1 entry at
        // offset SEGMENT_SIZE. Total 3 entries, FIFO order.
        byte[] d0a = createTestData(SEGMENT_SIZE, (byte) 0x10);
        byte[] d1 = createTestData(SEGMENT_SIZE, (byte) 0x20);
        byte[] d0b = createTestData(SEGMENT_SIZE, (byte) 0x11);
        writer.write(d0a, SEGMENT_SIZE, ch0);
        writer.write(d1, SEGMENT_SIZE, ch1);
        writer.write(d0b, SEGMENT_SIZE, ch0);
        writer.flush();

        // ch0's barrier passed before any drain — its startPos is the head of the file (include
        // every ch0 entry). ch1's barrier passed after the first ch0 entry was logically drained —
        // its startPos points at the third entry (file 0, offset 2*SEGMENT_SIZE), so its single
        // entry at offset SEGMENT_SIZE must be skipped (covered by Step 1 ready snapshot).
        EntryPosition ch0StartPos = new EntryPosition(0, 0);
        EntryPosition ch1StartPos = new EntryPosition(0, 2L * SEGMENT_SIZE);
        writer.onChannelCheckpointStarted(7L, ch0, ch0StartPos);
        writer.onChannelCheckpointStarted(7L, ch1, ch1StartPos);

        writer.close();

        // ch0 receives both of its entries; ch1 receives nothing in phase 2.
        List<byte[]> ch0Bytes =
                recordingWriter.inputDataCalls.stream()
                        .filter(c -> c.info.equals(ch0))
                        .map(c -> c.capturedBytes)
                        .collect(java.util.stream.Collectors.toList());
        List<byte[]> ch1Bytes =
                recordingWriter.inputDataCalls.stream()
                        .filter(c -> c.info.equals(ch1))
                        .map(c -> c.capturedBytes)
                        .collect(java.util.stream.Collectors.toList());
        assertThat(ch0Bytes).hasSize(2);
        assertThat(ch0Bytes.get(0)).isEqualTo(d0a);
        assertThat(ch0Bytes.get(1)).isEqualTo(d0b);
        assertThat(ch1Bytes).isEmpty();
    }

    /**
     * The phase-2 snapshot is captured at the <em>first</em> {@code onChannelCheckpointStarted}
     * call, not at wait-set convergence. This guards the original race: between the first and last
     * trigger, drainPendingSpill could pop entries off the original Reader; the pinned snapshot
     * preserves the pre-drain view so phase-2 sees every entry that was on disk when the first
     * channel passed its barrier.
     *
     * <p>Scenario: write three entries for two channels, fire ch0's trigger to capture the
     * snapshot, drain everything (which empties the original Reader's deque), then fire ch1's
     * trigger to converge the wait-set. Phase 2 must still report all three entries because the
     * snapshot was taken before drain ran.
     */
    @Test
    void testPhase2SnapshotPinnedAtFirstTrigger() throws Exception {
        RecordingChannelStateWriter recordingWriter = new RecordingChannelStateWriter();
        Queue<Buffer> drainPool = createBufferPool(5);
        FilteredBufferDispatcherImpl writer =
                new FilteredBufferDispatcherImpl(
                        stores,
                        recordingWriter,
                        spillDirs,
                        SEGMENT_SIZE,
                        TestBufferPool.drainOnly(drainPool));

        byte[] d0 = createTestData(SEGMENT_SIZE, (byte) 0xC0);
        byte[] d1 = createTestData(SEGMENT_SIZE, (byte) 0xC1);
        byte[] d2 = createTestData(SEGMENT_SIZE, (byte) 0xC2);
        writer.write(d0, SEGMENT_SIZE, ch0);
        writer.write(d1, SEGMENT_SIZE, ch1);
        writer.write(d2, SEGMENT_SIZE, ch0);
        writer.flush();

        // First trigger arrives BEFORE drain. The snapshot pins the full disk view here.
        EntryPosition initialDrainHead = writer.getCurrentDrainHead();
        writer.onChannelCheckpointStarted(11L, ch0, initialDrainHead);

        // drainPendingSpill empties the ORIGINAL Reader. Phase 2 must read from the pinned
        // snapshot, not the post-drain Reader, otherwise we lose every entry.
        writer.drainPendingSpill();

        // Now ch1's trigger converges the wait-set, firing phase-2.
        writer.onChannelCheckpointStarted(11L, ch1, initialDrainHead);
        writer.close();

        // All three entries must show up in phase-2 with their original channel info.
        List<RecordingChannelStateWriter.Call> calls = recordingWriter.inputDataCalls;
        assertThat(calls).hasSize(3);
        assertThat(calls.get(0).info).isEqualTo(ch0);
        assertThat(calls.get(0).capturedBytes).isEqualTo(d0);
        assertThat(calls.get(1).info).isEqualTo(ch1);
        assertThat(calls.get(1).capturedBytes).isEqualTo(d1);
        assertThat(calls.get(2).info).isEqualTo(ch0);
        assertThat(calls.get(2).capturedBytes).isEqualTo(d2);
    }

    /**
     * {@code onChannelCheckpointStopped} must close pinned snapshot Readers and clear the in-
     * progress checkpoint state, otherwise every aborted checkpoint leaks one {@code FileChannel}
     * per spill file and a stale per-channel startPos map can poison the next checkpoint's filter.
     * Following an abort, a fresh checkpoint must produce a complete phase-2 from a brand-new
     * snapshot.
     */
    @Test
    void testCheckpointStoppedReleasesSnapshotsAndStateForNextCheckpoint() throws Exception {
        RecordingChannelStateWriter recordingWriter = new RecordingChannelStateWriter();
        FilteredBufferDispatcherImpl writer =
                new FilteredBufferDispatcherImpl(
                        stores,
                        recordingWriter,
                        spillDirs,
                        SEGMENT_SIZE,
                        TestBufferPool.drainOnly(createBufferPool(0)));

        byte[] d0 = createTestData(SEGMENT_SIZE, (byte) 0xD0);
        byte[] d1 = createTestData(SEGMENT_SIZE, (byte) 0xD1);
        writer.write(d0, SEGMENT_SIZE, ch0);
        writer.write(d1, SEGMENT_SIZE, ch1);
        writer.flush();

        // Start checkpoint 5; only ch0 reports in, then both channels stop the checkpoint
        // (e.g. abort path) before ch1 reports.
        writer.onChannelCheckpointStarted(5L, ch0, writer.getCurrentDrainHead());
        writer.onChannelCheckpointStopped(5L, ch0);
        writer.onChannelCheckpointStopped(5L, ch1);
        // Phase 2 for ckpt 5 was never submitted because the wait-set never converged before stop.
        assertThat(recordingWriter.inputDataCalls).isEmpty();

        // A subsequent checkpoint 6 starts cleanly and converges normally — must include both
        // entries despite the dangling state from the aborted checkpoint 5.
        EntryPosition startPos = writer.getCurrentDrainHead();
        writer.onChannelCheckpointStarted(6L, ch0, startPos);
        writer.onChannelCheckpointStarted(6L, ch1, startPos);
        writer.close();

        assertThat(recordingWriter.inputDataCalls).hasSize(2);
        assertThat(recordingWriter.inputDataCalls.get(0).checkpointId).isEqualTo(6L);
        assertThat(recordingWriter.inputDataCalls.get(1).checkpointId).isEqualTo(6L);
    }

    /**
     * The {@code drainHead} field must advance only after each drain bundle's {@code addBuffer}, so
     * an external observer reading {@code getCurrentDrainHead()} can rely on the invariant
     * "drainHead crossed e ⇒ e is in store.readyBuffers". This test exercises the public
     * observable: at flush() drainHead points at the first entry; after each drain bundle commits,
     * drainHead advances to the next entry; after the last entry drainHead reaches END.
     */
    @Test
    void testDrainHeadAdvancesAfterEachAddBuffer() throws Exception {
        Queue<Buffer> drainPool = createBufferPool(5);
        FilteredBufferDispatcherImpl writer =
                new FilteredBufferDispatcherImpl(
                        stores,
                        ChannelStateWriter.NO_OP,
                        spillDirs,
                        SEGMENT_SIZE,
                        TestBufferPool.drainOnly(drainPool));

        byte[] d0 = createTestData(SEGMENT_SIZE, (byte) 0xE0);
        byte[] d1 = createTestData(SEGMENT_SIZE, (byte) 0xE1);
        writer.write(d0, SEGMENT_SIZE, ch0);
        writer.write(d1, SEGMENT_SIZE, ch1);
        writer.flush();

        // After flush, drainHead points at the first entry of file 0.
        EntryPosition headAfterFlush = writer.getCurrentDrainHead();
        assertThat(headAfterFlush.getFileIndex()).isEqualTo(0);
        assertThat(headAfterFlush.getOffset()).isEqualTo(0L);

        writer.drainPendingSpill();

        // After drain consumes everything, drainHead reaches the END sentinel.
        assertThat(writer.getCurrentDrainHead()).isEqualTo(EntryPosition.END);

        writer.close();
    }

    /**
     * Before the {@link #drainPendingSpill()} bundle was made atomic, a phase-2 snapshot taken in
     * the gap between {@code reader.skipNextEntry()} (entry gone from disk-side bookkeeping) and
     * {@code store.addBuffer()} (entry not yet in the channel's readyBuffers) would lose the entry:
     * Step 1 captures no buffer, phase-2 sees no entry. This test exercises the invariant by
     * injecting an {@code onChannelCheckpointStarted} call between two drain bundles and asserting
     * that every spilled entry appears either in the channel's readyBuffers (Step 1) or in phase-2
     * capture — never neither.
     */
    @Test
    void testNoEntryLostBetweenDrainAndCheckpointTrigger() throws Exception {
        RecordingChannelStateWriter recordingWriter = new RecordingChannelStateWriter();
        Queue<Buffer> drainPool = createBufferPool(5);
        FilteredBufferDispatcherImpl writer =
                new FilteredBufferDispatcherImpl(
                        stores,
                        recordingWriter,
                        spillDirs,
                        SEGMENT_SIZE,
                        TestBufferPool.drainOnly(drainPool));

        byte[] d0a = createTestData(SEGMENT_SIZE, (byte) 0x60);
        byte[] d0b = createTestData(SEGMENT_SIZE, (byte) 0x61);
        byte[] d0c = createTestData(SEGMENT_SIZE, (byte) 0x62);
        byte[] d1 = createTestData(SEGMENT_SIZE, (byte) 0x70);
        writer.write(d0a, SEGMENT_SIZE, ch0);
        writer.write(d1, SEGMENT_SIZE, ch1);
        writer.write(d0b, SEGMENT_SIZE, ch0);
        writer.write(d0c, SEGMENT_SIZE, ch0);
        writer.flush();

        // ch0's barrier arrives BEFORE drain runs — every ch0 entry is "in flight" for ckpt 9.
        EntryPosition ch0StartPos = writer.getCurrentDrainHead();
        writer.onChannelCheckpointStarted(9L, ch0, ch0StartPos);

        // Drain runs to completion: entries are popped from the original reader and addBuffered
        // to their stores. The phase-2 snapshot was pinned before drain so it still owns the full
        // disk view; per-channel filtering decides whether each snapshot entry is emitted.
        writer.drainPendingSpill();

        // ch1 reports in AFTER drain — its startPos is END (everything already drained for ch1's
        // perspective). Phase-2 must skip every ch1 snapshot entry (Step 1 captured them via
        // store readyBuffers) and emit the full ch0 set.
        EntryPosition ch1StartPos = writer.getCurrentDrainHead();
        writer.onChannelCheckpointStarted(9L, ch1, ch1StartPos);
        writer.close();

        // Aggregate every ch0 byte the dispatcher told the world about: phase-2 emits + buffers
        // the store actually received via drain. The set must equal the original three ch0 writes
        // — no entry can fall through both paths.
        List<byte[]> phase2Ch0 =
                recordingWriter.inputDataCalls.stream()
                        .filter(c -> c.info.equals(ch0))
                        .map(c -> c.capturedBytes)
                        .collect(java.util.stream.Collectors.toList());
        List<byte[]> readyCh0 = drainStore(store0);
        // ch0 trigger fired before drain → every ch0 spill entry has p_e >= startPos_ch0,
        // so phase 2 emits all three. Drain afterwards still adds them to readyBuffers, but the
        // race fix only guarantees coverage (no loss); a buffer being delivered post-trigger is
        // expected and Task will consume it as ckpt N+1's input.
        assertThat(phase2Ch0).hasSize(3);
        assertThat(phase2Ch0).containsExactly(d0a, d0b, d0c);
        // For ch1: ch1's barrier was after drain, so its single entry is captured by Step 1
        // (readyBuffers) and skipped in phase 2.
        List<byte[]> phase2Ch1 =
                recordingWriter.inputDataCalls.stream()
                        .filter(c -> c.info.equals(ch1))
                        .map(c -> c.capturedBytes)
                        .collect(java.util.stream.Collectors.toList());
        List<byte[]> readyCh1 = drainStore(store1);
        assertThat(phase2Ch1).isEmpty();
        assertThat(readyCh1).hasSize(1);
        assertThat(readyCh1.get(0)).isEqualTo(d1);
        // Sanity: the ready set for ch0 contains every original byte too (drain delivered them
        // post-trigger), but the test's correctness hinges on the phase-2 set being complete.
        assertThat(readyCh0).hasSize(3);
    }

    /**
     * AT-LOCK: FLINK-39519 deadlock regression. drainPendingSpill() must NOT hold the dispatcher
     * monitor while blocking on requestBufferBlocking. Concurrent onChannelCheckpointStopped (which
     * acquires the dispatcher monitor) must complete promptly while drain is blocked.
     *
     * <p>Before the close/drain split, drainSpillThroughBuffers() ran inside a synchronized block,
     * so onChannelCheckpointStopped would deadlock waiting for the monitor held by drain. This test
     * reproduces that scenario and asserts the callback completes in time.
     */
    @Test
    void testDrainPendingSpillReleasesMonitorForCheckpointStopped() throws Exception {
        BlockingForeverBufferRequester requester = new BlockingForeverBufferRequester();

        FilteredBufferDispatcherImpl writer =
                new FilteredBufferDispatcherImpl(
                        stores, ChannelStateWriter.NO_OP, spillDirs, SEGMENT_SIZE, requester);

        // Set up checkpoint wait-set state so onChannelCheckpointStopped follows the full path.
        // notifyCheckpointStopped on the stores will call writer.onChannelCheckpointStopped.
        long checkpointId = 42L;

        byte[] data = createTestData(SEGMENT_SIZE, (byte) 0xCD);
        writer.write(data, data.length, ch0);
        writer.flush();

        // Build the wait-set for checkpoint 42 by having ch0 report in.
        writer.onChannelCheckpointStarted(checkpointId, ch0, writer.getCurrentDrainHead());

        ExecutorService executor = Executors.newSingleThreadExecutor();
        Future<?> drainFuture =
                executor.submit(
                        () -> {
                            try {
                                writer.drainPendingSpill();
                            } catch (InterruptedException e) {
                                Thread.currentThread().interrupt();
                            } catch (Exception ignored) {
                            }
                        });

        // Wait until drainPendingSpill() is blocked inside requestBufferBlocking.
        assertThat(requester.blockingStarted.await(5, TimeUnit.SECONDS))
                .as("drain thread did not enter blocking state in time")
                .isTrue();

        // onChannelCheckpointStopped acquires the dispatcher monitor. If drain held the monitor
        // this call would deadlock; it must return within 1 s.
        assertTimeoutPreemptively(
                Duration.ofSeconds(1),
                () -> {
                    store0.notifyCheckpointStopped(checkpointId);
                    store1.notifyCheckpointStopped(checkpointId);
                },
                "onChannelCheckpointStopped deadlocked — drainPendingSpill held the dispatcher monitor");

        // Interrupt the drain thread so the test exits cleanly.
        executor.shutdownNow();
        drainFuture.get(5, TimeUnit.SECONDS);

        writer.close();
    }

    // -- helpers --

    private static void assertEmpty(RecoveredBufferStoreImpl store) {
        synchronized (store) {
            assertThat(store.isEmpty()).isTrue();
        }
    }

    private static void assertNotEmpty(RecoveredBufferStoreImpl store) {
        synchronized (store) {
            assertThat(store.isEmpty()).isFalse();
        }
    }

    private static Buffer tryTake(RecoveredBufferStoreImpl store) {
        synchronized (store) {
            return store.tryTake();
        }
    }

    private static void drainReady(RecoveredBufferStoreImpl store) {
        synchronized (store) {
            Buffer b;
            while ((b = store.tryTake()) != null) {
                b.recycleBuffer();
            }
        }
    }
}
