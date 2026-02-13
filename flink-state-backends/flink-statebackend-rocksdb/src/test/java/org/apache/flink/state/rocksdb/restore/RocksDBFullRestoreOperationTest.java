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

package org.apache.flink.state.rocksdb.restore;

import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.api.common.typeutils.base.IntSerializer;
import org.apache.flink.runtime.checkpoint.CheckpointOptions;
import org.apache.flink.runtime.state.KeyGroupRange;
import org.apache.flink.runtime.state.KeyGroupedInternalPriorityQueue;
import org.apache.flink.runtime.state.KeyedStateHandle;
import org.apache.flink.runtime.state.SnapshotResult;
import org.apache.flink.runtime.state.VoidNamespace;
import org.apache.flink.runtime.state.VoidNamespaceSerializer;
import org.apache.flink.runtime.state.filesystem.FsCheckpointStreamFactory;
import org.apache.flink.state.rocksdb.RocksDBKeyedStateBackend;
import org.apache.flink.state.rocksdb.RocksDBTestUtils;
import org.apache.flink.streaming.api.operators.TimerHeapInternalTimer;
import org.apache.flink.streaming.api.operators.TimerSerializer;
import org.apache.flink.testutils.junit.utils.TempDirUtils;
import org.apache.flink.util.IOUtils;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.util.Collections;
import java.util.UUID;
import java.util.concurrent.RunnableFuture;

import static org.apache.flink.core.fs.Path.fromLocalFile;
import static org.apache.flink.core.fs.local.LocalFileSystem.getSharedInstance;
import static org.assertj.core.api.Assertions.assertThat;

/** Tests for {@link RocksDBFullRestoreOperation} (FLINK-23886). */
public class RocksDBFullRestoreOperationTest {

    @TempDir private static java.nio.file.Path tempFolder;

    @Test
    public void testFullSnapshotRestorePreservesStateIsolation() throws Exception {
        final int maxParallelism = 128;
        final KeyGroupRange keyGroupRange = new KeyGroupRange(0, maxParallelism - 1);

        RocksDBKeyedStateBackend<Integer> backend =
                RocksDBTestUtils.builderForTestDefaults(
                                TempDirUtils.newFolder(tempFolder),
                                IntSerializer.INSTANCE,
                                maxParallelism,
                                keyGroupRange,
                                Collections.emptyList())
                        .build();

        SnapshotResult<KeyedStateHandle> snapshotResult;
        try {
            ValueState<Integer> valueState =
                    backend.getOrCreateKeyedState(
                            VoidNamespaceSerializer.INSTANCE,
                            new ValueStateDescriptor<>("test-value-state", IntSerializer.INSTANCE));

            KeyGroupedInternalPriorityQueue<TimerHeapInternalTimer<Integer, Integer>> timerState =
                    backend.create(
                            "test-timer-state",
                            new TimerSerializer<>(
                                    IntSerializer.INSTANCE, IntSerializer.INSTANCE));

            for (int i = 1; i <= 100; i++) {
                backend.setCurrentKey(i);
                valueState.update(i * 10);
                timerState.add(new TimerHeapInternalTimer<>(i * 1000L, i, 0));
            }

            FsCheckpointStreamFactory streamFactory =
                    new FsCheckpointStreamFactory(
                            getSharedInstance(),
                            fromLocalFile(
                                    TempDirUtils.newFolder(
                                            tempFolder,
                                            "checkpointsDir_" + UUID.randomUUID())),
                            fromLocalFile(
                                    TempDirUtils.newFolder(
                                            tempFolder,
                                            "sharedStateDir_" + UUID.randomUUID())),
                            1,
                            4096);

            RunnableFuture<SnapshotResult<KeyedStateHandle>> snapshotFuture =
                    backend.snapshot(
                            0L,
                            0L,
                            streamFactory,
                            CheckpointOptions.forCheckpointWithDefaultLocation());
            snapshotFuture.run();
            snapshotResult = snapshotFuture.get();
        } finally {
            IOUtils.closeQuietly(backend);
            backend.dispose();
        }

        KeyedStateHandle stateHandle = snapshotResult.getJobManagerOwnedSnapshot();
        assertThat(stateHandle).isNotNull();

        RocksDBKeyedStateBackend<Integer> restoredBackend =
                RocksDBTestUtils.builderForTestDefaults(
                                TempDirUtils.newFolder(tempFolder),
                                IntSerializer.INSTANCE,
                                maxParallelism,
                                keyGroupRange,
                                Collections.singletonList(stateHandle))
                        .build();

        try {
            ValueState<Integer> restoredValueState =
                    restoredBackend.getOrCreateKeyedState(
                            VoidNamespaceSerializer.INSTANCE,
                            new ValueStateDescriptor<>("test-value-state", IntSerializer.INSTANCE));

            for (int i = 1; i <= 100; i++) {
                restoredBackend.setCurrentKey(i);
                assertThat(restoredValueState.value())
                        .as("ValueState for key %d", i)
                        .isEqualTo(i * 10);
            }

            KeyGroupedInternalPriorityQueue<TimerHeapInternalTimer<Integer, Integer>>
                    restoredTimerState =
                            restoredBackend.create(
                                    "test-timer-state",
                                    new TimerSerializer<>(
                                            IntSerializer.INSTANCE, IntSerializer.INSTANCE));

            assertThat(restoredTimerState.isEmpty()).isFalse();
            int timerCount = 0;
            TimerHeapInternalTimer<Integer, Integer> timer;
            long lastTimestamp = Long.MIN_VALUE;
            while ((timer = restoredTimerState.poll()) != null) {
                assertThat(timer.getTimestamp()).isGreaterThanOrEqualTo(lastTimestamp);
                lastTimestamp = timer.getTimestamp();
                timerCount++;
            }
            assertThat(timerCount).isEqualTo(100);

            long keyCount =
                    restoredBackend
                            .getKeys("test-value-state", VoidNamespace.INSTANCE)
                            .count();
            assertThat(keyCount).isEqualTo(100);
        } finally {
            IOUtils.closeQuietly(restoredBackend);
            restoredBackend.dispose();
            snapshotResult.discardState();
        }
    }

    @Test
    public void testSnapshotRestoreSnapshotRoundTrip() throws Exception {
        final int maxParallelism = 16;
        final KeyGroupRange keyGroupRange = new KeyGroupRange(0, maxParallelism - 1);

        RocksDBKeyedStateBackend<Integer> backend =
                RocksDBTestUtils.builderForTestDefaults(
                                TempDirUtils.newFolder(tempFolder),
                                IntSerializer.INSTANCE,
                                maxParallelism,
                                keyGroupRange,
                                Collections.emptyList())
                        .build();

        SnapshotResult<KeyedStateHandle> firstSnapshot;
        try {
            ValueState<Integer> valueState =
                    backend.getOrCreateKeyedState(
                            VoidNamespaceSerializer.INSTANCE,
                            new ValueStateDescriptor<>("state-a", IntSerializer.INSTANCE));

            KeyGroupedInternalPriorityQueue<TimerHeapInternalTimer<Integer, Integer>> timerState =
                    backend.create(
                            "timer-state-a",
                            new TimerSerializer<>(
                                    IntSerializer.INSTANCE, IntSerializer.INSTANCE));

            for (int i = 1; i <= 50; i++) {
                backend.setCurrentKey(i);
                valueState.update(i);
                timerState.add(new TimerHeapInternalTimer<>(i, i, 0));
            }

            firstSnapshot = takeSnapshot(backend);
        } finally {
            IOUtils.closeQuietly(backend);
            backend.dispose();
        }

        KeyedStateHandle firstHandle = firstSnapshot.getJobManagerOwnedSnapshot();
        RocksDBKeyedStateBackend<Integer> restored1 =
                RocksDBTestUtils.builderForTestDefaults(
                                TempDirUtils.newFolder(tempFolder),
                                IntSerializer.INSTANCE,
                                maxParallelism,
                                keyGroupRange,
                                Collections.singletonList(firstHandle))
                        .build();

        SnapshotResult<KeyedStateHandle> secondSnapshot;
        try {
            restored1.getOrCreateKeyedState(
                    VoidNamespaceSerializer.INSTANCE,
                    new ValueStateDescriptor<>("state-a", IntSerializer.INSTANCE));
            restored1.create(
                    "timer-state-a",
                    new TimerSerializer<>(IntSerializer.INSTANCE, IntSerializer.INSTANCE));

            secondSnapshot = takeSnapshot(restored1);
        } finally {
            IOUtils.closeQuietly(restored1);
            restored1.dispose();
            firstSnapshot.discardState();
        }

        KeyedStateHandle secondHandle = secondSnapshot.getJobManagerOwnedSnapshot();
        RocksDBKeyedStateBackend<Integer> restored2 =
                RocksDBTestUtils.builderForTestDefaults(
                                TempDirUtils.newFolder(tempFolder),
                                IntSerializer.INSTANCE,
                                maxParallelism,
                                keyGroupRange,
                                Collections.singletonList(secondHandle))
                        .build();

        try {
            ValueState<Integer> finalState =
                    restored2.getOrCreateKeyedState(
                            VoidNamespaceSerializer.INSTANCE,
                            new ValueStateDescriptor<>("state-a", IntSerializer.INSTANCE));

            for (int i = 1; i <= 50; i++) {
                restored2.setCurrentKey(i);
                assertThat(finalState.value())
                        .as("Value for key %d after double restore", i)
                        .isEqualTo(i);
            }

            KeyGroupedInternalPriorityQueue<TimerHeapInternalTimer<Integer, Integer>>
                    finalTimerState =
                            restored2.create(
                                    "timer-state-a",
                                    new TimerSerializer<>(
                                            IntSerializer.INSTANCE, IntSerializer.INSTANCE));

            int count = 0;
            while (finalTimerState.poll() != null) {
                count++;
            }
            assertThat(count).isEqualTo(50);
        } finally {
            IOUtils.closeQuietly(restored2);
            restored2.dispose();
            secondSnapshot.discardState();
        }
    }

    private SnapshotResult<KeyedStateHandle> takeSnapshot(
            RocksDBKeyedStateBackend<Integer> backend) throws Exception {
        FsCheckpointStreamFactory streamFactory =
                new FsCheckpointStreamFactory(
                        getSharedInstance(),
                        fromLocalFile(
                                TempDirUtils.newFolder(
                                        tempFolder, "checkpointsDir_" + UUID.randomUUID())),
                        fromLocalFile(
                                TempDirUtils.newFolder(
                                        tempFolder, "sharedStateDir_" + UUID.randomUUID())),
                        1,
                        4096);

        RunnableFuture<SnapshotResult<KeyedStateHandle>> snapshotFuture =
                backend.snapshot(
                        0L,
                        0L,
                        streamFactory,
                        CheckpointOptions.forCheckpointWithDefaultLocation());
        snapshotFuture.run();
        return snapshotFuture.get();
    }
}
