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

package org.apache.flink.contrib.streaming.state;

import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.api.common.typeutils.base.IntSerializer;
import org.apache.flink.runtime.checkpoint.CheckpointOptions;
import org.apache.flink.runtime.state.KeyGroupRange;
import org.apache.flink.runtime.state.KeyGroupRangeAssignment;
import org.apache.flink.runtime.state.KeyedStateHandle;
import org.apache.flink.runtime.state.SnapshotResult;
import org.apache.flink.runtime.state.StateObject;
import org.apache.flink.runtime.state.VoidNamespace;
import org.apache.flink.runtime.state.VoidNamespaceSerializer;
import org.apache.flink.runtime.state.filesystem.FsCheckpointStreamFactory;
import org.apache.flink.testutils.junit.utils.TempDirUtils;
import org.apache.flink.util.IOUtils;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.io.IOException;
import java.io.OutputStream;
import java.io.PrintStream;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.RunnableFuture;
import java.util.stream.Collectors;

import static org.apache.flink.core.fs.Path.fromLocalFile;
import static org.apache.flink.core.fs.local.LocalFileSystem.getSharedInstance;

/** Rescaling test and microbenchmark for RocksDB. */
public class RocksDBRecoveryTest {

    // Assign System.out for console output.
    private static final PrintStream OUTPUT =
            new PrintStream(
                    new OutputStream() {
                        @Override
                        public void write(int b) {}
                    });

    @TempDir private static java.nio.file.Path tempFolder;

    @Test
    public void testScaleOut_1_2() throws Exception {
        testRescale(1, 2, 100_000, 10);
    }

    @Test
    public void testScaleOut_2_8() throws Exception {
        testRescale(2, 8, 100_000, 10);
    }

    @Test
    public void testScaleOut_2_7() throws Exception {
        testRescale(2, 7, 100_000, 10);
    }

    @Test
    public void testScaleIn_2_1() throws Exception {
        testRescale(2, 1, 100_000, 10);
    }

    @Test
    public void testScaleIn_8_2() throws Exception {
        testRescale(8, 2, 100_000, 10);
    }

    @Test
    public void testScaleIn_7_2() throws Exception {
        testRescale(7, 2, 100_000, 10);
    }

    @Test
    public void testScaleIn_2_3() throws Exception {
        testRescale(2, 3, 100_000, 10);
    }

    @Test
    public void testScaleIn_3_2() throws Exception {
        testRescale(3, 2, 100_000, 10);
    }

    public void testRescale(
            int startParallelism, int targetParallelism, int numKeys, int updateDistance)
            throws Exception {

        OUTPUT.println("Rescaling from " + startParallelism + " to " + targetParallelism + "...");
        final String stateName = "TestValueState";
        final int maxParallelism = startParallelism * targetParallelism;
        final List<RocksDBKeyedStateBackend<Integer>> backends = new ArrayList<>(maxParallelism);
        final List<SnapshotResult<KeyedStateHandle>> startSnapshotResult = new ArrayList<>();
        final List<SnapshotResult<KeyedStateHandle>> rescaleSnapshotResult = new ArrayList<>();
        final List<SnapshotResult<KeyedStateHandle>> cleanupSnapshotResult = new ArrayList<>();
        try {
            final List<ValueState<Integer>> valueStates = new ArrayList<>(maxParallelism);
            try {
                ValueStateDescriptor<Integer> stateDescriptor =
                        new ValueStateDescriptor<>(stateName, IntSerializer.INSTANCE);

                for (int i = 0; i < startParallelism; ++i) {
                    RocksDBKeyedStateBackend<Integer> backend =
                            RocksDBTestUtils.builderForTestDefaults(
                                            TempDirUtils.newFolder(tempFolder),
                                            IntSerializer.INSTANCE,
                                            maxParallelism,
                                            KeyGroupRangeAssignment
                                                    .computeKeyGroupRangeForOperatorIndex(
                                                            maxParallelism, startParallelism, i),
                                            Collections.emptyList())
                                    .setEnableIncrementalCheckpointing(true)
                                    .setUseIngestDbRestoreMode(true)
                                    .build();

                    valueStates.add(
                            backend.getOrCreateKeyedState(
                                    VoidNamespaceSerializer.INSTANCE, stateDescriptor));

                    backends.add(backend);
                }

                OUTPUT.println("Inserting " + numKeys + " keys...");

                for (int i = 1; i <= numKeys; ++i) {
                    int key = i;
                    int index =
                            KeyGroupRangeAssignment.assignKeyToParallelOperator(
                                    key, maxParallelism, startParallelism);
                    backends.get(index).setCurrentKey(key);
                    valueStates.get(index).update(i);

                    if (updateDistance > 0 && i % updateDistance == 0) {
                        key = i - updateDistance + 1;
                        index =
                                KeyGroupRangeAssignment.assignKeyToParallelOperator(
                                        key, maxParallelism, startParallelism);
                        backends.get(index).setCurrentKey(key);
                        valueStates.get(index).update(i);
                    }
                }

                OUTPUT.println("Creating snapshots...");
                snapshotAllBackends(backends, startSnapshotResult);
            } finally {
                for (RocksDBKeyedStateBackend<Integer> backend : backends) {
                    IOUtils.closeQuietly(backend);
                    backend.dispose();
                }
                valueStates.clear();
                backends.clear();
            }

            for (boolean useIngest : Arrays.asList(Boolean.TRUE, Boolean.FALSE)) {
                for (boolean asyncCompact : Arrays.asList(Boolean.TRUE, Boolean.FALSE)) {

                    // Rescale start -> target
                    rescaleAndRestoreBackends(
                            useIngest,
                            asyncCompact,
                            targetParallelism,
                            maxParallelism,
                            startSnapshotResult,
                            backends);

                    backends.forEach(
                            backend ->
                                    backend.getAsyncCompactAfterRestoreFuture()
                                            .ifPresent(
                                                    future -> {
                                                        try {
                                                            future.get();
                                                        } catch (Exception e) {
                                                            throw new RuntimeException(e);
                                                        }
                                                    }));

                    snapshotAllBackends(backends, rescaleSnapshotResult);

                    int count = 0;
                    for (RocksDBKeyedStateBackend<Integer> backend : backends) {
                        count += backend.getKeys(stateName, VoidNamespace.INSTANCE).count();
                        IOUtils.closeQuietly(backend);
                        backend.dispose();
                    }
                    Assertions.assertEquals(numKeys, count);
                    backends.clear();
                    cleanupSnapshotResult.addAll(rescaleSnapshotResult);

                    // Rescale reverse: target -> start
                    rescaleAndRestoreBackends(
                            useIngest,
                            false,
                            startParallelism,
                            maxParallelism,
                            rescaleSnapshotResult,
                            backends);

                    count = 0;
                    for (RocksDBKeyedStateBackend<Integer> backend : backends) {
                        count += backend.getKeys(stateName, VoidNamespace.INSTANCE).count();
                        IOUtils.closeQuietly(backend);
                        backend.dispose();
                    }
                    Assertions.assertEquals(numKeys, count);
                    rescaleSnapshotResult.clear();
                    backends.clear();
                }
            }
        } finally {
            for (RocksDBKeyedStateBackend<Integer> backend : backends) {
                IOUtils.closeQuietly(backend);
                backend.dispose();
            }
            for (SnapshotResult<KeyedStateHandle> snapshotResult : startSnapshotResult) {
                snapshotResult.discardState();
            }
            for (SnapshotResult<KeyedStateHandle> snapshotResult : rescaleSnapshotResult) {
                snapshotResult.discardState();
            }
            for (SnapshotResult<KeyedStateHandle> snapshotResult : cleanupSnapshotResult) {
                snapshotResult.discardState();
            }
        }
    }

    private void rescaleAndRestoreBackends(
            boolean useIngest,
            boolean asyncCompactAfterRescale,
            int targetParallelism,
            int maxParallelism,
            List<SnapshotResult<KeyedStateHandle>> snapshotResult,
            List<RocksDBKeyedStateBackend<Integer>> backendsOut)
            throws IOException {

        List<KeyedStateHandle> stateHandles =
                extractKeyedStateHandlesFromSnapshotResult(snapshotResult);
        List<KeyGroupRange> ranges = computeKeyGroupRanges(targetParallelism, maxParallelism);
        List<List<KeyedStateHandle>> handlesByInstance =
                computeHandlesByInstance(stateHandles, ranges, targetParallelism);

        OUTPUT.println(
                "Restoring using ingestDb="
                        + useIngest
                        + ", asyncCompact="
                        + asyncCompactAfterRescale
                        + "... ");

        OUTPUT.println(
                "Sum of snapshot sizes: "
                        + stateHandles.stream().mapToLong(StateObject::getStateSize).sum()
                                / (1024 * 1024)
                        + " MB");

        long maxInstanceTime = Long.MIN_VALUE;
        long t = System.currentTimeMillis();
        for (int i = 0; i < targetParallelism; ++i) {
            List<KeyedStateHandle> instanceHandles = handlesByInstance.get(i);
            long tInstance = System.currentTimeMillis();
            RocksDBKeyedStateBackend<Integer> backend =
                    RocksDBTestUtils.builderForTestDefaults(
                                    TempDirUtils.newFolder(tempFolder),
                                    IntSerializer.INSTANCE,
                                    maxParallelism,
                                    ranges.get(i),
                                    instanceHandles)
                            .setEnableIncrementalCheckpointing(true)
                            .setUseIngestDbRestoreMode(useIngest)
                            .setIncrementalRestoreAsyncCompactAfterRescale(asyncCompactAfterRescale)
                            .setRescalingUseDeleteFilesInRange(true)
                            .build();

            long instanceTime = System.currentTimeMillis() - tInstance;
            if (instanceTime > maxInstanceTime) {
                maxInstanceTime = instanceTime;
            }

            OUTPUT.println(
                    "    Restored instance "
                            + i
                            + " from "
                            + instanceHandles.size()
                            + " state handles"
                            + " time (ms): "
                            + instanceTime);

            backendsOut.add(backend);
        }
        OUTPUT.println("Total restore time (ms): " + (System.currentTimeMillis() - t));
        OUTPUT.println("Max restore time (ms): " + maxInstanceTime);
    }

    private void snapshotAllBackends(
            List<RocksDBKeyedStateBackend<Integer>> backends,
            List<SnapshotResult<KeyedStateHandle>> snapshotResultsOut)
            throws Exception {
        for (int i = 0; i < backends.size(); ++i) {
            RocksDBKeyedStateBackend<Integer> backend = backends.get(i);
            FsCheckpointStreamFactory fsCheckpointStreamFactory =
                    new FsCheckpointStreamFactory(
                            getSharedInstance(),
                            fromLocalFile(
                                    TempDirUtils.newFolder(
                                            tempFolder, "checkpointsDir_" + UUID.randomUUID() + i)),
                            fromLocalFile(
                                    TempDirUtils.newFolder(
                                            tempFolder, "sharedStateDir_" + UUID.randomUUID() + i)),
                            1,
                            4096);

            RunnableFuture<SnapshotResult<KeyedStateHandle>> snapshot =
                    backend.snapshot(
                            0L,
                            0L,
                            fsCheckpointStreamFactory,
                            CheckpointOptions.forCheckpointWithDefaultLocation());

            snapshot.run();
            snapshotResultsOut.add(snapshot.get());
        }
    }

    private List<KeyedStateHandle> extractKeyedStateHandlesFromSnapshotResult(
            List<SnapshotResult<KeyedStateHandle>> snapshotResults) {
        return snapshotResults.stream()
                .map(SnapshotResult::getJobManagerOwnedSnapshot)
                .collect(Collectors.toList());
    }

    private List<KeyGroupRange> computeKeyGroupRanges(int restoreParallelism, int maxParallelism) {
        List<KeyGroupRange> ranges = new ArrayList<>(restoreParallelism);
        for (int i = 0; i < restoreParallelism; ++i) {
            ranges.add(
                    KeyGroupRangeAssignment.computeKeyGroupRangeForOperatorIndex(
                            maxParallelism, restoreParallelism, i));
        }
        return ranges;
    }

    private List<List<KeyedStateHandle>> computeHandlesByInstance(
            List<KeyedStateHandle> stateHandles,
            List<KeyGroupRange> computedRanges,
            int restoreParallelism) {
        List<List<KeyedStateHandle>> handlesByInstance = new ArrayList<>(restoreParallelism);
        for (KeyGroupRange targetRange : computedRanges) {
            List<KeyedStateHandle> handlesForTargetRange = new ArrayList<>(1);
            handlesByInstance.add(handlesForTargetRange);

            for (KeyedStateHandle stateHandle : stateHandles) {
                if (stateHandle.getKeyGroupRange().getIntersection(targetRange)
                        != KeyGroupRange.EMPTY_KEY_GROUP_RANGE) {
                    handlesForTargetRange.add(stateHandle);
                }
            }
        }
        return handlesByInstance;
    }
}
