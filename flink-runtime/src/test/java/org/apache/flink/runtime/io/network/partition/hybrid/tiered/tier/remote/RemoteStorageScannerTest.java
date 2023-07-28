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

package org.apache.flink.runtime.io.network.partition.hybrid.tiered.tier.remote;

import org.apache.flink.core.fs.FileSystem;
import org.apache.flink.core.fs.Path;
import org.apache.flink.runtime.io.network.partition.ResultPartitionID;
import org.apache.flink.runtime.io.network.partition.hybrid.tiered.common.TieredStorageIdMappingUtils;
import org.apache.flink.runtime.io.network.partition.hybrid.tiered.common.TieredStoragePartitionId;
import org.apache.flink.runtime.io.network.partition.hybrid.tiered.common.TieredStorageSubpartitionId;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.io.File;
import java.io.IOException;
import java.io.OutputStream;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;

import static org.apache.flink.runtime.io.network.partition.hybrid.tiered.file.SegmentPartitionFile.getSegmentPath;
import static org.apache.flink.runtime.io.network.partition.hybrid.tiered.file.SegmentPartitionFile.writeSegmentFinishFile;
import static org.assertj.core.api.Assertions.assertThat;

/** Tests for {@link RemoteStorageScanner}. */
class RemoteStorageScannerTest {

    private static final TieredStoragePartitionId DEFAULT_PARTITION_ID =
            TieredStorageIdMappingUtils.convertId(new ResultPartitionID());

    private static final TieredStorageSubpartitionId DEFAULT_SUBPARTITION_ID =
            new TieredStorageSubpartitionId(0);

    @TempDir private File tempFolder;

    private String remoteStoragePath;

    @BeforeEach
    void before() {
        remoteStoragePath = Path.fromLocalFile(tempFolder).getPath();
    }

    @Test
    void testWatchSegment() throws IOException {
        CompletableFuture<Void> future = new CompletableFuture<>();
        TestingAvailabilityNotifier notifier =
                new TestingAvailabilityNotifier.Builder()
                        .setNotifyFunction(((partitionId, subpartitionId) -> future))
                        .build();

        // Create segment files with id 2, and finish file with id 2.
        createSegmentFile(2);
        createSegmentFinishFile(2);

        RemoteStorageScanner remoteStorageScanner = new RemoteStorageScanner(remoteStoragePath);
        remoteStorageScanner.registerAvailabilityAndPriorityNotifier(notifier);
        remoteStorageScanner.watchSegment(DEFAULT_PARTITION_ID, DEFAULT_SUBPARTITION_ID, 2);
        remoteStorageScanner.run();
        assertThat(future).isNotDone();
        remoteStorageScanner.run();
        assertThat(future).isDone();
    }

    @Test
    void testWatchSegmentIgnored() throws IOException {
        CompletableFuture<Void> future = new CompletableFuture<>();
        TestingAvailabilityNotifier notifier =
                new TestingAvailabilityNotifier.Builder()
                        .setNotifyFunction(((partitionId, subpartitionId) -> future))
                        .build();

        // Create segment files with id 2 and id 3, and finish file with id 3.
        createSegmentFile(2);
        createSegmentFile(3);
        createSegmentFinishFile(3);

        // If the larger segment id has been scanned, the smaller segment ids will be ignored and
        // not be watched.
        RemoteStorageScanner remoteStorageScanner = new RemoteStorageScanner(remoteStoragePath);
        remoteStorageScanner.registerAvailabilityAndPriorityNotifier(notifier);
        remoteStorageScanner.run();
        remoteStorageScanner.watchSegment(DEFAULT_PARTITION_ID, DEFAULT_SUBPARTITION_ID, 0);
        remoteStorageScanner.run();
        assertThat(future).isNotDone();
        remoteStorageScanner.watchSegment(DEFAULT_PARTITION_ID, DEFAULT_SUBPARTITION_ID, 1);
        remoteStorageScanner.run();
        assertThat(future).isNotDone();
        remoteStorageScanner.watchSegment(DEFAULT_PARTITION_ID, DEFAULT_SUBPARTITION_ID, 2);
        remoteStorageScanner.run();
        assertThat(future).isNotDone();
    }

    @Test
    void testStartAndClose() throws IOException, ExecutionException, InterruptedException {
        CompletableFuture<Void> future = new CompletableFuture<>();
        TestingAvailabilityNotifier notifier =
                new TestingAvailabilityNotifier.Builder()
                        .setNotifyFunction(((partitionId, subpartitionId) -> future))
                        .build();
        createSegmentFile(0);
        createSegmentFinishFile(0);
        RemoteStorageScanner remoteStorageScanner = new RemoteStorageScanner(remoteStoragePath);
        remoteStorageScanner.registerAvailabilityAndPriorityNotifier(notifier);
        remoteStorageScanner.watchSegment(DEFAULT_PARTITION_ID, DEFAULT_SUBPARTITION_ID, 0);
        remoteStorageScanner.start();
        future.get();
    }

    @Test
    void testScanStrategy() {
        int maxScanIntervalMs = 10_000;
        RemoteStorageScanner.ScanStrategy scanStrategy =
                new RemoteStorageScanner.ScanStrategy(maxScanIntervalMs);
        int currentScanIntervalMs = 100;
        assertThat(scanStrategy.getInterval(currentScanIntervalMs))
                .isEqualTo(currentScanIntervalMs * 2);
        currentScanIntervalMs = 6_000;
        assertThat(scanStrategy.getInterval(currentScanIntervalMs)).isEqualTo(maxScanIntervalMs);
        currentScanIntervalMs = 12_000;
        assertThat(scanStrategy.getInterval(currentScanIntervalMs)).isEqualTo(maxScanIntervalMs);
    }

    private void createSegmentFile(int segmentId) throws IOException {
        Path segmentPath =
                getSegmentPath(
                        remoteStoragePath,
                        DEFAULT_PARTITION_ID,
                        DEFAULT_SUBPARTITION_ID.getSubpartitionId(),
                        segmentId);
        OutputStream outputStream =
                segmentPath.getFileSystem().create(segmentPath, FileSystem.WriteMode.OVERWRITE);
        outputStream.close();
    }

    private void createSegmentFinishFile(int segmentId) throws IOException {
        writeSegmentFinishFile(
                remoteStoragePath,
                DEFAULT_PARTITION_ID,
                DEFAULT_SUBPARTITION_ID.getSubpartitionId(),
                segmentId);
    }
}
