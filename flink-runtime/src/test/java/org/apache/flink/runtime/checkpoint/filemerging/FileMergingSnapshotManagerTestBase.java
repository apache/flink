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

package org.apache.flink.runtime.checkpoint.filemerging;

import org.apache.flink.api.common.JobID;
import org.apache.flink.api.common.TaskInfo;
import org.apache.flink.api.common.TaskInfoImpl;
import org.apache.flink.core.fs.CloseableRegistry;
import org.apache.flink.core.fs.FSDataInputStream;
import org.apache.flink.core.fs.FileStatus;
import org.apache.flink.core.fs.FileSystem;
import org.apache.flink.core.fs.Path;
import org.apache.flink.core.fs.local.LocalFileSystem;
import org.apache.flink.runtime.checkpoint.OperatorSubtaskState;
import org.apache.flink.runtime.checkpoint.TaskStateSnapshot;
import org.apache.flink.runtime.checkpoint.filemerging.FileMergingSnapshotManager.SpaceStat;
import org.apache.flink.runtime.checkpoint.filemerging.FileMergingSnapshotManager.SubtaskKey;
import org.apache.flink.runtime.clusterframework.types.ResourceID;
import org.apache.flink.runtime.jobgraph.OperatorID;
import org.apache.flink.runtime.metrics.groups.UnregisteredMetricGroups;
import org.apache.flink.runtime.state.CheckpointedStateScope;
import org.apache.flink.runtime.state.IncrementalKeyedStateHandle;
import org.apache.flink.runtime.state.IncrementalRemoteKeyedStateHandle;
import org.apache.flink.runtime.state.KeyGroupRange;
import org.apache.flink.runtime.state.KeyGroupRangeOffsets;
import org.apache.flink.runtime.state.KeyGroupsStateHandle;
import org.apache.flink.runtime.state.OperatorStateHandle;
import org.apache.flink.runtime.state.filemerging.FileMergingOperatorStreamStateHandle;
import org.apache.flink.runtime.state.filemerging.SegmentFileStateHandle;
import org.apache.flink.runtime.state.filesystem.FileMergingCheckpointStateOutputStream;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Random;
import java.util.Set;
import java.util.TreeMap;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.Future;
import java.util.stream.Collectors;

import static org.apache.flink.runtime.state.filesystem.AbstractFsCheckpointStorageAccess.CHECKPOINT_SHARED_STATE_DIR;
import static org.apache.flink.runtime.state.filesystem.AbstractFsCheckpointStorageAccess.CHECKPOINT_TASK_OWNED_STATE_DIR;
import static org.assertj.core.api.Assertions.assertThat;

/** Tests for {@link FileMergingSnapshotManager}. */
public abstract class FileMergingSnapshotManagerTestBase {

    final String tmId = "Testing";

    final JobID jobID = new JobID();

    final OperatorID operatorID = new OperatorID(289347923L, 75893479L);

    SubtaskKey subtaskKey1;
    SubtaskKey subtaskKey2;

    Path checkpointBaseDir;
    Path sharedStateDir;
    Path taskOwnedStateDir;

    int writeBufferSize;

    abstract FileMergingType getFileMergingType();

    @BeforeEach
    public void setup(@TempDir java.nio.file.Path tempFolder) {
        subtaskKey1 =
                new SubtaskKey(jobID, operatorID, new TaskInfoImpl("TestingTask", 128, 0, 128, 3));
        subtaskKey2 =
                new SubtaskKey(jobID, operatorID, new TaskInfoImpl("TestingTask", 128, 1, 128, 3));

        checkpointBaseDir = new Path(tempFolder.toString(), jobID.toHexString());
        sharedStateDir = new Path(checkpointBaseDir, CHECKPOINT_SHARED_STATE_DIR);
        taskOwnedStateDir = new Path(checkpointBaseDir, CHECKPOINT_TASK_OWNED_STATE_DIR);

        writeBufferSize = 4096;
    }

    @Test
    void testCreateFileMergingSnapshotManager() throws IOException {
        try (FileMergingSnapshotManagerBase fmsm =
                (FileMergingSnapshotManagerBase)
                        createFileMergingSnapshotManager(checkpointBaseDir)) {
            fmsm.registerSubtaskForSharedStates(subtaskKey1);

            String expectManagerId = String.format("job_%s_tm_%s", jobID, tmId);
            assertThat(fmsm.getManagedDir(subtaskKey1, CheckpointedStateScope.EXCLUSIVE))
                    .isEqualTo(new Path(taskOwnedStateDir, expectManagerId));
            assertThat(fmsm.getManagedDir(subtaskKey1, CheckpointedStateScope.SHARED))
                    .isEqualTo(new Path(sharedStateDir, subtaskKey1.getManagedDirName()));
        }
    }

    @Test
    public void testSpecialCharactersInPath() throws IOException {
        FileSystem fs = LocalFileSystem.getSharedInstance();
        if (!fs.exists(checkpointBaseDir)) {
            fs.mkdirs(checkpointBaseDir);
            fs.mkdirs(sharedStateDir);
            fs.mkdirs(taskOwnedStateDir);
        }
        // No exception will throw.
        try (FileMergingSnapshotManager fmsm =
                new FileMergingSnapshotManagerBuilder(
                                jobID,
                                new ResourceID("localhost:53424-,;:$&+=?/[]@#qqq"),
                                getFileMergingType())
                        .setMetricGroup(
                                new UnregisteredMetricGroups
                                        .UnregisteredTaskManagerJobMetricGroup())
                        .build()) {
            fmsm.initFileSystem(
                    LocalFileSystem.getSharedInstance(),
                    checkpointBaseDir,
                    sharedStateDir,
                    taskOwnedStateDir,
                    writeBufferSize);
            assertThat(fmsm).isNotNull();
            fmsm.registerSubtaskForSharedStates(
                    new SubtaskKey(jobID.toString(), ",;:$&+=?/[]@#www", 0, 1));
        }
    }

    @Test
    void testRefCountBetweenLogicalAndPhysicalFiles() throws IOException {
        try (FileMergingSnapshotManagerBase fmsm =
                (FileMergingSnapshotManagerBase)
                        createFileMergingSnapshotManager(checkpointBaseDir)) {
            fmsm.registerSubtaskForSharedStates(subtaskKey1);
            fmsm.registerSubtaskForSharedStates(subtaskKey2);

            PhysicalFile physicalFile1 =
                    fmsm.getOrCreatePhysicalFileForCheckpoint(
                            subtaskKey1, 0, CheckpointedStateScope.SHARED);
            assertThat(physicalFile1.isOpen()).isTrue();

            LogicalFile logicalFile1 = fmsm.createLogicalFile(physicalFile1, 0, 10, subtaskKey1);
            assertThat(logicalFile1.getSubtaskKey()).isEqualTo(subtaskKey1);
            assertThat(logicalFile1.getPhysicalFile()).isEqualTo(physicalFile1);
            assertThat(logicalFile1.getStartOffset()).isZero();
            assertThat(logicalFile1.getLength()).isEqualTo(10);
            assertThat(physicalFile1.getRefCount()).isOne();

            assertThat(logicalFile1.isDiscarded()).isFalse();
            logicalFile1.advanceLastCheckpointId(2);
            assertThat(logicalFile1.getLastUsedCheckpointID()).isEqualTo(2);
            logicalFile1.advanceLastCheckpointId(1);
            assertThat(logicalFile1.getLastUsedCheckpointID()).isEqualTo(2);
            logicalFile1.discardWithCheckpointId(1);
            assertThat(logicalFile1.isDiscarded()).isFalse();
            logicalFile1.discardWithCheckpointId(2);
            assertThat(logicalFile1.isDiscarded()).isTrue();

            // the stream is still open for reuse
            assertThat(physicalFile1.isOpen()).isTrue();
            assertThat(physicalFile1.isDeleted()).isFalse();
            assertThat(physicalFile1.getRefCount()).isZero();

            // duplicated discard takes no effect
            logicalFile1.discardWithCheckpointId(2);
            assertThat(physicalFile1.getRefCount()).isZero();

            physicalFile1.close();
            assertThat(physicalFile1.isOpen()).isFalse();
            assertThat(physicalFile1.isDeleted()).isTrue();

            // try close physical file but not deleted
            PhysicalFile physicalFile2 =
                    fmsm.getOrCreatePhysicalFileForCheckpoint(
                            subtaskKey1, 0, CheckpointedStateScope.SHARED);
            LogicalFile logicalFile2 = fmsm.createLogicalFile(physicalFile2, 0, 10, subtaskKey1);
            assertThat(logicalFile2.getPhysicalFile()).isEqualTo(physicalFile2);
            assertThat(logicalFile2.getStartOffset()).isZero();
            assertThat(logicalFile2.getLength()).isEqualTo(10);
            assertThat(physicalFile2.getRefCount()).isOne();
            logicalFile2.advanceLastCheckpointId(2);

            assertThat(physicalFile2.isOpen()).isTrue();
            assertThat(physicalFile2.isDeleted()).isFalse();
            physicalFile2.close();
            assertThat(physicalFile2.isOpen()).isFalse();
            assertThat(physicalFile2.isDeleted()).isFalse();
            assertThat(physicalFile2.getRefCount()).isOne();

            logicalFile2.discardWithCheckpointId(2);
            assertThat(logicalFile2.isDiscarded()).isTrue();
            assertThat(physicalFile2.isDeleted()).isTrue();
            assertThat(physicalFile2.getRefCount()).isZero();
        }
    }

    @Test
    void testSizeStatsInPhysicalFile() throws IOException {
        try (FileMergingSnapshotManagerBase fmsm =
                (FileMergingSnapshotManagerBase)
                        createFileMergingSnapshotManager(checkpointBaseDir)) {
            fmsm.registerSubtaskForSharedStates(subtaskKey1);
            fmsm.registerSubtaskForSharedStates(subtaskKey2);
            PhysicalFile physicalFile =
                    fmsm.getOrCreatePhysicalFileForCheckpoint(
                            subtaskKey1, 0, CheckpointedStateScope.SHARED);

            assertThat(physicalFile.getSize()).isZero();
            physicalFile.incSize(123);
            assertThat(physicalFile.getSize()).isEqualTo(123);
            physicalFile.incSize(456);
            assertThat(physicalFile.getSize()).isEqualTo(123 + 456);
        }
    }

    @Test
    void testSpaceStat() throws IOException {
        try (FileMergingSnapshotManagerBase fmsm =
                (FileMergingSnapshotManagerBase)
                        createFileMergingSnapshotManager(checkpointBaseDir)) {
            fmsm.registerSubtaskForSharedStates(subtaskKey1);
            fmsm.registerSubtaskForSharedStates(subtaskKey2);
            PhysicalFile physicalFile1 =
                    fmsm.getOrCreatePhysicalFileForCheckpoint(
                            subtaskKey1, 0, CheckpointedStateScope.SHARED);
            assertThat(physicalFile1.isOpen()).isTrue();

            LogicalFile logicalFile1 = fmsm.createLogicalFile(physicalFile1, 0, 123, subtaskKey1);
            assertThat(fmsm.spaceStat.physicalFileSize.get()).isEqualTo(123);
            assertThat(fmsm.spaceStat.logicalFileSize.get()).isEqualTo(123);
            assertThat(fmsm.spaceStat.physicalFileCount.get()).isEqualTo(1);
            assertThat(fmsm.spaceStat.logicalFileCount.get()).isEqualTo(1);
            assertThat(physicalFile1.getSize()).isEqualTo(123);

            LogicalFile logicalFile2 = fmsm.createLogicalFile(physicalFile1, 0, 456, subtaskKey1);
            assertThat(fmsm.spaceStat.physicalFileSize.get()).isEqualTo(123 + 456);
            assertThat(fmsm.spaceStat.logicalFileSize.get()).isEqualTo(123 + 456);
            assertThat(fmsm.spaceStat.physicalFileCount.get()).isEqualTo(1);
            assertThat(fmsm.spaceStat.logicalFileCount.get()).isEqualTo(2);
            assertThat(physicalFile1.getSize()).isEqualTo(123 + 456);

            logicalFile1.discardWithCheckpointId(1);
            fmsm.discardSingleLogicalFile(logicalFile1, 1);
            assertThat(fmsm.spaceStat.physicalFileSize.get()).isEqualTo(123 + 456);
            assertThat(fmsm.spaceStat.logicalFileSize.get()).isEqualTo(456);
            assertThat(fmsm.spaceStat.physicalFileCount.get()).isEqualTo(1);
            assertThat(fmsm.spaceStat.logicalFileCount.get()).isEqualTo(1);

            physicalFile1.close();
            fmsm.discardSingleLogicalFile(logicalFile2, 1);
            assertThat(fmsm.spaceStat.physicalFileSize.get()).isEqualTo(0);
            assertThat(fmsm.spaceStat.logicalFileSize.get()).isEqualTo(0);
            assertThat(fmsm.spaceStat.physicalFileCount.get()).isEqualTo(0);
            assertThat(fmsm.spaceStat.logicalFileCount.get()).isEqualTo(0);
        }
    }

    @Test
    public void testReusedFileWriting() throws Exception {
        long checkpointId = 1;
        int streamNum = 10;
        int perStreamWriteNum = 128;

        // write random bytes and then read them from the file
        byte[] bytes = new byte[streamNum * perStreamWriteNum];
        Random rd = new Random();
        rd.nextBytes(bytes);
        int byteIndex = 0;

        SegmentFileStateHandle[] handles = new SegmentFileStateHandle[streamNum];
        try (FileMergingSnapshotManager fmsm = createFileMergingSnapshotManager(checkpointBaseDir);
                CloseableRegistry closeableRegistry = new CloseableRegistry()) {

            // repeatedly get-write-close streams
            for (int i = 0; i < streamNum; i++) {
                FileMergingCheckpointStateOutputStream stream =
                        fmsm.createCheckpointStateOutputStream(
                                subtaskKey1, checkpointId, CheckpointedStateScope.EXCLUSIVE);
                try {
                    closeableRegistry.registerCloseable(stream);
                    for (int j = 0; j < perStreamWriteNum; j++) {
                        stream.write(bytes[byteIndex++]);
                    }
                    handles[i] = stream.closeAndGetHandle();
                } catch (IOException e) {
                    throw new RuntimeException(e);
                }
            }

            // assert the streams writes to the same file correctly
            byteIndex = 0;
            Path filePath = null;
            for (SegmentFileStateHandle handle : handles) {
                // check file path
                Path thisFilePath = handle.getFilePath();
                assertThat(filePath == null || filePath.equals(thisFilePath)).isTrue();
                filePath = thisFilePath;
                // check file content
                FSDataInputStream is = handle.openInputStream();

                closeableRegistry.registerCloseable(is);
                int readValue;

                while ((readValue = is.read()) != -1) {
                    assertThat((byte) readValue).isEqualTo(bytes[byteIndex++]);
                }
            }
        }
    }

    @Test
    public void testConcurrentWriting() throws Exception {
        long checkpointId = 1;
        int numThreads = 12;
        int perStreamWriteNum = 128;
        Set<Future<SegmentFileStateHandle>> futures = new HashSet<>();

        try (FileMergingSnapshotManager fmsm = createFileMergingSnapshotManager(checkpointBaseDir);
                CloseableRegistry closeableRegistry = new CloseableRegistry()) {
            // write data concurrently
            for (int i = 0; i < numThreads; i++) {
                futures.add(
                        CompletableFuture.supplyAsync(
                                () -> {
                                    FileMergingCheckpointStateOutputStream stream =
                                            fmsm.createCheckpointStateOutputStream(
                                                    subtaskKey1,
                                                    checkpointId,
                                                    CheckpointedStateScope.EXCLUSIVE);
                                    try {
                                        closeableRegistry.registerCloseable(stream);
                                        for (int j = 0; j < perStreamWriteNum; j++) {
                                            stream.write(j);
                                        }
                                        return stream.closeAndGetHandle();
                                    } catch (IOException e) {
                                        throw new RuntimeException(e);
                                    }
                                }));
            }

            // assert that multiple segments in the same file were not written concurrently
            for (Future<SegmentFileStateHandle> future : futures) {
                SegmentFileStateHandle segmentFileStateHandle = future.get();
                FSDataInputStream is = segmentFileStateHandle.openInputStream();
                closeableRegistry.registerCloseable(is);
                int readValue;
                int expected = 0;
                while ((readValue = is.read()) != -1) {
                    assertThat(readValue).isEqualTo(expected++);
                }
            }
        }
    }

    @Test
    public void testConcurrentFileReusingWithBlockingPool() throws Exception {
        try (FileMergingSnapshotManagerBase fmsm =
                (FileMergingSnapshotManagerBase)
                        createFileMergingSnapshotManager(
                                checkpointBaseDir,
                                32,
                                PhysicalFilePool.Type.BLOCKING,
                                Float.MAX_VALUE)) {
            fmsm.registerSubtaskForSharedStates(subtaskKey1);

            // test reusing a physical file
            PhysicalFile file1 =
                    fmsm.getOrCreatePhysicalFileForCheckpoint(
                            subtaskKey1, 0, CheckpointedStateScope.SHARED);
            fmsm.returnPhysicalFileForNextReuse(subtaskKey1, 0, file1);
            PhysicalFile file2 =
                    fmsm.getOrCreatePhysicalFileForCheckpoint(
                            subtaskKey1, 0, CheckpointedStateScope.SHARED);
            assertThat(file2).isEqualTo(file1);

            // a physical file whose size is bigger than maxPhysicalFileSize cannot be reused
            file2.incSize(fmsm.maxPhysicalFileSize);
            fmsm.returnPhysicalFileForNextReuse(subtaskKey1, 0, file2);
            PhysicalFile file3 =
                    fmsm.getOrCreatePhysicalFileForCheckpoint(
                            subtaskKey1, 0, CheckpointedStateScope.SHARED);
            assertThat(file3).isNotEqualTo(file2);

            // test for exclusive scope
            PhysicalFile file4 =
                    fmsm.getOrCreatePhysicalFileForCheckpoint(
                            subtaskKey1, 0, CheckpointedStateScope.EXCLUSIVE);
            fmsm.returnPhysicalFileForNextReuse(subtaskKey1, 0, file4);
            PhysicalFile file5 =
                    fmsm.getOrCreatePhysicalFileForCheckpoint(
                            subtaskKey1, 0, CheckpointedStateScope.EXCLUSIVE);
            assertThat(file5).isEqualTo(file4);

            file5.incSize(fmsm.maxPhysicalFileSize);
            fmsm.returnPhysicalFileForNextReuse(subtaskKey1, 0, file5);
            PhysicalFile file6 =
                    fmsm.getOrCreatePhysicalFileForCheckpoint(
                            subtaskKey1, 0, CheckpointedStateScope.EXCLUSIVE);
            assertThat(file6).isNotEqualTo(file5);
        }
    }

    @Test
    public void testReuseCallbackAndAdvanceWatermark() throws Exception {
        long checkpointId = 1;
        int streamNum = 20;
        int perStreamWriteNum = 128;

        // write random bytes and then read them from the file
        byte[] bytes = new byte[streamNum * perStreamWriteNum];
        Random rd = new Random();
        rd.nextBytes(bytes);
        int byteIndex = 0;

        SegmentFileStateHandle[] handles = new SegmentFileStateHandle[streamNum];
        try (FileMergingSnapshotManager fmsm = createFileMergingSnapshotManager(checkpointBaseDir);
                CloseableRegistry closeableRegistry = new CloseableRegistry()) {
            fmsm.registerSubtaskForSharedStates(subtaskKey1);

            // repeatedly get-write-close streams
            for (int i = 0; i < streamNum; i++) {
                FileMergingCheckpointStateOutputStream stream =
                        fmsm.createCheckpointStateOutputStream(
                                subtaskKey1, checkpointId, CheckpointedStateScope.SHARED);
                try {
                    closeableRegistry.registerCloseable(stream);
                    for (int j = 0; j < perStreamWriteNum; j++) {
                        stream.write(bytes[byteIndex++]);
                    }
                    handles[i] = stream.closeAndGetHandle();
                } catch (IOException e) {
                    throw new RuntimeException(e);
                }
            }

            // start reuse
            for (long cp = checkpointId + 1; cp <= 10; cp++) {
                ArrayList<SegmentFileStateHandle> reuse = new ArrayList<>();
                for (int j = 0; j <= 10 - cp; j++) {
                    reuse.add(handles[j]);
                }
                fmsm.reusePreviousStateHandle(cp, reuse);
                // assert the reusing affects the watermark
                for (SegmentFileStateHandle handle : reuse) {
                    assertThat(
                                    ((FileMergingSnapshotManagerBase) fmsm)
                                            .getLogicalFile(handle.getLogicalFileId())
                                            .getLastUsedCheckpointID())
                            .isEqualTo(cp);
                }
                // subsumed
                fmsm.notifyCheckpointSubsumed(subtaskKey1, cp - 1);
                // assert the other files discarded.
                for (int j = 10 - (int) cp + 1; j < streamNum; j++) {
                    assertThat(
                                    ((FileMergingSnapshotManagerBase) fmsm)
                                            .getLogicalFile(handles[j].getLogicalFileId()))
                            .isNull();
                }
            }
        }
    }

    @Test
    public void testRestore() throws Exception {
        TaskStateSnapshot taskStateSnapshot;
        long checkpointId = 222;
        SpaceStat oldSpaceStat;

        // Step1: build TaskStateSnapshot using FileMergingSnapshotManagerBase;
        try (FileMergingSnapshotManagerBase fmsm =
                        (FileMergingSnapshotManagerBase)
                                createFileMergingSnapshotManager(checkpointBaseDir);
                CloseableRegistry closeableRegistry = new CloseableRegistry()) {

            fmsm.notifyCheckpointStart(subtaskKey1, checkpointId);

            Map<OperatorID, OperatorSubtaskState> subtaskStatesByOperatorID = new HashMap<>();
            subtaskStatesByOperatorID.put(
                    operatorID, buildOperatorSubtaskState(checkpointId, fmsm, closeableRegistry));
            taskStateSnapshot = new TaskStateSnapshot(subtaskStatesByOperatorID);
            oldSpaceStat = fmsm.spaceStat;

            fmsm.notifyCheckpointComplete(subtaskKey1, checkpointId);
        }

        assertThat(taskStateSnapshot).isNotNull();

        // Step 2: restore FileMergingSnapshotManagerBase from the TaskStateSnapshot.
        try (FileMergingSnapshotManagerBase fmsm =
                (FileMergingSnapshotManagerBase)
                        createFileMergingSnapshotManager(checkpointBaseDir)) {
            TaskInfo taskInfo =
                    new TaskInfoImpl(
                            "test restore",
                            128,
                            subtaskKey1.subtaskIndex,
                            subtaskKey1.parallelism,
                            0);
            for (Map.Entry<OperatorID, OperatorSubtaskState> entry :
                    taskStateSnapshot.getSubtaskStateMappings()) {
                SubtaskFileMergingManagerRestoreOperation restoreOperation =
                        new SubtaskFileMergingManagerRestoreOperation(
                                checkpointId,
                                fmsm,
                                jobID,
                                taskInfo,
                                entry.getKey(),
                                entry.getValue());
                restoreOperation.restore();
            }
            TreeMap<Long, Set<LogicalFile>> stateFiles = fmsm.getUploadedStates();
            assertThat(stateFiles.size()).isEqualTo(1);
            Set<LogicalFile> restoreFileSet = stateFiles.get(checkpointId);
            assertThat(restoreFileSet).isNotNull();
            assertThat(restoreFileSet.size()).isEqualTo(4);
            assertThat(fmsm.spaceStat).isEqualTo(oldSpaceStat);
            for (LogicalFile file : restoreFileSet) {
                assertThat(fmsm.getLogicalFile(file.getFileId())).isEqualTo(file);
            }
            Set<Path> physicalFileSet =
                    restoreFileSet.stream()
                            .map(LogicalFile::getPhysicalFile)
                            .map(PhysicalFile::getFilePath)
                            .collect(Collectors.toSet());
            fmsm.notifyCheckpointSubsumed(subtaskKey1, checkpointId);
            for (Path path : physicalFileSet) {
                assertThat(path.getFileSystem().exists(path)).isFalse();
            }
        }
    }

    @Test
    public void testManagedDirCleanup() throws Exception {
        FileSystem fs = LocalFileSystem.getSharedInstance();

        Path sharedDirOfSubtask1 = new Path(sharedStateDir, subtaskKey1.getManagedDirName());
        Path sharedDirOfSubtask2 = new Path(sharedStateDir, subtaskKey2.getManagedDirName());
        Path exclusiveDir;

        // 1. Test clean up managed dir after non checkpoint triggered
        emptyCheckpointBaseDir();
        try (FileMergingSnapshotManagerBase fmsm =
                (FileMergingSnapshotManagerBase)
                        createFileMergingSnapshotManager(
                                checkpointBaseDir,
                                32,
                                PhysicalFilePool.Type.BLOCKING,
                                Float.MAX_VALUE)) {

            fmsm.registerSubtaskForSharedStates(subtaskKey1);
            fmsm.registerSubtaskForSharedStates(subtaskKey2);

            assertThat(fs.exists(sharedDirOfSubtask1)).isTrue();
            assertThat(fs.exists(sharedDirOfSubtask2)).isTrue();
            exclusiveDir = new Path(taskOwnedStateDir, fmsm.getId());
            assertThat(fs.exists(exclusiveDir)).isTrue();
        }
        assertThat(fs.exists(sharedDirOfSubtask1)).isFalse();
        assertThat(fs.exists(sharedDirOfSubtask2)).isFalse();
        assertThat(fs.exists(exclusiveDir)).isFalse();

        // 2. Test clean up managed dir after all checkpoint abort
        emptyCheckpointBaseDir();
        try (FileMergingSnapshotManagerBase fmsm =
                (FileMergingSnapshotManagerBase)
                        createFileMergingSnapshotManager(
                                checkpointBaseDir,
                                32,
                                PhysicalFilePool.Type.BLOCKING,
                                Float.MAX_VALUE)) {

            fmsm.registerSubtaskForSharedStates(subtaskKey1);
            fmsm.registerSubtaskForSharedStates(subtaskKey2);

            // record reference from checkpoint 1
            fmsm.notifyCheckpointStart(subtaskKey1, 1L);
            fmsm.notifyCheckpointStart(subtaskKey2, 1L);

            // checkpoint 1 aborted
            fmsm.notifyCheckpointAborted(subtaskKey1, 1L);
            fmsm.notifyCheckpointAborted(subtaskKey2, 1L);

            assertThat(fs.exists(sharedDirOfSubtask1)).isTrue();
            assertThat(fs.exists(sharedDirOfSubtask2)).isTrue();
            exclusiveDir = new Path(taskOwnedStateDir, fmsm.getId());
            assertThat(fs.exists(exclusiveDir)).isTrue();
        }
        assertThat(fs.exists(sharedDirOfSubtask1)).isFalse();
        assertThat(fs.exists(sharedDirOfSubtask2)).isFalse();
        assertThat(fs.exists(exclusiveDir)).isFalse();

        // 3. Test not clean up managed dir after checkpoint complete
        emptyCheckpointBaseDir();
        try (FileMergingSnapshotManagerBase fmsm =
                (FileMergingSnapshotManagerBase)
                        createFileMergingSnapshotManager(
                                checkpointBaseDir,
                                32,
                                PhysicalFilePool.Type.BLOCKING,
                                Float.MAX_VALUE)) {

            fmsm.registerSubtaskForSharedStates(subtaskKey1);
            fmsm.registerSubtaskForSharedStates(subtaskKey2);

            // record reference from checkpoint 1
            fmsm.notifyCheckpointStart(subtaskKey1, 1L);
            fmsm.notifyCheckpointStart(subtaskKey2, 1L);

            // checkpoint 1 complete
            fmsm.notifyCheckpointComplete(subtaskKey1, 1L);
            fmsm.notifyCheckpointComplete(subtaskKey2, 1L);

            assertThat(fs.exists(sharedDirOfSubtask1)).isTrue();
            assertThat(fs.exists(sharedDirOfSubtask2)).isTrue();
            exclusiveDir = new Path(taskOwnedStateDir, fmsm.getId());
            assertThat(fs.exists(exclusiveDir)).isTrue();
        }
        assertThat(fs.exists(sharedDirOfSubtask1)).isTrue();
        assertThat(fs.exists(sharedDirOfSubtask2)).isTrue();
        assertThat(fs.exists(exclusiveDir)).isTrue();
    }

    private void emptyCheckpointBaseDir() throws IOException {
        FileSystem fs = checkpointBaseDir.getFileSystem();
        FileStatus[] sub = fs.listStatus(checkpointBaseDir);
        if (sub != null) {
            for (FileStatus subFile : sub) {
                fs.delete(subFile.getPath(), true);
            }
        }
    }

    private OperatorSubtaskState buildOperatorSubtaskState(
            long checkpointId, FileMergingSnapshotManager fmsm, CloseableRegistry closeableRegistry)
            throws Exception {
        IncrementalRemoteKeyedStateHandle keyedStateHandle1 =
                new IncrementalRemoteKeyedStateHandle(
                        UUID.randomUUID(),
                        new KeyGroupRange(0, 8),
                        checkpointId,
                        Collections.singletonList(
                                IncrementalKeyedStateHandle.HandleAndLocalPath.of(
                                        buildOneSegmentFileHandle(
                                                checkpointId, fmsm, closeableRegistry),
                                        "localPath")),
                        Collections.emptyList(),
                        null);

        KeyGroupsStateHandle keyedStateHandle2 =
                new KeyGroupsStateHandle(
                        new KeyGroupRangeOffsets(0, 8),
                        buildOneSegmentFileHandle(checkpointId, fmsm, closeableRegistry));

        OperatorStateHandle operatorStateHandle1 =
                new FileMergingOperatorStreamStateHandle(
                        null,
                        null,
                        Collections.emptyMap(),
                        buildOneSegmentFileHandle(checkpointId, fmsm, closeableRegistry));

        OperatorStateHandle operatorStateHandle2 =
                new FileMergingOperatorStreamStateHandle(
                        null,
                        null,
                        Collections.emptyMap(),
                        buildOneSegmentFileHandle(checkpointId, fmsm, closeableRegistry));

        return OperatorSubtaskState.builder()
                .setManagedKeyedState(keyedStateHandle1)
                .setRawKeyedState(keyedStateHandle2)
                .setManagedOperatorState(operatorStateHandle1)
                .setRawOperatorState(operatorStateHandle2)
                .build();
    }

    private SegmentFileStateHandle buildOneSegmentFileHandle(
            long checkpointId, FileMergingSnapshotManager fmsm, CloseableRegistry closeableRegistry)
            throws Exception {
        FileMergingCheckpointStateOutputStream outputStream =
                writeCheckpointAndGetStream(checkpointId, fmsm, closeableRegistry);
        return outputStream.closeAndGetHandle();
    }

    FileMergingSnapshotManager createFileMergingSnapshotManager(Path checkpointBaseDir)
            throws IOException {
        return createFileMergingSnapshotManager(
                checkpointBaseDir, 32 * 1024 * 1024, PhysicalFilePool.Type.NON_BLOCKING, 2f);
    }

    FileMergingSnapshotManager createFileMergingSnapshotManager(
            Path checkpointBaseDir,
            long maxFileSize,
            PhysicalFilePool.Type filePoolType,
            float spaceAmplification)
            throws IOException {
        FileSystem fs = LocalFileSystem.getSharedInstance();
        if (!fs.exists(checkpointBaseDir)) {
            fs.mkdirs(checkpointBaseDir);
            fs.mkdirs(sharedStateDir);
            fs.mkdirs(taskOwnedStateDir);
        }
        FileMergingSnapshotManager fmsm =
                new FileMergingSnapshotManagerBuilder(
                                jobID, new ResourceID(tmId), getFileMergingType())
                        .setMaxFileSize(maxFileSize)
                        .setFilePoolType(filePoolType)
                        .setMaxSpaceAmplification(spaceAmplification)
                        .setMetricGroup(
                                new UnregisteredMetricGroups
                                        .UnregisteredTaskManagerJobMetricGroup())
                        .build();
        fmsm.initFileSystem(
                LocalFileSystem.getSharedInstance(),
                checkpointBaseDir,
                sharedStateDir,
                taskOwnedStateDir,
                writeBufferSize);
        assertThat(fmsm).isNotNull();
        return fmsm;
    }

    FileMergingCheckpointStateOutputStream writeCheckpointAndGetStream(
            long checkpointId, FileMergingSnapshotManager fmsm, CloseableRegistry closeableRegistry)
            throws IOException {
        return writeCheckpointAndGetStream(
                subtaskKey1,
                checkpointId,
                CheckpointedStateScope.EXCLUSIVE,
                fmsm,
                closeableRegistry,
                32);
    }

    FileMergingCheckpointStateOutputStream writeCheckpointAndGetStream(
            SubtaskKey subtaskKey,
            long checkpointId,
            CheckpointedStateScope scope,
            FileMergingSnapshotManager fmsm,
            CloseableRegistry closeableRegistry)
            throws IOException {
        return writeCheckpointAndGetStream(
                subtaskKey, checkpointId, scope, fmsm, closeableRegistry, 32);
    }

    FileMergingCheckpointStateOutputStream writeCheckpointAndGetStream(
            SubtaskKey subtaskKey,
            long checkpointId,
            CheckpointedStateScope scope,
            FileMergingSnapshotManager fmsm,
            CloseableRegistry closeableRegistry,
            int numBytes)
            throws IOException {
        FileMergingCheckpointStateOutputStream stream =
                fmsm.createCheckpointStateOutputStream(subtaskKey, checkpointId, scope);
        closeableRegistry.registerCloseable(stream);
        for (int i = 0; i < numBytes; i++) {
            stream.write(i);
        }
        return stream;
    }

    void assertFileInManagedDir(
            FileMergingSnapshotManager fmsm, SegmentFileStateHandle stateHandle) {
        assertThat(fmsm instanceof FileMergingSnapshotManagerBase).isTrue();
        assertThat(stateHandle).isNotNull();
        Path filePath = stateHandle.getFilePath();
        assertThat(filePath).isNotNull();
        assertThat(((FileMergingSnapshotManagerBase) fmsm).isResponsibleForFile(filePath)).isTrue();
    }

    boolean fileExists(SegmentFileStateHandle stateHandle) throws IOException {
        assertThat(stateHandle).isNotNull();
        Path filePath = stateHandle.getFilePath();
        assertThat(filePath).isNotNull();
        return filePath.getFileSystem().exists(filePath);
    }
}
