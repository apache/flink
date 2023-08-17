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

import org.apache.flink.api.common.TaskInfo;
import org.apache.flink.core.fs.FileSystem;
import org.apache.flink.core.fs.Path;
import org.apache.flink.core.fs.local.LocalFileSystem;
import org.apache.flink.runtime.checkpoint.filemerging.FileMergingSnapshotManager.SubtaskKey;
import org.apache.flink.runtime.jobgraph.OperatorID;
import org.apache.flink.runtime.state.CheckpointedStateScope;
import org.apache.flink.runtime.state.filesystem.AbstractFsCheckpointStorageAccess;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.io.IOException;

import static org.assertj.core.api.Assertions.assertThat;

/** Tests for {@link FileMergingSnapshotManager}. */
public class FileMergingSnapshotManagerTest {

    private final String tmId = "Testing";

    private final OperatorID operatorID = new OperatorID(289347923L, 75893479L);

    private SubtaskKey subtaskKey1;
    private SubtaskKey subtaskKey2;

    private Path checkpointBaseDir;

    @BeforeEach
    public void setup(@TempDir java.nio.file.Path tempFolder) {
        // use simplified job ids for the tests
        long jobId = 1;
        subtaskKey1 = new SubtaskKey(operatorID, new TaskInfo("TestingTask", 128, 0, 128, 3));
        subtaskKey2 = new SubtaskKey(operatorID, new TaskInfo("TestingTask", 128, 1, 128, 3));
        checkpointBaseDir = new Path(tempFolder.toString(), String.valueOf(jobId));
    }

    @Test
    void testCreateFileMergingSnapshotManager() throws IOException {
        try (FileMergingSnapshotManagerBase fmsm =
                (FileMergingSnapshotManagerBase)
                        createFileMergingSnapshotManager(checkpointBaseDir)) {
            fmsm.registerSubtaskForSharedStates(subtaskKey1);
            assertThat(fmsm.getManagedDir(subtaskKey1, CheckpointedStateScope.EXCLUSIVE))
                    .isEqualTo(
                            new Path(
                                    checkpointBaseDir,
                                    AbstractFsCheckpointStorageAccess
                                                    .CHECKPOINT_TASK_OWNED_STATE_DIR
                                            + "/"
                                            + tmId));
            assertThat(fmsm.getManagedDir(subtaskKey1, CheckpointedStateScope.SHARED))
                    .isEqualTo(
                            new Path(
                                    checkpointBaseDir,
                                    AbstractFsCheckpointStorageAccess.CHECKPOINT_SHARED_STATE_DIR
                                            + "/"
                                            + subtaskKey1.getManagedDirName()));
        }
    }

    @Test
    void testCreateAndReuseFiles() throws IOException {
        try (FileMergingSnapshotManagerBase fmsm =
                (FileMergingSnapshotManagerBase)
                        createFileMergingSnapshotManager(checkpointBaseDir)) {
            fmsm.registerSubtaskForSharedStates(subtaskKey1);
            fmsm.registerSubtaskForSharedStates(subtaskKey2);
            // firstly, we try shared state.
            PhysicalFile file1 =
                    fmsm.getOrCreatePhysicalFileForCheckpoint(
                            subtaskKey1, 0, CheckpointedStateScope.SHARED);
            assertThat(file1.getFilePath().getParent())
                    .isEqualTo(fmsm.getManagedDir(subtaskKey1, CheckpointedStateScope.SHARED));
            // allocate another
            PhysicalFile file2 =
                    fmsm.getOrCreatePhysicalFileForCheckpoint(
                            subtaskKey1, 0, CheckpointedStateScope.SHARED);
            assertThat(file2.getFilePath().getParent())
                    .isEqualTo(fmsm.getManagedDir(subtaskKey1, CheckpointedStateScope.SHARED));
            assertThat(file2).isNotEqualTo(file1);

            // return for reuse
            fmsm.returnPhysicalFileForNextReuse(subtaskKey1, 0, file1);

            // allocate for another subtask
            PhysicalFile file3 =
                    fmsm.getOrCreatePhysicalFileForCheckpoint(
                            subtaskKey2, 0, CheckpointedStateScope.SHARED);
            assertThat(file3.getFilePath().getParent())
                    .isEqualTo(fmsm.getManagedDir(subtaskKey2, CheckpointedStateScope.SHARED));
            assertThat(file3).isNotEqualTo(file1);

            // allocate for another checkpoint
            PhysicalFile file4 =
                    fmsm.getOrCreatePhysicalFileForCheckpoint(
                            subtaskKey1, 1, CheckpointedStateScope.SHARED);
            assertThat(file4.getFilePath().getParent())
                    .isEqualTo(fmsm.getManagedDir(subtaskKey1, CheckpointedStateScope.SHARED));
            assertThat(file4).isNotEqualTo(file1);

            // allocate for this checkpoint
            PhysicalFile file5 =
                    fmsm.getOrCreatePhysicalFileForCheckpoint(
                            subtaskKey1, 0, CheckpointedStateScope.SHARED);
            assertThat(file5.getFilePath().getParent())
                    .isEqualTo(fmsm.getManagedDir(subtaskKey1, CheckpointedStateScope.SHARED));
            assertThat(file5).isEqualTo(file1);

            // Secondly, we try private state
            PhysicalFile file6 =
                    fmsm.getOrCreatePhysicalFileForCheckpoint(
                            subtaskKey1, 0, CheckpointedStateScope.EXCLUSIVE);
            assertThat(file6.getFilePath().getParent())
                    .isEqualTo(fmsm.getManagedDir(subtaskKey1, CheckpointedStateScope.EXCLUSIVE));

            // allocate another
            PhysicalFile file7 =
                    fmsm.getOrCreatePhysicalFileForCheckpoint(
                            subtaskKey1, 0, CheckpointedStateScope.EXCLUSIVE);
            assertThat(file7.getFilePath().getParent())
                    .isEqualTo(fmsm.getManagedDir(subtaskKey1, CheckpointedStateScope.EXCLUSIVE));
            assertThat(file7).isNotEqualTo(file6);

            // return for reuse
            fmsm.returnPhysicalFileForNextReuse(subtaskKey1, 0, file6);

            // allocate for another checkpoint
            PhysicalFile file8 =
                    fmsm.getOrCreatePhysicalFileForCheckpoint(
                            subtaskKey1, 1, CheckpointedStateScope.EXCLUSIVE);
            assertThat(file8.getFilePath().getParent())
                    .isEqualTo(fmsm.getManagedDir(subtaskKey1, CheckpointedStateScope.EXCLUSIVE));
            assertThat(file8).isNotEqualTo(file6);

            // allocate for this checkpoint but another subtask
            PhysicalFile file9 =
                    fmsm.getOrCreatePhysicalFileForCheckpoint(
                            subtaskKey2, 0, CheckpointedStateScope.EXCLUSIVE);
            assertThat(file9.getFilePath().getParent())
                    .isEqualTo(fmsm.getManagedDir(subtaskKey2, CheckpointedStateScope.EXCLUSIVE));
            assertThat(file9).isEqualTo(file6);

            assertThat(fmsm.getManagedDir(subtaskKey2, CheckpointedStateScope.EXCLUSIVE))
                    .isEqualTo(fmsm.getManagedDir(subtaskKey1, CheckpointedStateScope.EXCLUSIVE));
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

    private FileMergingSnapshotManager createFileMergingSnapshotManager(Path checkpointBaseDir)
            throws IOException {
        FileSystem fs = LocalFileSystem.getSharedInstance();
        Path sharedStateDir =
                new Path(
                        checkpointBaseDir,
                        AbstractFsCheckpointStorageAccess.CHECKPOINT_SHARED_STATE_DIR);
        Path taskOwnedStateDir =
                new Path(
                        checkpointBaseDir,
                        AbstractFsCheckpointStorageAccess.CHECKPOINT_TASK_OWNED_STATE_DIR);
        if (!fs.exists(checkpointBaseDir)) {
            fs.mkdirs(checkpointBaseDir);
            fs.mkdirs(sharedStateDir);
            fs.mkdirs(taskOwnedStateDir);
        }
        FileMergingSnapshotManager fmsm = new FileMergingSnapshotManagerBuilder(tmId).build();
        fmsm.initFileSystem(
                LocalFileSystem.getSharedInstance(),
                checkpointBaseDir,
                sharedStateDir,
                taskOwnedStateDir);
        assertThat(fmsm).isNotNull();
        return fmsm;
    }
}
