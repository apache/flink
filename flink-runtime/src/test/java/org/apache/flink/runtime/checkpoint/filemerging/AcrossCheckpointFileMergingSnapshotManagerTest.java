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

import org.apache.flink.core.fs.CloseableRegistry;
import org.apache.flink.runtime.state.CheckpointedStateScope;
import org.apache.flink.runtime.state.filemerging.SegmentFileStateHandle;
import org.apache.flink.runtime.state.filesystem.FileMergingCheckpointStateOutputStream;

import org.junit.jupiter.api.Test;

import java.io.IOException;

import static org.assertj.core.api.Assertions.assertThat;

/** Tests for {@link AcrossCheckpointFileMergingSnapshotManager}. */
public class AcrossCheckpointFileMergingSnapshotManagerTest
        extends FileMergingSnapshotManagerTestBase {
    @Override
    FileMergingType getFileMergingType() {
        return FileMergingType.MERGE_ACROSS_CHECKPOINT;
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
            assertThat(fmsm.spaceStat.physicalFileCount.get()).isEqualTo(2);

            // return for reuse
            fmsm.returnPhysicalFileForNextReuse(subtaskKey1, 0, file1);
            assertThat(fmsm.spaceStat.physicalFileCount.get()).isEqualTo(2);

            // allocate for another subtask
            PhysicalFile file3 =
                    fmsm.getOrCreatePhysicalFileForCheckpoint(
                            subtaskKey2, 0, CheckpointedStateScope.SHARED);
            assertThat(file3.getFilePath().getParent())
                    .isEqualTo(fmsm.getManagedDir(subtaskKey2, CheckpointedStateScope.SHARED));
            assertThat(file3).isNotEqualTo(file1);
            assertThat(fmsm.spaceStat.physicalFileCount.get()).isEqualTo(3);

            // allocate for another checkpoint
            PhysicalFile file4 =
                    fmsm.getOrCreatePhysicalFileForCheckpoint(
                            subtaskKey1, 1, CheckpointedStateScope.SHARED);
            assertThat(file4.getFilePath().getParent())
                    .isEqualTo(fmsm.getManagedDir(subtaskKey1, CheckpointedStateScope.SHARED));
            assertThat(file4).isEqualTo(file1);
            assertThat(fmsm.spaceStat.physicalFileCount.get()).isEqualTo(3);

            // a physical file whose size is bigger than maxPhysicalFileSize cannot be reused
            file4.incSize(fmsm.maxPhysicalFileSize);
            fmsm.returnPhysicalFileForNextReuse(subtaskKey1, 1, file4);
            // file4 is discarded because it's size is bigger than maxPhysicalFileSize
            assertThat(fmsm.spaceStat.physicalFileCount.get()).isEqualTo(2);
            PhysicalFile file5 =
                    fmsm.getOrCreatePhysicalFileForCheckpoint(
                            subtaskKey1, 1, CheckpointedStateScope.SHARED);
            assertThat(file5.getFilePath().getParent())
                    .isEqualTo(fmsm.getManagedDir(subtaskKey1, CheckpointedStateScope.SHARED));
            assertThat(file5).isNotEqualTo(file4);
            assertThat(fmsm.spaceStat.physicalFileCount.get()).isEqualTo(3);

            // Secondly, we try private state
            PhysicalFile file6 =
                    fmsm.getOrCreatePhysicalFileForCheckpoint(
                            subtaskKey1, 1, CheckpointedStateScope.EXCLUSIVE);
            assertThat(file6.getFilePath().getParent())
                    .isEqualTo(fmsm.getManagedDir(subtaskKey1, CheckpointedStateScope.EXCLUSIVE));
            assertThat(fmsm.spaceStat.physicalFileCount.get()).isEqualTo(4);

            // allocate another
            PhysicalFile file7 =
                    fmsm.getOrCreatePhysicalFileForCheckpoint(
                            subtaskKey1, 1, CheckpointedStateScope.EXCLUSIVE);
            assertThat(file7.getFilePath().getParent())
                    .isEqualTo(fmsm.getManagedDir(subtaskKey1, CheckpointedStateScope.EXCLUSIVE));
            assertThat(file7).isNotEqualTo(file5);
            assertThat(fmsm.spaceStat.physicalFileCount.get()).isEqualTo(5);

            // return for reuse
            fmsm.returnPhysicalFileForNextReuse(subtaskKey1, 0, file6);
            assertThat(fmsm.spaceStat.physicalFileCount.get()).isEqualTo(5);

            // allocate for another checkpoint
            PhysicalFile file8 =
                    fmsm.getOrCreatePhysicalFileForCheckpoint(
                            subtaskKey1, 2, CheckpointedStateScope.EXCLUSIVE);
            assertThat(file8.getFilePath().getParent())
                    .isEqualTo(fmsm.getManagedDir(subtaskKey1, CheckpointedStateScope.EXCLUSIVE));
            assertThat(file8).isEqualTo(file6);
            assertThat(fmsm.spaceStat.physicalFileCount.get()).isEqualTo(5);

            // return for reuse
            fmsm.returnPhysicalFileForNextReuse(subtaskKey1, 0, file8);
            assertThat(fmsm.spaceStat.physicalFileCount.get()).isEqualTo(5);

            // allocate for this checkpoint but another subtask
            PhysicalFile file9 =
                    fmsm.getOrCreatePhysicalFileForCheckpoint(
                            subtaskKey2, 2, CheckpointedStateScope.EXCLUSIVE);
            assertThat(file9.getFilePath().getParent())
                    .isEqualTo(fmsm.getManagedDir(subtaskKey2, CheckpointedStateScope.EXCLUSIVE));
            assertThat(file9).isEqualTo(file6);
            assertThat(fmsm.spaceStat.physicalFileCount.get()).isEqualTo(5);

            // a physical file whose size is bigger than maxPhysicalFileSize cannot be reused
            file9.incSize(fmsm.maxPhysicalFileSize);
            fmsm.returnPhysicalFileForNextReuse(subtaskKey1, 2, file9);
            assertThat(fmsm.spaceStat.physicalFileCount.get()).isEqualTo(4);
            PhysicalFile file10 =
                    fmsm.getOrCreatePhysicalFileForCheckpoint(
                            subtaskKey1, 2, CheckpointedStateScope.SHARED);
            assertThat(file10.getFilePath().getParent())
                    .isEqualTo(fmsm.getManagedDir(subtaskKey1, CheckpointedStateScope.SHARED));
            assertThat(file10).isNotEqualTo(file9);
            assertThat(fmsm.spaceStat.physicalFileCount.get()).isEqualTo(5);

            assertThat(fmsm.getManagedDir(subtaskKey2, CheckpointedStateScope.EXCLUSIVE))
                    .isEqualTo(fmsm.getManagedDir(subtaskKey1, CheckpointedStateScope.EXCLUSIVE));
        }
    }

    @Test
    public void testCheckpointNotification() throws Exception {
        try (FileMergingSnapshotManagerBase fmsm =
                        (FileMergingSnapshotManagerBase)
                                createFileMergingSnapshotManager(checkpointBaseDir);
                CloseableRegistry closeableRegistry = new CloseableRegistry()) {
            FileMergingCheckpointStateOutputStream cp1Stream =
                    writeCheckpointAndGetStream(1, fmsm, closeableRegistry);
            SegmentFileStateHandle cp1StateHandle = cp1Stream.closeAndGetHandle();
            fmsm.notifyCheckpointComplete(subtaskKey1, 1);
            assertFileInManagedDir(fmsm, cp1StateHandle);
            assertThat(fmsm.spaceStat.physicalFileCount.get()).isEqualTo(1);
            assertThat(fmsm.spaceStat.logicalFileCount.get()).isEqualTo(1);
            // complete checkpoint-2
            FileMergingCheckpointStateOutputStream cp2Stream =
                    writeCheckpointAndGetStream(2, fmsm, closeableRegistry);
            SegmentFileStateHandle cp2StateHandle = cp2Stream.closeAndGetHandle();
            fmsm.notifyCheckpointComplete(subtaskKey1, 2);
            assertFileInManagedDir(fmsm, cp2StateHandle);
            assertThat(fmsm.spaceStat.physicalFileCount.get()).isEqualTo(1);
            assertThat(fmsm.spaceStat.logicalFileCount.get()).isEqualTo(2);

            // subsume checkpoint-1
            assertThat(fileExists(cp1StateHandle)).isTrue();
            fmsm.notifyCheckpointSubsumed(subtaskKey1, 1);
            assertThat(fileExists(cp1StateHandle)).isTrue();
            assertThat(fmsm.spaceStat.physicalFileCount.get()).isEqualTo(1);
            assertThat(fmsm.spaceStat.logicalFileCount.get()).isEqualTo(1);

            // abort checkpoint-3
            FileMergingCheckpointStateOutputStream cp3Stream =
                    writeCheckpointAndGetStream(3, fmsm, closeableRegistry);
            SegmentFileStateHandle cp3StateHandle = cp3Stream.closeAndGetHandle();
            assertThat(fmsm.spaceStat.physicalFileCount.get()).isEqualTo(1);
            assertThat(fmsm.spaceStat.logicalFileCount.get()).isEqualTo(2);
            assertFileInManagedDir(fmsm, cp3StateHandle);
            fmsm.notifyCheckpointAborted(subtaskKey1, 3);
            assertThat(fileExists(cp3StateHandle)).isTrue();
            assertThat(fmsm.spaceStat.physicalFileCount.get()).isEqualTo(1);
            assertThat(fmsm.spaceStat.logicalFileCount.get()).isEqualTo(1);
        }
    }
}
