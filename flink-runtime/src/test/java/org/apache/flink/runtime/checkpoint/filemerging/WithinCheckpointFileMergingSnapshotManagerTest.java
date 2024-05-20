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
import org.apache.flink.util.function.BiFunctionWithException;

import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.util.Collections;

import static org.assertj.core.api.Assertions.assertThat;

/** Tests for {@link WithinCheckpointFileMergingSnapshotManager}. */
public class WithinCheckpointFileMergingSnapshotManagerTest
        extends FileMergingSnapshotManagerTestBase {
    @Override
    FileMergingType getFileMergingType() {
        return FileMergingType.MERGE_WITHIN_CHECKPOINT;
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
            assertThat(fmsm.spaceStat.physicalFileCount.get()).isEqualTo(1);
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
            assertThat(file4).isNotEqualTo(file1);
            assertThat(fmsm.spaceStat.physicalFileCount.get()).isEqualTo(4);

            // allocate for this checkpoint
            PhysicalFile file5 =
                    fmsm.getOrCreatePhysicalFileForCheckpoint(
                            subtaskKey1, 0, CheckpointedStateScope.SHARED);
            assertThat(file5.getFilePath().getParent())
                    .isEqualTo(fmsm.getManagedDir(subtaskKey1, CheckpointedStateScope.SHARED));
            assertThat(file5).isEqualTo(file1);
            assertThat(fmsm.spaceStat.physicalFileCount.get()).isEqualTo(4);

            // a physical file whose size is bigger than maxPhysicalFileSize cannot be reused
            file5.incSize(fmsm.maxPhysicalFileSize);
            fmsm.returnPhysicalFileForNextReuse(subtaskKey1, 0, file5);
            assertThat(fmsm.spaceStat.physicalFileCount.get()).isEqualTo(3);
            PhysicalFile file6 =
                    fmsm.getOrCreatePhysicalFileForCheckpoint(
                            subtaskKey1, 0, CheckpointedStateScope.SHARED);
            assertThat(file6.getFilePath().getParent())
                    .isEqualTo(fmsm.getManagedDir(subtaskKey1, CheckpointedStateScope.SHARED));
            assertThat(file6).isNotEqualTo(file5);
            assertThat(fmsm.spaceStat.physicalFileCount.get()).isEqualTo(4);

            // Secondly, we try private state
            PhysicalFile file7 =
                    fmsm.getOrCreatePhysicalFileForCheckpoint(
                            subtaskKey1, 0, CheckpointedStateScope.EXCLUSIVE);
            assertThat(file7.getFilePath().getParent())
                    .isEqualTo(fmsm.getManagedDir(subtaskKey1, CheckpointedStateScope.EXCLUSIVE));
            assertThat(fmsm.spaceStat.physicalFileCount.get()).isEqualTo(5);

            // allocate another
            PhysicalFile file8 =
                    fmsm.getOrCreatePhysicalFileForCheckpoint(
                            subtaskKey1, 0, CheckpointedStateScope.EXCLUSIVE);
            assertThat(file8.getFilePath().getParent())
                    .isEqualTo(fmsm.getManagedDir(subtaskKey1, CheckpointedStateScope.EXCLUSIVE));
            assertThat(file8).isNotEqualTo(file6);
            assertThat(fmsm.spaceStat.physicalFileCount.get()).isEqualTo(6);

            // return for reuse
            fmsm.returnPhysicalFileForNextReuse(subtaskKey1, 0, file7);
            assertThat(fmsm.spaceStat.physicalFileCount.get()).isEqualTo(6);

            // allocate for another checkpoint
            PhysicalFile file9 =
                    fmsm.getOrCreatePhysicalFileForCheckpoint(
                            subtaskKey1, 1, CheckpointedStateScope.EXCLUSIVE);
            assertThat(file9.getFilePath().getParent())
                    .isEqualTo(fmsm.getManagedDir(subtaskKey1, CheckpointedStateScope.EXCLUSIVE));
            assertThat(file9).isNotEqualTo(file7);
            assertThat(fmsm.spaceStat.physicalFileCount.get()).isEqualTo(7);

            // allocate for this checkpoint but another subtask
            PhysicalFile file10 =
                    fmsm.getOrCreatePhysicalFileForCheckpoint(
                            subtaskKey2, 0, CheckpointedStateScope.EXCLUSIVE);
            assertThat(file10.getFilePath().getParent())
                    .isEqualTo(fmsm.getManagedDir(subtaskKey2, CheckpointedStateScope.EXCLUSIVE));
            assertThat(file10).isEqualTo(file7);
            assertThat(fmsm.spaceStat.physicalFileCount.get()).isEqualTo(7);

            // a physical file whose size is bigger than maxPhysicalFileSize cannot be reused
            file10.incSize(fmsm.maxPhysicalFileSize);
            fmsm.returnPhysicalFileForNextReuse(subtaskKey1, 0, file10);
            assertThat(fmsm.spaceStat.physicalFileCount.get()).isEqualTo(6);
            PhysicalFile file11 =
                    fmsm.getOrCreatePhysicalFileForCheckpoint(
                            subtaskKey1, 0, CheckpointedStateScope.SHARED);
            assertThat(file11.getFilePath().getParent())
                    .isEqualTo(fmsm.getManagedDir(subtaskKey1, CheckpointedStateScope.SHARED));
            assertThat(file11).isNotEqualTo(file10);
            assertThat(fmsm.spaceStat.physicalFileCount.get()).isEqualTo(7);

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
            fmsm.registerSubtaskForSharedStates(subtaskKey1);
            fmsm.registerSubtaskForSharedStates(subtaskKey2);
            BiFunctionWithException<
                            FileMergingSnapshotManager.SubtaskKey,
                            Long,
                            SegmentFileStateHandle,
                            Exception>
                    writer =
                            ((subtaskKey, checkpointId) -> {
                                return writeCheckpointAndGetStream(
                                                subtaskKey,
                                                checkpointId,
                                                CheckpointedStateScope.SHARED,
                                                fmsm,
                                                closeableRegistry)
                                        .closeAndGetHandle();
                            });

            SegmentFileStateHandle cp1StateHandle1 = writer.apply(subtaskKey1, 1L);
            SegmentFileStateHandle cp1StateHandle2 = writer.apply(subtaskKey2, 1L);
            fmsm.notifyCheckpointComplete(subtaskKey1, 1);
            assertFileInManagedDir(fmsm, cp1StateHandle1);
            assertFileInManagedDir(fmsm, cp1StateHandle2);
            assertThat(fmsm.spaceStat.physicalFileCount.get()).isEqualTo(2);
            assertThat(fmsm.spaceStat.logicalFileCount.get()).isEqualTo(2);

            // complete checkpoint-2
            SegmentFileStateHandle cp2StateHandle1 = writer.apply(subtaskKey1, 2L);
            SegmentFileStateHandle cp2StateHandle2 = writer.apply(subtaskKey2, 2L);
            fmsm.reusePreviousStateHandle(2L, Collections.singleton(cp1StateHandle2));
            fmsm.notifyCheckpointComplete(subtaskKey1, 2);
            fmsm.notifyCheckpointComplete(subtaskKey2, 2);
            assertFileInManagedDir(fmsm, cp2StateHandle1);
            assertFileInManagedDir(fmsm, cp2StateHandle2);
            assertThat(fmsm.spaceStat.physicalFileCount.get()).isEqualTo(4);
            assertThat(fmsm.spaceStat.logicalFileCount.get()).isEqualTo(4);

            assertThat(fmsm.isCheckpointDiscard(1)).isFalse();

            // subsume checkpoint-1
            assertThat(fileExists(cp1StateHandle1)).isTrue();
            assertThat(fileExists(cp1StateHandle2)).isTrue();
            fmsm.notifyCheckpointSubsumed(subtaskKey1, 1);
            assertThat(fileExists(cp1StateHandle1)).isFalse();
            assertThat(fileExists(cp1StateHandle2)).isTrue();
            assertThat(fmsm.spaceStat.physicalFileCount.get()).isEqualTo(3);
            assertThat(fmsm.spaceStat.logicalFileCount.get()).isEqualTo(3);

            assertThat(fmsm.isCheckpointDiscard(1)).isFalse();
            fmsm.notifyCheckpointSubsumed(subtaskKey2, 1);
            assertThat(fmsm.isCheckpointDiscard(1)).isTrue();
            assertThat(fmsm.spaceStat.physicalFileCount.get()).isEqualTo(3);
            assertThat(fmsm.spaceStat.logicalFileCount.get()).isEqualTo(3);

            // abort checkpoint-3
            SegmentFileStateHandle cp3StateHandle1 = writer.apply(subtaskKey1, 3L);
            SegmentFileStateHandle cp3StateHandle2 = writer.apply(subtaskKey2, 3L);
            assertThat(fmsm.spaceStat.physicalFileCount.get()).isEqualTo(5);
            assertThat(fmsm.spaceStat.logicalFileCount.get()).isEqualTo(5);
            assertFileInManagedDir(fmsm, cp3StateHandle1);
            assertFileInManagedDir(fmsm, cp3StateHandle2);
            fmsm.notifyCheckpointAborted(subtaskKey1, 3);
            assertThat(fileExists(cp3StateHandle1)).isFalse();
            assertThat(fmsm.spaceStat.physicalFileCount.get()).isEqualTo(4);
            assertThat(fmsm.spaceStat.logicalFileCount.get()).isEqualTo(4);

            assertThat(fmsm.isCheckpointDiscard(3)).isFalse();
            fmsm.notifyCheckpointAborted(subtaskKey2, 3);
            assertThat(fmsm.isCheckpointDiscard(3)).isTrue();
            assertThat(fmsm.spaceStat.physicalFileCount.get()).isEqualTo(3);
            assertThat(fmsm.spaceStat.logicalFileCount.get()).isEqualTo(3);
        }
    }
}
