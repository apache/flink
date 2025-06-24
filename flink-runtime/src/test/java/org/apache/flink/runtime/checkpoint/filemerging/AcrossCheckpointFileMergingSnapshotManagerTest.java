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
            fmsm.notifyCheckpointComplete(subtaskKey1, 2);
            fmsm.notifyCheckpointComplete(subtaskKey2, 2);
            assertFileInManagedDir(fmsm, cp2StateHandle1);
            assertFileInManagedDir(fmsm, cp2StateHandle2);
            assertThat(fmsm.spaceStat.physicalFileCount.get()).isEqualTo(2);
            assertThat(fmsm.spaceStat.logicalFileCount.get()).isEqualTo(4);

            assertThat(fmsm.isCheckpointDiscard(1)).isFalse();

            // subsume checkpoint-1
            assertThat(fileExists(cp1StateHandle1)).isTrue();
            assertThat(fileExists(cp1StateHandle2)).isTrue();
            fmsm.notifyCheckpointSubsumed(subtaskKey1, 1);
            assertThat(fileExists(cp1StateHandle1)).isTrue();
            assertThat(fileExists(cp1StateHandle2)).isTrue();
            assertThat(fmsm.spaceStat.physicalFileCount.get()).isEqualTo(2);
            assertThat(fmsm.spaceStat.logicalFileCount.get()).isEqualTo(3);

            assertThat(fmsm.isCheckpointDiscard(1)).isFalse();
            fmsm.notifyCheckpointSubsumed(subtaskKey2, 1);
            assertThat(fmsm.isCheckpointDiscard(1)).isTrue();
            assertThat(fmsm.spaceStat.physicalFileCount.get()).isEqualTo(2);
            assertThat(fmsm.spaceStat.logicalFileCount.get()).isEqualTo(2);

            // abort checkpoint-3
            SegmentFileStateHandle cp3StateHandle1 = writer.apply(subtaskKey1, 3L);
            SegmentFileStateHandle cp3StateHandle2 = writer.apply(subtaskKey2, 3L);
            assertThat(fmsm.spaceStat.physicalFileCount.get()).isEqualTo(2);
            assertThat(fmsm.spaceStat.logicalFileCount.get()).isEqualTo(4);
            assertFileInManagedDir(fmsm, cp3StateHandle1);
            assertFileInManagedDir(fmsm, cp3StateHandle2);
            fmsm.notifyCheckpointAborted(subtaskKey1, 3);
            assertThat(fileExists(cp3StateHandle1)).isTrue();
            assertThat(fmsm.spaceStat.physicalFileCount.get()).isEqualTo(2);
            assertThat(fmsm.spaceStat.logicalFileCount.get()).isEqualTo(3);

            assertThat(fmsm.isCheckpointDiscard(3)).isFalse();
            fmsm.notifyCheckpointAborted(subtaskKey2, 3);
            assertThat(fmsm.isCheckpointDiscard(3)).isTrue();
            assertThat(fmsm.spaceStat.physicalFileCount.get()).isEqualTo(2);
            assertThat(fmsm.spaceStat.logicalFileCount.get()).isEqualTo(2);
        }
    }

    @Test
    public void testSpaceControl() throws Exception {
        try (FileMergingSnapshotManagerBase fmsm =
                        (FileMergingSnapshotManagerBase)
                                createFileMergingSnapshotManager(checkpointBaseDir);
                CloseableRegistry closeableRegistry = new CloseableRegistry()) {

            fmsm.registerSubtaskForSharedStates(subtaskKey1);

            BiFunctionWithException<Long, Integer, SegmentFileStateHandle, Exception> writer =
                    ((checkpointId, size) -> {
                        return writeCheckpointAndGetStream(
                                        subtaskKey1,
                                        checkpointId,
                                        CheckpointedStateScope.SHARED,
                                        fmsm,
                                        closeableRegistry,
                                        size)
                                .closeAndGetHandle();
                    });
            Integer eighthOfFile = 4 * 1024 * 1024;

            // Doing checkpoint-1 with 6 files
            SegmentFileStateHandle stateHandle1 = writer.apply(1L, eighthOfFile);
            SegmentFileStateHandle stateHandle2 = writer.apply(1L, eighthOfFile);
            SegmentFileStateHandle stateHandle3 = writer.apply(1L, eighthOfFile);
            SegmentFileStateHandle stateHandle4 = writer.apply(1L, eighthOfFile);
            SegmentFileStateHandle stateHandle5 = writer.apply(1L, eighthOfFile);
            SegmentFileStateHandle stateHandle6 = writer.apply(1L, eighthOfFile);

            fmsm.notifyCheckpointComplete(subtaskKey1, 1);
            assertThat(fmsm.spaceStat.physicalFileCount.get()).isEqualTo(1);
            assertThat(fmsm.spaceStat.logicalFileCount.get()).isEqualTo(6);

            // complete checkpoint-2 with 3 files written and 1 file reused from checkpoint 1
            assertThat(fmsm.couldReusePreviousStateHandle(stateHandle1)).isTrue();
            SegmentFileStateHandle stateHandle7 = writer.apply(2L, eighthOfFile);
            SegmentFileStateHandle stateHandle8 = writer.apply(2L, eighthOfFile);
            SegmentFileStateHandle stateHandle9 = writer.apply(2L, eighthOfFile);
            fmsm.reusePreviousStateHandle(2, Collections.singletonList(stateHandle1));
            fmsm.notifyCheckpointComplete(subtaskKey1, 2);
            assertThat(fmsm.spaceStat.physicalFileCount.get()).isEqualTo(2);
            assertThat(fmsm.spaceStat.logicalFileCount.get()).isEqualTo(9);

            // subsume checkpoint-1
            fmsm.notifyCheckpointSubsumed(subtaskKey1, 1);
            assertThat(fmsm.spaceStat.physicalFileCount.get()).isEqualTo(2);
            assertThat(fmsm.spaceStat.logicalFileCount.get()).isEqualTo(4);

            // complete checkpoint-3 with 1 files reuse from checkpoint 1 and 2.
            assertThat(fmsm.couldReusePreviousStateHandle(stateHandle1)).isFalse();
            assertThat(fmsm.couldReusePreviousStateHandle(stateHandle7)).isFalse();
            assertThat(fmsm.couldReusePreviousStateHandle(stateHandle9)).isTrue();
            SegmentFileStateHandle stateHandle10 = writer.apply(3L, eighthOfFile);
            SegmentFileStateHandle stateHandle11 = writer.apply(3L, eighthOfFile);
            SegmentFileStateHandle stateHandle12 = writer.apply(3L, eighthOfFile);

            fmsm.notifyCheckpointComplete(subtaskKey1, 3);
            assertThat(fmsm.spaceStat.physicalFileCount.get()).isEqualTo(2);
            assertThat(fmsm.spaceStat.logicalFileCount.get()).isEqualTo(7);

            // subsume checkpoint-2
            fmsm.notifyCheckpointSubsumed(subtaskKey1, 2);
            assertThat(fmsm.spaceStat.physicalFileCount.get()).isEqualTo(1);
            assertThat(fmsm.spaceStat.logicalFileCount.get()).isEqualTo(3);
        }
    }
}
