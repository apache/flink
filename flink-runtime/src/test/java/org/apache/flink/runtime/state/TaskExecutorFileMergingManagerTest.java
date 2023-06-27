/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.flink.runtime.state;

import org.apache.flink.api.common.JobID;
import org.apache.flink.core.fs.Path;
import org.apache.flink.runtime.checkpoint.filemerging.FileMergingSnapshotManager;
import org.apache.flink.runtime.checkpoint.filemerging.FileMergingSnapshotManager.SubtaskKey;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.io.IOException;

import static org.assertj.core.api.Assertions.assertThat;

/** Tests for {@link TaskExecutorFileMergingManager}. */
public class TaskExecutorFileMergingManagerTest {
    @Test
    public void testCheckpointScope(@TempDir java.nio.file.Path testBaseDir) throws IOException {
        TaskExecutorFileMergingManager taskExecutorFileMergingManager =
                new TaskExecutorFileMergingManager();
        JobID job1 = new JobID(1234L, 4321L);
        JobID job2 = new JobID(1234L, 5678L);
        SubtaskKey key1 = new SubtaskKey("test-op1", 0, 128);
        SubtaskKey key2 = new SubtaskKey("test-op2", 1, 128);
        Path checkpointDir1 = new Path(testBaseDir.toString(), "job1");
        Path checkpointDir2 = new Path(testBaseDir.toString(), "job2");
        FileMergingSnapshotManager manager1 =
                taskExecutorFileMergingManager.fileMergingSnapshotManagerForJob(job1);
        manager1.initFileSystem(
                checkpointDir1.getFileSystem(),
                checkpointDir1,
                new Path(checkpointDir1, "shared"),
                new Path(checkpointDir1, "taskowned"));
        FileMergingSnapshotManager manager2 =
                taskExecutorFileMergingManager.fileMergingSnapshotManagerForJob(job1);
        manager2.initFileSystem(
                checkpointDir1.getFileSystem(),
                checkpointDir1,
                new Path(checkpointDir1, "shared"),
                new Path(checkpointDir1, "taskowned"));
        FileMergingSnapshotManager manager3 =
                taskExecutorFileMergingManager.fileMergingSnapshotManagerForJob(job2);
        manager3.initFileSystem(
                checkpointDir2.getFileSystem(),
                checkpointDir2,
                new Path(checkpointDir2, "shared"),
                new Path(checkpointDir2, "taskowned"));

        assertThat(manager1).isEqualTo(manager2);
        assertThat(manager1).isNotEqualTo(manager3);

        // tasks of same job should have same private dir.
        assertThat(manager1.getManagedDir(key1, CheckpointedStateScope.EXCLUSIVE))
                .isEqualTo(manager2.getManagedDir(key2, CheckpointedStateScope.EXCLUSIVE));
        // tasks of different jobs should have different private dirs.
        assertThat(manager1.getManagedDir(key1, CheckpointedStateScope.EXCLUSIVE))
                .isNotEqualTo(manager3.getManagedDir(key2, CheckpointedStateScope.EXCLUSIVE));

        manager1.registerSubtaskForSharedStates(key1);
        manager1.registerSubtaskForSharedStates(key2);
        manager3.registerSubtaskForSharedStates(key1);
        manager3.registerSubtaskForSharedStates(key2);
        // tasks of same job should have different shared dirs for different subtasks.
        assertThat(manager1.getManagedDir(key1, CheckpointedStateScope.SHARED))
                .isNotEqualTo(manager1.getManagedDir(key2, CheckpointedStateScope.SHARED));
        // tasks with same SubtaskKey of different jobs should have different shared dirs.
        assertThat(manager1.getManagedDir(key1, CheckpointedStateScope.SHARED))
                .isNotEqualTo(manager3.getManagedDir(key1, CheckpointedStateScope.SHARED));
    }
}
