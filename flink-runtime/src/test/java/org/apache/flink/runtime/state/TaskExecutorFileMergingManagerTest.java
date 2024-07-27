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
import org.apache.flink.configuration.Configuration;
import org.apache.flink.core.fs.Path;
import org.apache.flink.runtime.checkpoint.filemerging.FileMergingSnapshotManager;
import org.apache.flink.runtime.checkpoint.filemerging.FileMergingSnapshotManager.SubtaskKey;
import org.apache.flink.runtime.clusterframework.types.ResourceID;
import org.apache.flink.runtime.executiongraph.ExecutionAttemptID;
import org.apache.flink.runtime.metrics.groups.UnregisteredMetricGroups;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.io.IOException;

import static org.apache.flink.configuration.CheckpointingOptions.FILE_MERGING_ENABLED;
import static org.assertj.core.api.Assertions.assertThat;

/** Tests for {@link TaskExecutorFileMergingManager}. */
public class TaskExecutorFileMergingManagerTest {
    @Test
    public void testCheckpointScope(@TempDir java.nio.file.Path testBaseDir) throws IOException {
        ResourceID tmResourceId1 = ResourceID.generate();
        ResourceID tmResourceId2 = ResourceID.generate();
        ResourceID tmResourceId3 = ResourceID.generate();
        ResourceID tmResourceId4 = ResourceID.generate();

        TaskExecutorFileMergingManager taskExecutorFileMergingManager =
                new TaskExecutorFileMergingManager();
        JobID job1 = new JobID(1234L, 4321L);
        JobID job2 = new JobID(1234L, 5678L);
        SubtaskKey key1 = new SubtaskKey("test-jobId", "test-op1", 0, 128);
        SubtaskKey key2 = new SubtaskKey("test-jobId", "test-op2", 1, 128);
        Path checkpointDir1 = new Path(testBaseDir.toString(), "job1");
        Path checkpointDir2 = new Path(testBaseDir.toString(), "job2");
        int writeBufferSize = 4096;
        Configuration jobConfig = new Configuration();
        jobConfig.setBoolean(FILE_MERGING_ENABLED, true);
        Configuration clusterConfig = new Configuration();
        ExecutionAttemptID executionID1 = ExecutionAttemptID.randomId();
        FileMergingSnapshotManager manager1 =
                taskExecutorFileMergingManager.fileMergingSnapshotManagerForTask(
                        job1,
                        tmResourceId1,
                        executionID1,
                        clusterConfig,
                        jobConfig,
                        new UnregisteredMetricGroups.UnregisteredTaskManagerJobMetricGroup());
        manager1.initFileSystem(
                checkpointDir1.getFileSystem(),
                checkpointDir1,
                new Path(checkpointDir1, "shared"),
                new Path(checkpointDir1, "taskowned"),
                writeBufferSize);

        ExecutionAttemptID executionID2 = ExecutionAttemptID.randomId();
        FileMergingSnapshotManager manager2 =
                taskExecutorFileMergingManager.fileMergingSnapshotManagerForTask(
                        job1,
                        tmResourceId2,
                        executionID2,
                        clusterConfig,
                        jobConfig,
                        new UnregisteredMetricGroups.UnregisteredTaskManagerJobMetricGroup());
        manager2.initFileSystem(
                checkpointDir1.getFileSystem(),
                checkpointDir1,
                new Path(checkpointDir1, "shared"),
                new Path(checkpointDir1, "taskowned"),
                writeBufferSize);

        ExecutionAttemptID executionID3 = ExecutionAttemptID.randomId();
        FileMergingSnapshotManager manager3 =
                taskExecutorFileMergingManager.fileMergingSnapshotManagerForTask(
                        job2,
                        tmResourceId3,
                        executionID3,
                        clusterConfig,
                        jobConfig,
                        new UnregisteredMetricGroups.UnregisteredTaskManagerJobMetricGroup());
        manager3.initFileSystem(
                checkpointDir2.getFileSystem(),
                checkpointDir2,
                new Path(checkpointDir2, "shared"),
                new Path(checkpointDir2, "taskowned"),
                writeBufferSize);

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

        taskExecutorFileMergingManager.releaseMergingSnapshotManagerForTask(job1, executionID1);
        taskExecutorFileMergingManager.releaseMergingSnapshotManagerForTask(job1, executionID2);
        taskExecutorFileMergingManager.releaseMergingSnapshotManagerForTask(job2, executionID3);

        ExecutionAttemptID executionID4 = ExecutionAttemptID.randomId();
        FileMergingSnapshotManager manager4 =
                taskExecutorFileMergingManager.fileMergingSnapshotManagerForTask(
                        job1,
                        tmResourceId4,
                        executionID4,
                        clusterConfig,
                        jobConfig,
                        new UnregisteredMetricGroups.UnregisteredTaskManagerJobMetricGroup());
        assertThat(manager4).isNotEqualTo(manager1);
    }
}
