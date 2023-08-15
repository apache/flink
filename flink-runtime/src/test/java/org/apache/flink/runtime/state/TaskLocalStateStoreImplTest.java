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
import org.apache.flink.runtime.checkpoint.OperatorSubtaskState;
import org.apache.flink.runtime.checkpoint.TaskStateSnapshot;
import org.apache.flink.runtime.clusterframework.types.AllocationID;
import org.apache.flink.runtime.jobgraph.JobVertexID;
import org.apache.flink.runtime.jobgraph.OperatorID;
import org.apache.flink.testutils.junit.utils.TempDirUtils;
import org.apache.flink.util.concurrent.Executors;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import javax.annotation.Nonnull;

import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;

/** Test for the {@link TaskLocalStateStoreImpl}. */
class TaskLocalStateStoreImplTest {

    protected @TempDir Path temporaryFolder;
    protected File[] allocationBaseDirs;
    protected TaskLocalStateStoreImpl taskLocalStateStore;
    protected JobID jobID;
    protected AllocationID allocationID;
    protected JobVertexID jobVertexID;
    protected int subtaskIdx;

    @BeforeEach
    void before() throws Exception {
        jobID = new JobID();
        allocationID = new AllocationID();
        jobVertexID = new JobVertexID();
        subtaskIdx = 0;
        this.allocationBaseDirs =
                new File[] {
                    TempDirUtils.newFolder(temporaryFolder), TempDirUtils.newFolder(temporaryFolder)
                };

        this.taskLocalStateStore =
                createTaskLocalStateStoreImpl(
                        allocationBaseDirs, jobID, allocationID, jobVertexID, subtaskIdx);
    }

    @Nonnull
    private TaskLocalStateStoreImpl createTaskLocalStateStoreImpl(
            File[] allocationBaseDirs,
            JobID jobID,
            AllocationID allocationID,
            JobVertexID jobVertexID,
            int subtaskIdx) {
        LocalRecoveryDirectoryProviderImpl directoryProvider =
                new LocalRecoveryDirectoryProviderImpl(
                        allocationBaseDirs, jobID, jobVertexID, subtaskIdx);

        LocalRecoveryConfig localRecoveryConfig = new LocalRecoveryConfig(directoryProvider);
        return new TaskLocalStateStoreImpl(
                jobID,
                allocationID,
                jobVertexID,
                subtaskIdx,
                localRecoveryConfig,
                Executors.directExecutor());
    }

    /** Test that the instance delivers a correctly configured LocalRecoveryDirectoryProvider. */
    @Test
    void getLocalRecoveryRootDirectoryProvider() {

        LocalRecoveryConfig directoryProvider = taskLocalStateStore.getLocalRecoveryConfig();
        assertThat(
                        directoryProvider
                                .getLocalStateDirectoryProvider()
                                .get()
                                .allocationBaseDirsCount())
                .isEqualTo(allocationBaseDirs.length);

        for (int i = 0; i < allocationBaseDirs.length; ++i) {
            assertThat(
                            directoryProvider
                                    .getLocalStateDirectoryProvider()
                                    .get()
                                    .selectAllocationBaseDirectory(i))
                    .isEqualTo(allocationBaseDirs[i]);
        }
    }

    /** Tests basic store/retrieve of local state. */
    @Test
    void storeAndRetrieve() throws Exception {

        final int chkCount = 3;

        for (int i = 0; i < chkCount; ++i) {
            assertThat(taskLocalStateStore.retrieveLocalState(i)).isNull();
        }

        List<TestingTaskStateSnapshot> taskStateSnapshots = storeStates(chkCount);

        checkStoredAsExpected(taskStateSnapshots, 0, chkCount);

        assertThat(taskLocalStateStore.retrieveLocalState(chkCount + 1)).isNull();
    }

    /** Test checkpoint pruning. */
    @Test
    void pruneCheckpoints() throws Exception {

        final int chkCount = 3;

        List<TestingTaskStateSnapshot> taskStateSnapshots = storeStates(chkCount);

        // test retrieve with pruning
        taskLocalStateStore.pruneMatchingCheckpoints((long chk) -> chk != chkCount - 1);

        for (int i = 0; i < chkCount - 1; ++i) {
            assertThat(taskLocalStateStore.retrieveLocalState(i)).isNull();
        }

        checkStoredAsExpected(taskStateSnapshots, chkCount - 1, chkCount);
    }

    /** Tests pruning of previous checkpoints if a new checkpoint is confirmed. */
    @Test
    void confirmCheckpoint() throws Exception {

        final int chkCount = 3;
        final int confirmed = chkCount - 1;
        List<TestingTaskStateSnapshot> taskStateSnapshots = storeStates(chkCount);
        taskLocalStateStore.confirmCheckpoint(confirmed);
        checkPrunedAndDiscarded(taskStateSnapshots, 0, confirmed);
        checkStoredAsExpected(taskStateSnapshots, confirmed, chkCount);
    }

    /** Tests pruning of target previous checkpoints if that checkpoint is aborted. */
    @Test
    void abortCheckpoint() throws Exception {

        final int chkCount = 4;
        final int aborted = chkCount - 2;
        List<TestingTaskStateSnapshot> taskStateSnapshots = storeStates(chkCount);
        taskLocalStateStore.abortCheckpoint(aborted);
        checkPrunedAndDiscarded(taskStateSnapshots, aborted, aborted + 1);
        checkStoredAsExpected(taskStateSnapshots, 0, aborted);
        checkStoredAsExpected(taskStateSnapshots, aborted + 1, chkCount);
    }

    /**
     * Tests that disposal of a {@link TaskLocalStateStoreImpl} works and discards all local states.
     */
    @Test
    void dispose() throws Exception {
        final int chkCount = 3;
        final int confirmed = chkCount - 1;
        List<TestingTaskStateSnapshot> taskStateSnapshots = storeStates(chkCount);
        taskLocalStateStore.confirmCheckpoint(confirmed);
        taskLocalStateStore.dispose();

        checkPrunedAndDiscarded(taskStateSnapshots, 0, chkCount);
    }

    @Test
    void retrieveNullIfNoPersistedLocalState() {
        assertThat(taskLocalStateStore.retrieveLocalState(0)).isNull();
    }

    @Test
    void retrievePersistedLocalStateFromDisc() {
        final TaskStateSnapshot taskStateSnapshot = createTaskStateSnapshot();
        final long checkpointId = 0L;
        taskLocalStateStore.storeLocalState(checkpointId, taskStateSnapshot);

        final TaskLocalStateStoreImpl newTaskLocalStateStore =
                createTaskLocalStateStoreImpl(
                        allocationBaseDirs, jobID, allocationID, jobVertexID, 0);

        final TaskStateSnapshot retrievedTaskStateSnapshot =
                newTaskLocalStateStore.retrieveLocalState(checkpointId);

        assertThat(retrievedTaskStateSnapshot).isEqualTo(taskStateSnapshot);
    }

    @Nonnull
    protected TaskStateSnapshot createTaskStateSnapshot() {
        final Map<OperatorID, OperatorSubtaskState> operatorSubtaskStates = new HashMap<>();
        operatorSubtaskStates.put(new OperatorID(), OperatorSubtaskState.builder().build());
        operatorSubtaskStates.put(new OperatorID(), OperatorSubtaskState.builder().build());
        final TaskStateSnapshot taskStateSnapshot = new TaskStateSnapshot(operatorSubtaskStates);
        return taskStateSnapshot;
    }

    @Test
    void deletesLocalStateIfRetrievalFails() throws IOException {
        final TaskStateSnapshot taskStateSnapshot = createTaskStateSnapshot();
        final long checkpointId = 0L;
        taskLocalStateStore.storeLocalState(checkpointId, taskStateSnapshot);

        final File taskStateSnapshotFile =
                taskLocalStateStore.getTaskStateSnapshotFile(checkpointId);

        Files.write(
                taskStateSnapshotFile.toPath(), new byte[] {1, 2, 3, 4}, StandardOpenOption.WRITE);

        final TaskLocalStateStoreImpl newTaskLocalStateStore =
                createTaskLocalStateStoreImpl(
                        allocationBaseDirs, jobID, allocationID, jobVertexID, subtaskIdx);

        assertThat(newTaskLocalStateStore.retrieveLocalState(checkpointId)).isNull();
        assertThat(taskStateSnapshotFile.getParentFile()).doesNotExist();
    }

    private void checkStoredAsExpected(List<TestingTaskStateSnapshot> history, int start, int end) {
        for (int i = start; i < end; ++i) {
            TestingTaskStateSnapshot expected = history.get(i);
            assertThat(taskLocalStateStore.retrieveLocalState(i)).isSameAs(expected);
            assertThat(expected.isDiscarded()).isFalse();
        }
    }

    private void checkPrunedAndDiscarded(
            List<TestingTaskStateSnapshot> history, int start, int end) {
        for (int i = start; i < end; ++i) {
            assertThat(taskLocalStateStore.retrieveLocalState(i)).isNull();
            assertThat(history.get(i).isDiscarded()).isTrue();
        }
    }

    private List<TestingTaskStateSnapshot> storeStates(int count) {
        List<TestingTaskStateSnapshot> taskStateSnapshots = new ArrayList<>(count);
        for (int i = 0; i < count; ++i) {
            OperatorID operatorID = new OperatorID();
            TestingTaskStateSnapshot taskStateSnapshot = new TestingTaskStateSnapshot();
            OperatorSubtaskState operatorSubtaskState = OperatorSubtaskState.builder().build();
            taskStateSnapshot.putSubtaskStateByOperatorID(operatorID, operatorSubtaskState);
            taskLocalStateStore.storeLocalState(i, taskStateSnapshot);
            taskStateSnapshots.add(taskStateSnapshot);
        }
        return taskStateSnapshots;
    }

    protected static final class TestingTaskStateSnapshot extends TaskStateSnapshot {
        private static final long serialVersionUID = 2046321877379917040L;

        private boolean isDiscarded = false;

        @Override
        public void discardState() throws Exception {
            super.discardState();
            isDiscarded = true;
        }

        boolean isDiscarded() {
            return isDiscarded;
        }
    }
}
