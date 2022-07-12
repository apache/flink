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
import org.apache.flink.core.fs.FileSystem;
import org.apache.flink.core.fs.Path;
import org.apache.flink.runtime.checkpoint.OperatorSubtaskState;
import org.apache.flink.runtime.checkpoint.TaskStateSnapshot;
import org.apache.flink.runtime.clusterframework.types.AllocationID;
import org.apache.flink.runtime.jobgraph.JobVertexID;
import org.apache.flink.runtime.jobgraph.OperatorID;
import org.apache.flink.util.TestLogger;
import org.apache.flink.util.concurrent.Executors;

import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

import javax.annotation.Nonnull;

import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.StandardOpenOption;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

/** Test for the {@link TaskLocalStateStoreImpl}. */
public class TaskLocalStateStoreImplTest extends TestLogger {

    private TemporaryFolder temporaryFolder;
    private File[] allocationBaseDirs;
    private TaskLocalStateStoreImpl taskLocalStateStore;
    private JobID jobID;
    private AllocationID allocationID;
    private JobVertexID jobVertexID;
    private int subtaskIdx;
    private LocalRecoveryDirectoryProviderImpl directoryProvider;

    @Before
    public void before() throws Exception {
        jobID = new JobID();
        allocationID = new AllocationID();
        jobVertexID = new JobVertexID();
        subtaskIdx = 0;
        this.temporaryFolder = new TemporaryFolder();
        this.temporaryFolder.create();
        this.allocationBaseDirs =
                new File[] {temporaryFolder.newFolder(), temporaryFolder.newFolder()};

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
        directoryProvider =
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

    @After
    public void after() {
        this.temporaryFolder.delete();
    }

    /** Test that the instance delivers a correctly configured LocalRecoveryDirectoryProvider. */
    @Test
    public void getLocalRecoveryRootDirectoryProvider() {

        LocalRecoveryConfig directoryProvider = taskLocalStateStore.getLocalRecoveryConfig();
        Assert.assertEquals(
                allocationBaseDirs.length,
                directoryProvider.getLocalStateDirectoryProvider().get().allocationBaseDirsCount());

        for (int i = 0; i < allocationBaseDirs.length; ++i) {
            Assert.assertEquals(
                    allocationBaseDirs[i],
                    directoryProvider
                            .getLocalStateDirectoryProvider()
                            .get()
                            .selectAllocationBaseDirectory(i));
        }
    }

    /** Tests basic store/retrieve of local state. */
    @Test
    public void storeAndRetrieve() throws Exception {

        final int chkCount = 3;

        for (int i = 0; i < chkCount; ++i) {
            Assert.assertNull(taskLocalStateStore.retrieveLocalState(i));
        }

        List<TestingTaskStateSnapshot> taskStateSnapshots = storeStates(chkCount);

        checkStoredAsExpected(taskStateSnapshots, 0, chkCount);

        Assert.assertNull(taskLocalStateStore.retrieveLocalState(chkCount + 1));
    }

    /** Test checkpoint pruning. */
    @Test
    public void pruneCheckpoints() throws Exception {

        final int chkCount = 3;

        List<TestingTaskStateSnapshot> taskStateSnapshots = storeStates(chkCount);

        // test retrieve with pruning
        taskLocalStateStore.pruneMatchingCheckpoints((long chk) -> chk != chkCount - 1);

        for (int i = 0; i < chkCount - 1; ++i) {
            Assert.assertNull(taskLocalStateStore.retrieveLocalState(i));
        }

        checkStoredAsExpected(taskStateSnapshots, chkCount - 1, chkCount);
    }

    /** Tests pruning of previous checkpoints if a new checkpoint is confirmed. */
    @Test
    public void confirmCheckpoint() throws Exception {

        final int chkCount = 3;
        final int confirmed = chkCount - 1;
        List<TestingTaskStateSnapshot> taskStateSnapshots = storeStates(chkCount);
        taskLocalStateStore.confirmCheckpoint(confirmed);
        checkPrunedAndDiscarded(taskStateSnapshots, 0, confirmed);
        checkStoredAsExpected(taskStateSnapshots, confirmed, chkCount);
    }

    /** Tests pruning of target previous checkpoints if that checkpoint is aborted. */
    @Test
    public void abortCheckpoint() throws Exception {

        final int chkCount = 4;
        final int aborted = chkCount - 2;
        List<TestingTaskStateSnapshot> taskStateSnapshots = storeStates(chkCount);
        checkLocalStateDirectoryExistOrNot(aborted, true);
        taskLocalStateStore.abortCheckpoint(aborted);
        checkLocalStateDirectoryExistOrNot(aborted, false);
        checkPrunedAndDiscarded(taskStateSnapshots, aborted, aborted + 1);
        checkStoredAsExpected(taskStateSnapshots, 0, aborted);
        checkStoredAsExpected(taskStateSnapshots, aborted + 1, chkCount);
    }

    @Test
    public void abortUnregisteredCheckpoint() throws Exception {
        final int chkCount = 4;
        final int aborted = chkCount - 2;
        List<TestingTaskStateSnapshot> taskStateSnapshots = new ArrayList<>(chkCount);
        for (int i = 0; i < chkCount; ++i) {
            TestingTaskStateSnapshot taskStateSnapshot = new TestingTaskStateSnapshot();
            taskStateSnapshot.putSubtaskStateByOperatorID(
                    new OperatorID(), OperatorSubtaskState.builder().build());
            taskStateSnapshots.add(taskStateSnapshot);
            if (i == aborted) {
                java.nio.file.Path chkDir =
                        directoryProvider.subtaskSpecificCheckpointDirectory(aborted).toPath();
                Files.createDirectory(chkDir);
                Files.createFile(chkDir.resolve("checkpoint_test_file"));
                // For checkpoint to be aborted, don't register it into taskLocalStateStore.
                continue;
            }
            taskLocalStateStore.storeLocalState(i, taskStateSnapshot);
        }

        checkLocalStateDirectoryExistOrNot(aborted, true);
        taskLocalStateStore.abortCheckpoint(aborted);
        checkLocalStateDirectoryExistOrNot(aborted, false);
        checkStoredAsExpected(taskStateSnapshots, 0, aborted);
        checkStoredAsExpected(taskStateSnapshots, aborted + 1, chkCount);
    }

    /**
     * Tests that disposal of a {@link TaskLocalStateStoreImpl} works and discards all local states.
     */
    @Test
    public void dispose() throws Exception {
        final int chkCount = 3;
        final int confirmed = chkCount - 1;
        List<TestingTaskStateSnapshot> taskStateSnapshots = storeStates(chkCount);
        taskLocalStateStore.confirmCheckpoint(confirmed);
        taskLocalStateStore.dispose();

        checkPrunedAndDiscarded(taskStateSnapshots, 0, chkCount);
    }

    @Test
    public void retrieveNullIfNoPersistedLocalState() {
        assertThat(taskLocalStateStore.retrieveLocalState(0)).isNull();
    }

    @Test
    public void retrievePersistedLocalStateFromDisc() {
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
    private TaskStateSnapshot createTaskStateSnapshot() {
        final Map<OperatorID, OperatorSubtaskState> operatorSubtaskStates = new HashMap<>();
        operatorSubtaskStates.put(new OperatorID(), OperatorSubtaskState.builder().build());
        operatorSubtaskStates.put(new OperatorID(), OperatorSubtaskState.builder().build());
        final TaskStateSnapshot taskStateSnapshot = new TaskStateSnapshot(operatorSubtaskStates);
        return taskStateSnapshot;
    }

    @Test
    public void deletesLocalStateIfRetrievalFails() throws IOException {
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
            assertTrue(expected == taskLocalStateStore.retrieveLocalState(i));
            assertFalse(expected.isDiscarded());
        }
    }

    private void checkPrunedAndDiscarded(
            List<TestingTaskStateSnapshot> history, int start, int end) {
        for (int i = start; i < end; ++i) {
            Assert.assertNull(taskLocalStateStore.retrieveLocalState(i));
            assertTrue(history.get(i).isDiscarded());
        }
    }

    private void checkLocalStateDirectoryExistOrNot(long checkpointID, boolean exist)
            throws IOException {
        File localStateDir = directoryProvider.subtaskSpecificCheckpointDirectory(checkpointID);
        Path path = new Path(localStateDir.toURI());
        FileSystem fileSystem = path.getFileSystem();
        assertEquals(exist, fileSystem.exists(path));
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

    private static final class TestingTaskStateSnapshot extends TaskStateSnapshot {
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
