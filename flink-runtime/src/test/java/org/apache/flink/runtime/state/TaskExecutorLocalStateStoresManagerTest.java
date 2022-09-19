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

package org.apache.flink.runtime.state;

import org.apache.flink.api.common.JobID;
import org.apache.flink.configuration.CheckpointingOptions;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.runtime.blob.VoidPermanentBlobService;
import org.apache.flink.runtime.clusterframework.types.AllocationID;
import org.apache.flink.runtime.clusterframework.types.ResourceID;
import org.apache.flink.runtime.entrypoint.WorkingDirectory;
import org.apache.flink.runtime.jobgraph.JobVertexID;
import org.apache.flink.runtime.metrics.groups.UnregisteredMetricGroups;
import org.apache.flink.runtime.taskexecutor.TaskExecutorResourceUtils;
import org.apache.flink.runtime.taskexecutor.TaskManagerServices;
import org.apache.flink.runtime.taskexecutor.TaskManagerServicesConfiguration;
import org.apache.flink.runtime.testutils.WorkingDirectoryResource;
import org.apache.flink.util.FileUtils;
import org.apache.flink.util.Reference;
import org.apache.flink.util.TestLogger;
import org.apache.flink.util.concurrent.Executors;

import org.apache.flink.shaded.guava30.com.google.common.collect.Sets;

import org.junit.Assert;
import org.junit.ClassRule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

import java.io.File;
import java.io.IOException;
import java.net.InetAddress;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.Collection;

import static org.assertj.core.api.Assertions.assertThat;

/** Test for {@link TaskExecutorLocalStateStoresManager}. */
public class TaskExecutorLocalStateStoresManagerTest extends TestLogger {

    @ClassRule public static TemporaryFolder temporaryFolder = new TemporaryFolder();

    @ClassRule
    public static final WorkingDirectoryResource WORKING_DIRECTORY_RESOURCE =
            new WorkingDirectoryResource();

    /**
     * This tests that the creation of {@link TaskManagerServices} correctly creates the local state
     * root directory for the {@link TaskExecutorLocalStateStoresManager} with the configured root
     * directory.
     */
    @Test
    public void testCreationFromConfig() throws Exception {

        final Configuration config = new Configuration();

        File newFolder = temporaryFolder.newFolder();
        String tmpDir = newFolder.getAbsolutePath() + File.separator;
        final String rootDirString =
                "__localStateRoot1,__localStateRoot2,__localStateRoot3".replaceAll("__", tmpDir);

        // test configuration of the local state directories
        config.setString(
                CheckpointingOptions.LOCAL_RECOVERY_TASK_MANAGER_STATE_ROOT_DIRS, rootDirString);

        // test configuration of the local state mode
        config.setBoolean(CheckpointingOptions.LOCAL_RECOVERY, true);

        final WorkingDirectory workingDirectory =
                WORKING_DIRECTORY_RESOURCE.createNewWorkingDirectory();
        TaskManagerServices taskManagerServices =
                createTaskManagerServices(
                        createTaskManagerServiceConfiguration(config, workingDirectory),
                        workingDirectory);

        try {
            TaskExecutorLocalStateStoresManager taskStateManager =
                    taskManagerServices.getTaskManagerStateStore();

            // verify configured directories for local state
            String[] split = rootDirString.split(",");
            File[] rootDirectories = taskStateManager.getLocalStateRootDirectories();
            for (int i = 0; i < split.length; ++i) {
                assertThat(rootDirectories[i].toPath()).startsWith(Paths.get(split[i]));
            }

            // verify local recovery mode
            Assert.assertTrue(taskStateManager.isLocalRecoveryEnabled());

            for (File rootDirectory : rootDirectories) {
                FileUtils.deleteFileOrDirectory(rootDirectory);
            }
        } finally {
            taskManagerServices.shutDown();
        }
    }

    /**
     * This tests that the creation of {@link TaskManagerServices} correctly falls back to the first
     * tmp directory of the IOManager as default for the local state root directory.
     */
    @Test
    public void testCreationFromConfigDefault() throws Exception {

        final Configuration config = new Configuration();

        final WorkingDirectory workingDirectory =
                WORKING_DIRECTORY_RESOURCE.createNewWorkingDirectory();
        TaskManagerServicesConfiguration taskManagerServicesConfiguration =
                createTaskManagerServiceConfiguration(config, workingDirectory);

        TaskManagerServices taskManagerServices =
                createTaskManagerServices(taskManagerServicesConfiguration, workingDirectory);

        try {
            TaskExecutorLocalStateStoresManager taskStateManager =
                    taskManagerServices.getTaskManagerStateStore();

            File[] localStateRootDirectories = taskStateManager.getLocalStateRootDirectories();

            for (int i = 0; i < localStateRootDirectories.length; ++i) {
                Assert.assertEquals(
                        workingDirectory.getLocalStateDirectory(), localStateRootDirectories[i]);
            }

            Assert.assertFalse(taskStateManager.isLocalRecoveryEnabled());
        } finally {
            taskManagerServices.shutDown();
        }
    }

    @Test
    public void testLocalStateNoCreateDirWhenDisabledLocalRecovery() throws Exception {
        JobID jobID = new JobID();
        JobVertexID jobVertexID = new JobVertexID();
        AllocationID allocationID = new AllocationID();
        int subtaskIdx = 23;

        File[] rootDirs = {
            temporaryFolder.newFolder(), temporaryFolder.newFolder(), temporaryFolder.newFolder()
        };

        boolean localRecoveryEnabled = false;
        TaskExecutorLocalStateStoresManager storesManager =
                new TaskExecutorLocalStateStoresManager(
                        localRecoveryEnabled,
                        Reference.owned(rootDirs),
                        Executors.directExecutor());

        TaskLocalStateStore taskLocalStateStore =
                storesManager.localStateStoreForSubtask(
                        jobID,
                        allocationID,
                        jobVertexID,
                        subtaskIdx,
                        new Configuration(),
                        new Configuration());

        Assert.assertFalse(taskLocalStateStore.getLocalRecoveryConfig().isLocalRecoveryEnabled());
        Assert.assertNull(
                taskLocalStateStore
                        .getLocalRecoveryConfig()
                        .getLocalStateDirectoryProvider()
                        .orElse(null));

        for (File recoveryDir : rootDirs) {
            Assert.assertEquals(0, recoveryDir.listFiles().length);
        }
    }

    /**
     * This tests that the {@link TaskExecutorLocalStateStoresManager} creates {@link
     * TaskLocalStateStoreImpl} that have a properly initialized local state base directory. It also
     * checks that subdirectories are correctly deleted on shutdown.
     */
    @Test
    public void testSubtaskStateStoreDirectoryCreateAndDelete() throws Exception {

        JobID jobID = new JobID();
        JobVertexID jobVertexID = new JobVertexID();
        AllocationID allocationID = new AllocationID();
        int subtaskIdx = 23;

        File[] rootDirs = {
            temporaryFolder.newFolder(), temporaryFolder.newFolder(), temporaryFolder.newFolder()
        };
        TaskExecutorLocalStateStoresManager storesManager =
                new TaskExecutorLocalStateStoresManager(
                        true, Reference.owned(rootDirs), Executors.directExecutor());

        TaskLocalStateStore taskLocalStateStore =
                storesManager.localStateStoreForSubtask(
                        jobID,
                        allocationID,
                        jobVertexID,
                        subtaskIdx,
                        new Configuration(),
                        new Configuration());

        LocalRecoveryDirectoryProvider directoryProvider =
                taskLocalStateStore
                        .getLocalRecoveryConfig()
                        .getLocalStateDirectoryProvider()
                        .orElseThrow(LocalRecoveryConfig.localRecoveryNotEnabled());

        for (int i = 0; i < 10; ++i) {
            Assert.assertEquals(
                    new File(
                            rootDirs[(i & Integer.MAX_VALUE) % rootDirs.length],
                            storesManager.allocationSubDirString(allocationID)),
                    directoryProvider.allocationBaseDirectory(i));
        }

        long chkId = 42L;
        File allocBaseDirChk42 = directoryProvider.allocationBaseDirectory(chkId);
        File subtaskSpecificCheckpointDirectory =
                directoryProvider.subtaskSpecificCheckpointDirectory(chkId);
        Assert.assertEquals(
                new File(
                        allocBaseDirChk42,
                        "jid_"
                                + jobID
                                + File.separator
                                + "vtx_"
                                + jobVertexID
                                + "_"
                                + "sti_"
                                + subtaskIdx
                                + File.separator
                                + "chk_"
                                + chkId),
                subtaskSpecificCheckpointDirectory);

        Assert.assertTrue(subtaskSpecificCheckpointDirectory.mkdirs());

        File testFile = new File(subtaskSpecificCheckpointDirectory, "test");
        Assert.assertTrue(testFile.createNewFile());

        // test that local recovery mode is forwarded to the created store
        Assert.assertEquals(
                storesManager.isLocalRecoveryEnabled(),
                taskLocalStateStore.getLocalRecoveryConfig().isLocalRecoveryEnabled());

        Assert.assertTrue(testFile.exists());

        // check cleanup after releasing allocation id
        storesManager.releaseLocalStateForAllocationId(allocationID);
        checkRootDirsClean(rootDirs);

        AllocationID otherAllocationID = new AllocationID();

        taskLocalStateStore =
                storesManager.localStateStoreForSubtask(
                        jobID,
                        otherAllocationID,
                        jobVertexID,
                        subtaskIdx,
                        new Configuration(),
                        new Configuration());

        directoryProvider =
                taskLocalStateStore
                        .getLocalRecoveryConfig()
                        .getLocalStateDirectoryProvider()
                        .orElseThrow(LocalRecoveryConfig.localRecoveryNotEnabled());

        File chkDir = directoryProvider.subtaskSpecificCheckpointDirectory(23L);
        Assert.assertTrue(chkDir.mkdirs());
        testFile = new File(chkDir, "test");
        Assert.assertTrue(testFile.createNewFile());

        // check cleanup after shutdown
        storesManager.shutdown();
        checkRootDirsClean(rootDirs);
    }

    @Test
    public void testOwnedLocalStateDirectoriesAreDeletedOnShutdown() throws IOException {
        final File localStateStoreA = temporaryFolder.newFolder();
        final File localStateStoreB = temporaryFolder.newFolder();

        final File[] localStateDirectories = {localStateStoreA, localStateStoreB};

        final TaskExecutorLocalStateStoresManager taskExecutorLocalStateStoresManager =
                new TaskExecutorLocalStateStoresManager(
                        true, Reference.owned(localStateDirectories), Executors.directExecutor());

        for (File localStateDirectory : localStateDirectories) {
            assertThat(localStateDirectory).exists();
        }

        taskExecutorLocalStateStoresManager.shutdown();

        for (File localStateDirectory : localStateDirectories) {
            assertThat(localStateDirectory).doesNotExist();
        }
    }

    @Test
    public void testBorrowedLocalStateDirectoriesAreNotDeletedOnShutdown() throws IOException {
        final File localStateStoreA = temporaryFolder.newFolder();
        final File localStateStoreB = temporaryFolder.newFolder();

        final File[] localStateDirectories = {localStateStoreA, localStateStoreB};

        final TaskExecutorLocalStateStoresManager taskExecutorLocalStateStoresManager =
                new TaskExecutorLocalStateStoresManager(
                        true,
                        Reference.borrowed(localStateDirectories),
                        Executors.directExecutor());

        for (File localStateDirectory : localStateDirectories) {
            assertThat(localStateDirectory).exists();
        }

        taskExecutorLocalStateStoresManager.shutdown();

        for (File localStateDirectory : localStateDirectories) {
            assertThat(localStateDirectory).exists();
        }
    }

    @Test
    public void testRetainLocalStateForAllocationsDeletesUnretainedAllocationDirectories()
            throws IOException {
        final File localStateStore = temporaryFolder.newFolder();
        final TaskExecutorLocalStateStoresManager taskExecutorLocalStateStoresManager =
                new TaskExecutorLocalStateStoresManager(
                        true,
                        Reference.owned(new File[] {localStateStore}),
                        Executors.directExecutor());
        final JobID jobId = new JobID();
        final AllocationID retainedAllocationId = new AllocationID();
        final AllocationID otherAllocationId = new AllocationID();
        final JobVertexID jobVertexId = new JobVertexID();

        // register local state stores
        taskExecutorLocalStateStoresManager.localStateStoreForSubtask(
                jobId,
                retainedAllocationId,
                jobVertexId,
                0,
                new Configuration(),
                new Configuration());
        taskExecutorLocalStateStoresManager.localStateStoreForSubtask(
                jobId, otherAllocationId, jobVertexId, 1, new Configuration(), new Configuration());

        final Collection<Path> allocationDirectories =
                TaskExecutorLocalStateStoresManager.listAllocationDirectoriesIn(localStateStore);

        assertThat(allocationDirectories).hasSize(2);

        taskExecutorLocalStateStoresManager.retainLocalStateForAllocations(
                Sets.newHashSet(retainedAllocationId));

        final Collection<Path> allocationDirectoriesAfterCleanup =
                TaskExecutorLocalStateStoresManager.listAllocationDirectoriesIn(localStateStore);

        assertThat(allocationDirectoriesAfterCleanup).hasSize(1);
        assertThat(
                        new File(
                                localStateStore,
                                taskExecutorLocalStateStoresManager.allocationSubDirString(
                                        otherAllocationId)))
                .doesNotExist();
    }

    private void checkRootDirsClean(File[] rootDirs) {
        for (File rootDir : rootDirs) {
            File[] files = rootDir.listFiles();
            if (files != null) {
                Assert.assertArrayEquals(new File[0], files);
            }
        }
    }

    private TaskManagerServicesConfiguration createTaskManagerServiceConfiguration(
            Configuration config, WorkingDirectory workingDirectory) throws Exception {
        return TaskManagerServicesConfiguration.fromConfiguration(
                config,
                ResourceID.generate(),
                InetAddress.getLocalHost().getHostName(),
                true,
                TaskExecutorResourceUtils.resourceSpecFromConfigForLocalExecution(config),
                workingDirectory);
    }

    private TaskManagerServices createTaskManagerServices(
            TaskManagerServicesConfiguration config, WorkingDirectory workingDirectory)
            throws Exception {
        return TaskManagerServices.fromConfiguration(
                config,
                VoidPermanentBlobService.INSTANCE,
                UnregisteredMetricGroups.createUnregisteredTaskManagerMetricGroup(),
                Executors.newDirectExecutorService(),
                null,
                throwable -> {},
                workingDirectory);
    }
}
