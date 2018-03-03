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
import org.apache.flink.runtime.clusterframework.types.AllocationID;
import org.apache.flink.runtime.clusterframework.types.ResourceID;
import org.apache.flink.runtime.concurrent.Executors;
import org.apache.flink.runtime.jobgraph.JobVertexID;
import org.apache.flink.runtime.taskexecutor.TaskManagerServices;
import org.apache.flink.runtime.taskexecutor.TaskManagerServicesConfiguration;
import org.apache.flink.util.FileUtils;
import org.apache.flink.util.TestLogger;

import org.junit.Assert;
import org.junit.ClassRule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

import java.io.File;
import java.net.InetAddress;

public class TaskExecutorLocalStateStoresManagerTest extends TestLogger {

	@ClassRule
	public static TemporaryFolder temporaryFolder = new TemporaryFolder();

	private static final long MEM_SIZE_PARAM = 128L*1024*1024;

	/**
	 * This tests that the creation of {@link TaskManagerServices} correctly creates the local state root directory
	 * for the {@link TaskExecutorLocalStateStoresManager} with the configured root directory.
	 */
	@Test
	public void testCreationFromConfig() throws Exception {

		final Configuration config = new Configuration();

		File newFolder = temporaryFolder.newFolder();
		String tmpDir = newFolder.getAbsolutePath() + File.separator;
		final String rootDirString = "__localStateRoot1,__localStateRoot2,__localStateRoot3".replaceAll("__", tmpDir);

		// test configuration of the local state directories
		config.setString(CheckpointingOptions.LOCAL_RECOVERY_TASK_MANAGER_STATE_ROOT_DIRS, rootDirString);

		// test configuration of the local state mode
		config.setString(CheckpointingOptions.LOCAL_RECOVERY, "ENABLE_FILE_BASED");

		final ResourceID tmResourceID = ResourceID.generate();

		TaskManagerServicesConfiguration taskManagerServicesConfiguration =
			TaskManagerServicesConfiguration.fromConfiguration(config, InetAddress.getLocalHost(), true);

		TaskManagerServices taskManagerServices = TaskManagerServices.fromConfiguration(
			taskManagerServicesConfiguration,
			tmResourceID,
			Executors.directExecutor(),
			MEM_SIZE_PARAM,
			MEM_SIZE_PARAM);

		TaskExecutorLocalStateStoresManager taskStateManager = taskManagerServices.getTaskManagerStateStore();

		// verify configured directories for local state
		String[] split = rootDirString.split(",");
		File[] rootDirectories = taskStateManager.getLocalStateRootDirectories();
		for (int i = 0; i < split.length; ++i) {
			Assert.assertEquals(
				new File(split[i], TaskManagerServices.LOCAL_STATE_SUB_DIRECTORY_ROOT),
				rootDirectories[i]);
		}

		// verify local recovery mode
		Assert.assertEquals(
			LocalRecoveryConfig.LocalRecoveryMode.ENABLE_FILE_BASED,
			taskStateManager.getLocalRecoveryMode());

		Assert.assertEquals("localState", TaskManagerServices.LOCAL_STATE_SUB_DIRECTORY_ROOT);
		for (File rootDirectory : rootDirectories) {
			FileUtils.deleteFileOrDirectory(rootDirectory);
		}
	}

	/**
	 * This tests that the creation of {@link TaskManagerServices} correctly falls back to the first tmp directory of
	 * the IOManager as default for the local state root directory.
	 */
	@Test
	public void testCreationFromConfigDefault() throws Exception {

		final Configuration config = new Configuration();

		final ResourceID tmResourceID = ResourceID.generate();

		TaskManagerServicesConfiguration taskManagerServicesConfiguration =
			TaskManagerServicesConfiguration.fromConfiguration(config, InetAddress.getLocalHost(), true);

		TaskManagerServices taskManagerServices = TaskManagerServices.fromConfiguration(
			taskManagerServicesConfiguration,
			tmResourceID,
			Executors.directExecutor(),
			MEM_SIZE_PARAM,
			MEM_SIZE_PARAM);

		TaskExecutorLocalStateStoresManager taskStateManager = taskManagerServices.getTaskManagerStateStore();

		String[] tmpDirPaths = taskManagerServicesConfiguration.getTmpDirPaths();
		File[] localStateRootDirectories = taskStateManager.getLocalStateRootDirectories();

		for (int i = 0; i < tmpDirPaths.length; ++i) {
			Assert.assertEquals(
				new File(tmpDirPaths[i], TaskManagerServices.LOCAL_STATE_SUB_DIRECTORY_ROOT),
				localStateRootDirectories[i]);
		}

		Assert.assertEquals(
			LocalRecoveryConfig.LocalRecoveryMode.DISABLED,
			taskStateManager.getLocalRecoveryMode());
	}

	/**
	 * This tests that the {@link TaskExecutorLocalStateStoresManager} creates {@link TaskLocalStateStoreImpl} that have
	 * a properly initialized local state base directory. It also checks that subdirectories are correctly deleted on
	 * shutdown.
	 */
	@Test
	public void testSubtaskStateStoreDirectoryCreateAndDelete() throws Exception {

		JobID jobID = new JobID();
		JobVertexID jobVertexID = new JobVertexID();
		AllocationID allocationID = new AllocationID();
		int subtaskIdx = 23;

		File[] rootDirs = {temporaryFolder.newFolder(), temporaryFolder.newFolder(), temporaryFolder.newFolder()};
		TaskExecutorLocalStateStoresManager storesManager = new TaskExecutorLocalStateStoresManager(
			LocalRecoveryConfig.LocalRecoveryMode.ENABLE_FILE_BASED,
			rootDirs,
			Executors.directExecutor());

		TaskLocalStateStore taskLocalStateStore =
			storesManager.localStateStoreForSubtask(jobID, allocationID, jobVertexID, subtaskIdx);

		LocalRecoveryDirectoryProvider directoryProvider =
			taskLocalStateStore.getLocalRecoveryConfig().getLocalStateDirectoryProvider();

		for (int i = 0; i < 10; ++i) {
			Assert.assertEquals(
				new File(
					rootDirs[(i & Integer.MAX_VALUE) % rootDirs.length],
					storesManager.allocationSubDirString(allocationID)),
				directoryProvider.allocationBaseDirectory(i));
		}

		long chkId = 42L;
		File allocBaseDirChk42 = directoryProvider.allocationBaseDirectory(chkId);
		File subtaskSpecificCheckpointDirectory = directoryProvider.subtaskSpecificCheckpointDirectory(chkId);
		Assert.assertEquals(
			new File(
				allocBaseDirChk42,
				"jid_" + jobID + File.separator +
					"vtx_" + jobVertexID + "_" +
					"sti_" + subtaskIdx + File.separator +
					"chk_" + chkId),
			subtaskSpecificCheckpointDirectory);

		Assert.assertTrue(subtaskSpecificCheckpointDirectory.mkdirs());

		File testFile = new File(subtaskSpecificCheckpointDirectory, "test");
		Assert.assertTrue(testFile.createNewFile());

		// test that local recovery mode is forwarded to the created store
		Assert.assertEquals(
			storesManager.getLocalRecoveryMode(),
			taskLocalStateStore.getLocalRecoveryConfig().getLocalRecoveryMode());

		Assert.assertTrue(testFile.exists());

		// check cleanup after releasing allocation id
		storesManager.releaseLocalStateForAllocationId(allocationID);
		checkRootDirsClean(rootDirs);

		AllocationID otherAllocationID = new AllocationID();

		taskLocalStateStore =
			storesManager.localStateStoreForSubtask(jobID, otherAllocationID, jobVertexID, subtaskIdx);

		directoryProvider = taskLocalStateStore.getLocalRecoveryConfig().getLocalStateDirectoryProvider();

		File chkDir = directoryProvider.subtaskSpecificCheckpointDirectory(23L);
		Assert.assertTrue(chkDir.mkdirs());
		testFile = new File(chkDir, "test");
		Assert.assertTrue(testFile.createNewFile());

		// check cleanup after shutdown
		storesManager.shutdown();
		checkRootDirsClean(rootDirs);
	}

	private void checkRootDirsClean(File[] rootDirs) {
		for (File rootDir : rootDirs) {
			File[] files = rootDir.listFiles();
			if (files != null) {
				Assert.assertArrayEquals(new File[0], files);
			}
		}
	}
}
