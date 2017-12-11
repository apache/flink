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
import org.apache.flink.configuration.ConfigConstants;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.runtime.clusterframework.types.ResourceID;
import org.apache.flink.runtime.jobgraph.JobVertexID;
import org.apache.flink.runtime.taskexecutor.TaskManagerServices;
import org.apache.flink.runtime.taskexecutor.TaskManagerServicesConfiguration;

import org.junit.Assert;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

import java.io.File;
import java.net.InetAddress;

public class TaskExecutorLocalStateStoresManagerTest {

	/**
	 * This tests that the creation of {@link TaskManagerServices} correctly creates the local state root directory
	 * for the {@link TaskExecutorLocalStateStoresManager} with the configured root directory.
	 */
	@Test
	public void testCreationFromConfig() throws Exception {

		final Configuration config = new Configuration();

		final String rootDirString = "localStateRoot1,localStateRoot2,localStateRoot3";
		config.setString(ConfigConstants.TASK_MANAGER_LOCAL_STATE_ROOT_DIR_KEY, rootDirString);

		final ResourceID tmResourceID = ResourceID.generate();

		TaskManagerServicesConfiguration taskManagerServicesConfiguration =
			TaskManagerServicesConfiguration.fromConfiguration(config, InetAddress.getLocalHost(), true);

		TaskManagerServices taskManagerServices =
			TaskManagerServices.fromConfiguration(taskManagerServicesConfiguration, tmResourceID);

		TaskExecutorLocalStateStoresManager taskStateManager = taskManagerServices.getTaskManagerStateStore();

		String[] split = rootDirString.split(",");
		File[] rootDirectories = taskStateManager.getLocalStateRootDirectories();
		for (int i = 0; i < split.length; ++i) {
			Assert.assertEquals(
				new File(split[i], TaskManagerServices.LOCAL_STATE_SUB_DIRECTORY_ROOT),
				rootDirectories[i]);
		}

		Assert.assertEquals("localState", TaskManagerServices.LOCAL_STATE_SUB_DIRECTORY_ROOT);
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

		TaskManagerServices taskManagerServices =
			TaskManagerServices.fromConfiguration(taskManagerServicesConfiguration, tmResourceID);

		TaskExecutorLocalStateStoresManager taskStateManager = taskManagerServices.getTaskManagerStateStore();

		String[] tmpDirPaths = taskManagerServicesConfiguration.getTmpDirPaths();
		File[] localStateRootDirectories = taskStateManager.getLocalStateRootDirectories();

		for (int i = 0; i < tmpDirPaths.length; ++i) {
			Assert.assertEquals(
				new File(tmpDirPaths[i], TaskManagerServices.LOCAL_STATE_SUB_DIRECTORY_ROOT),
				localStateRootDirectories[i]);
		}
	}

	/**
	 * This tests that the {@link TaskExecutorLocalStateStoresManager} creates {@link TaskLocalStateStore} that have
	 * a properly initialized local state base directory.
	 */
	@Test
	public void testSubtaskStateStoreDirectoryCreation() throws Exception {

		JobID jobID = new JobID();
		JobVertexID jobVertexID = new JobVertexID();
		int subtaskIdx = 42;
		TemporaryFolder tmp = new TemporaryFolder();
		try {

			tmp.create();
			File[] rootDirs = {tmp.newFolder(), tmp.newFolder(), tmp.newFolder()};
			TaskExecutorLocalStateStoresManager storesManager =
				new TaskExecutorLocalStateStoresManager(rootDirs);

			TaskLocalStateStore taskLocalStateStore =
				storesManager.localStateStoreForTask(jobID, jobVertexID, subtaskIdx);

			LocalRecoveryDirectoryProvider directoryProvider =
				taskLocalStateStore.createLocalRecoveryRootDirectoryProvider();

			for (int i = 0; i < 10; ++i) {
				Assert.assertEquals(
					rootDirs[(i & Integer.MAX_VALUE) % rootDirs.length],
					directoryProvider.nextRootDirectory());
			}

			Assert.assertEquals(taskLocalStateStore.createSubtaskPath(), directoryProvider.getSubtaskSpecificPath());
			Assert.assertEquals(jobID + File.separator + jobVertexID + File.separator + subtaskIdx, taskLocalStateStore.createSubtaskPath());

		} finally {
			tmp.delete();
		}
	}
}
