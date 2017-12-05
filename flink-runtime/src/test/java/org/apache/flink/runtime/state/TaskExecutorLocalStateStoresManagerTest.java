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

		final String rootDirString = "localStateRoot";
		config.setString(ConfigConstants.TASK_MANAGER_LOCAL_STATE_ROOT_DIR_KEY, rootDirString);

		final ResourceID tmResourceID = ResourceID.generate();

		TaskManagerServicesConfiguration taskManagerServicesConfiguration =
			TaskManagerServicesConfiguration.fromConfiguration(config, InetAddress.getLocalHost(), true);

		TaskManagerServices taskManagerServices =
			TaskManagerServices.fromConfiguration(taskManagerServicesConfiguration, tmResourceID);

		TaskExecutorLocalStateStoresManager taskStateManager = taskManagerServices.getTaskStateManager();

		Assert.assertEquals(
			new File(rootDirString, TaskManagerServices.LOCAL_STATE_SUB_DIRECTORY_ROOT),
			taskStateManager.getLocalStateRootDirectory());

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

		TaskExecutorLocalStateStoresManager taskStateManager = taskManagerServices.getTaskStateManager();

		Assert.assertEquals(
			new File(taskManagerServicesConfiguration.getTmpDirPaths()[0], TaskManagerServices.LOCAL_STATE_SUB_DIRECTORY_ROOT),
			taskStateManager.getLocalStateRootDirectory());
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
			File localStoreDir = tmp.newFolder();
			TaskExecutorLocalStateStoresManager storesManager =
				new TaskExecutorLocalStateStoresManager(localStoreDir);

			TaskLocalStateStore taskLocalStateStore =
				storesManager.localStateStoreForTask(jobID, jobVertexID, subtaskIdx);

			Assert.assertEquals(
				new File(localStoreDir, TaskLocalStateStore.createSubtaskPath(jobID, jobVertexID, subtaskIdx)),
				taskLocalStateStore.getSubtaskLocalStateBaseDirectory());

			Assert.assertEquals(
				new File(localStoreDir, "jid-" + jobID + "_vtx-" + jobVertexID + "_sti-" + subtaskIdx),
				taskLocalStateStore.getSubtaskLocalStateBaseDirectory());
		} finally {
			tmp.delete();
		}
	}
}
