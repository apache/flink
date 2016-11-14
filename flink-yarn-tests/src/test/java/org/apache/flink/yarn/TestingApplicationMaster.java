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

package org.apache.flink.yarn;

import org.apache.flink.runtime.jobmanager.JobManager;
import org.apache.flink.runtime.jobmanager.MemoryArchivist;
import org.apache.flink.runtime.taskmanager.TaskManager;
import org.apache.flink.runtime.testingUtils.TestingMemoryArchivist;
import org.apache.flink.runtime.testutils.TestingResourceManager;
import org.apache.flink.runtime.util.EnvironmentInformation;
import org.apache.flink.runtime.util.JvmShutdownSafeguard;
import org.apache.flink.runtime.util.SignalHandler;

/**
 * Yarn application master which starts the {@link TestingYarnJobManager},
 * {@link TestingResourceManager}, and the {@link TestingMemoryArchivist}.
 */
public class TestingApplicationMaster extends YarnApplicationMasterRunner {

	@Override
	public Class<? extends JobManager> getJobManagerClass() {
		return TestingYarnJobManager.class;
	}

	@Override
	public Class<? extends MemoryArchivist> getArchivistClass() {
		return TestingMemoryArchivist.class;
	}

	@Override
	protected Class<? extends TaskManager> getTaskManagerClass() {
		return TestingYarnTaskManager.class;
	}

	@Override
	public Class<? extends YarnFlinkResourceManager> getResourceManagerClass() {
		return TestingYarnFlinkResourceManager.class;
	}

	public static void main(String[] args) {
		EnvironmentInformation.logEnvironmentInfo(LOG, "YARN ApplicationMaster / JobManager", args);
		SignalHandler.register(LOG);
		JvmShutdownSafeguard.installAsShutdownHook(LOG);

		// run and exit with the proper return code
		int returnCode = new TestingApplicationMaster().run(args);
		System.exit(returnCode);
	}

}
