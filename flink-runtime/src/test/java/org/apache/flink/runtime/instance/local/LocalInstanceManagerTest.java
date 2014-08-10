/**
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


package org.apache.flink.runtime.instance.local;

import org.junit.Assert;

import org.apache.flink.configuration.ConfigConstants;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.configuration.GlobalConfiguration;
import org.apache.flink.runtime.ExecutionMode;
import org.apache.flink.runtime.instance.InstanceManager;
import org.apache.flink.runtime.jobmanager.JobManager;
import org.junit.Test;

public class LocalInstanceManagerTest {

	/**
	 * Checks if the local instance manager reads the default correctly from the configuration file.
	 */
	@Test
	public void testInstanceTypeFromConfiguration() {

		try {
			Configuration cfg = new Configuration();
			cfg.setString(ConfigConstants.JOB_MANAGER_IPC_ADDRESS_KEY, "127.0.0.1");
			cfg.setInteger(ConfigConstants.JOB_MANAGER_IPC_PORT_KEY, 6123);
			cfg.setInteger(ConfigConstants.TASK_MANAGER_MEMORY_SIZE_KEY, 1);
			cfg.setInteger(ConfigConstants.TASK_MANAGER_NUM_TASK_SLOTS, 1);
			
			GlobalConfiguration.includeConfiguration(cfg);

			// start JobManager
			ExecutionMode executionMode = ExecutionMode.LOCAL;
			JobManager jm = new JobManager(executionMode);

			final TestInstanceListener testInstanceListener = new TestInstanceListener();
	
			InstanceManager im = jm.getInstanceManager();
			try {
				im.setInstanceListener(testInstanceListener);
	
			} catch (Exception e) {
				e.printStackTrace();
				Assert.fail("Instantiation of LocalInstanceManager failed: " + e.getMessage());
			} finally {
				jm.shutdown();
			}
		}
		catch (Exception e) {
			System.err.println(e.getMessage());
			e.printStackTrace();
			Assert.fail("Test caused an error: " + e.getMessage());
		}
	}
}
