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

package org.apache.flink.runtime.instance;

import java.util.ArrayList;
import java.util.List;

import org.apache.flink.configuration.ConfigConstants;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.configuration.GlobalConfiguration;
import org.apache.flink.runtime.ExecutionMode;
import org.apache.flink.runtime.taskmanager.TaskManager;
import org.slf4j.LoggerFactory;

/**
 * A variant of the {@link InstanceManager} that internally spawn task managers as instances, rather than waiting for external
 * TaskManagers to register.
 */
public class LocalInstanceManager extends InstanceManager {
	
	private final List<TaskManager> taskManagers = new ArrayList<TaskManager>();

	
	public LocalInstanceManager(int numTaskManagers) throws Exception {
		ExecutionMode execMode = numTaskManagers == 1 ? ExecutionMode.LOCAL : ExecutionMode.CLUSTER;
		
		final int ipcPort = GlobalConfiguration.getInteger(ConfigConstants.TASK_MANAGER_IPC_PORT_KEY, -1);
		final int dataPort = GlobalConfiguration.getInteger(ConfigConstants.TASK_MANAGER_DATA_PORT_KEY, -1);
		
		for (int i = 0; i < numTaskManagers; i++) {
			
			// configure ports, if necessary
			if (ipcPort > 0 || dataPort > 0) {
				Configuration tm = new Configuration();
				if (ipcPort > 0) {
					tm.setInteger(ConfigConstants.TASK_MANAGER_IPC_PORT_KEY, ipcPort + i);
				}
				if (dataPort > 0) {
					tm.setInteger(ConfigConstants.TASK_MANAGER_DATA_PORT_KEY, dataPort + i);
				}

				GlobalConfiguration.includeConfiguration(tm);
			}

			taskManagers.add(TaskManager.createTaskManager(execMode));
		}
	}

	@Override
	public void shutdown() {
		try {
			for (TaskManager taskManager: taskManagers){
				try {
					taskManager.shutdown();
				}
				catch (Throwable t) {
					// log and continue in any case
					// we initialize the log lazily, because this is the only place we log
					// and most likely we never log.
					LoggerFactory.getLogger(LocalInstanceManager.class).error("Error shutting down local embedded TaskManager.", t);
				}
			}
		} finally {
			this.taskManagers.clear();
			super.shutdown();
		}
	}
	
	public TaskManager[] getTaskManagers() {
		return (TaskManager[]) this.taskManagers.toArray(new TaskManager[this.taskManagers.size()]);
	}
}
