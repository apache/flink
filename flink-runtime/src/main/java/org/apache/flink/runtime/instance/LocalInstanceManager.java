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


package org.apache.flink.runtime.instance;


import java.util.ArrayList;
import java.util.List;

import org.apache.flink.configuration.ConfigConstants;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.configuration.GlobalConfiguration;
import org.apache.flink.runtime.ExecutionMode;
import org.apache.flink.runtime.taskmanager.TaskManager;

public class LocalInstanceManager extends DefaultInstanceManager {
	
	private List<TaskManager> taskManagers = new ArrayList<TaskManager>();

	public LocalInstanceManager() throws Exception{
		int numTaskManager = GlobalConfiguration.getInteger(ConfigConstants
				.LOCAL_INSTANCE_MANAGER_NUMBER_TASK_MANAGER, 1);

		ExecutionMode execMode = numTaskManager == 1 ? ExecutionMode.LOCAL : ExecutionMode.CLUSTER;
		
		for (int i=0; i < numTaskManager; i++){
			Configuration tm = new Configuration();
			int ipcPort = GlobalConfiguration.getInteger(ConfigConstants.TASK_MANAGER_IPC_PORT_KEY,
					ConfigConstants.DEFAULT_TASK_MANAGER_IPC_PORT);
			int dataPort = GlobalConfiguration.getInteger(ConfigConstants.TASK_MANAGER_DATA_PORT_KEY,
					ConfigConstants.DEFAULT_TASK_MANAGER_DATA_PORT);

			tm.setInteger(ConfigConstants.TASK_MANAGER_IPC_PORT_KEY, ipcPort + i);
			tm.setInteger(ConfigConstants.TASK_MANAGER_DATA_PORT_KEY, dataPort + i);

			GlobalConfiguration.includeConfiguration(tm);

			taskManagers.add(new TaskManager(execMode));
		}
	}

	@Override
	public void shutdown(){
		for(TaskManager taskManager: taskManagers){
			taskManager.shutdown();
		}

		super.shutdown();
	}
}
