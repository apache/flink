/***********************************************************************************************************************
 * Copyright (C) 2010-2013 by the Stratosphere project (http://stratosphere.eu)
 *
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software distributed under the License is distributed on
 * an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the License for the
 * specific language governing permissions and limitations under the License.
 **********************************************************************************************************************/

package eu.stratosphere.nephele.taskmanager;

/**
 * This is an auxiliary thread to facilitate the shutdown of the
 * task manager through a shutdown hook.
 * 
 * @author warneke
 */
public class TaskManagerCleanUp extends Thread {

	/**
	 * The task manager to shut down.
	 */
	private final TaskManager taskManager;

	/**
	 * Constructs a new shutdown thread.
	 * 
	 * @param taskManager
	 *        the task manager to shut down
	 */
	public TaskManagerCleanUp(TaskManager taskManager) {
		this.taskManager = taskManager;
	}


	@Override
	public void run() {

		// Call shutdown method for the task manager
		this.taskManager.shutdown();

	}
}
