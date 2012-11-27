/***********************************************************************************************************************
 *
 * Copyright (C) 2010-2012 by the Stratosphere project (http://stratosphere.eu)
 *
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software distributed under the License is distributed on
 * an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the License for the
 * specific language governing permissions and limitations under the License.
 *
 **********************************************************************************************************************/

package eu.stratosphere.nephele.instance.local;

import eu.stratosphere.nephele.taskmanager.TaskManager;

/**
 * This class represents the thread which runs the task manager when Nephele is executed in local mode.
 * 
 * @author warneke
 */
public class LocalTaskManagerThread extends Thread {

	/**
	 * The task manager to run in this thread.
	 */
	private final TaskManager taskManager;

	/**
	 * Constructs a new thread to run the task manager in Nephele's local mode.
	 * 
	 * @param configDir
	 *        the configuration directory to pass on to the task manager instance
	 */
	public LocalTaskManagerThread(String configDir) {

		TaskManager tmpTaskManager = null;
		try {
			tmpTaskManager = new TaskManager(configDir);
		} catch (Exception e) {
			throw new RuntimeException(e);
		}
		this.taskManager = tmpTaskManager;
	}

	/**
	 * {@inheritDoc}
	 */
	@Override
	public void run() {

		this.taskManager.runIOLoop();

		// Wait until the task manager is shut down
		while (!this.taskManager.isShutDown()) {
			try {
				Thread.sleep(100);
			} catch (InterruptedException e) {
				break;
			}
		}
	}

	/**
	 * Checks if the task manager run by this thread is completely shut down.
	 * 
	 * @return <code>true</code> if the task manager is completely shut down, <code>false</code> otherwise
	 */
	public boolean isTaskManagerShutDown() {

		return this.taskManager.isShutDown();
	}

	/**
	 * Returns true if the TaskManager was successfully initialized.
	 * 
	 * @return true if the TaskManager was successfully initialized, false otherwise.
	 */
	/*
	 * public boolean isTaskManagerInitialized() {
	 * return this.taskManagerSuccessfullyInitialized;
	 * }
	 */
}
