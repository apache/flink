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

package eu.stratosphere.nephele.protocols;

import java.io.IOException;

import eu.stratosphere.core.protocols.VersionedProtocol;
import eu.stratosphere.nephele.instance.HardwareDescription;
import eu.stratosphere.nephele.instance.InstanceConnectionInfo;
import eu.stratosphere.nephele.taskmanager.TaskExecutionState;
import eu.stratosphere.nephele.taskmanager.transferenvelope.RegisterTaskManagerResult;
import eu.stratosphere.nephele.types.IntegerRecord;

/**
 * The job manager protocol is implemented by the job manager and offers functionality
 * to task managers which allows them to register themselves, send heart beat messages
 * or to report the results of a task execution.
 * 
 */
public interface JobManagerProtocol extends VersionedProtocol {

	/**
	 * Sends a heart beat to the job manager.
	 * 
	 * @param instanceConnectionInfo
	 *        the information the job manager requires to connect to the instance's task manager
	 * @throws IOException
	 *         thrown if an error occurs during this remote procedure call
	 */
	void sendHeartbeat(InstanceConnectionInfo instanceConnectionInfo)
			throws IOException;

	/**
	 * Registers a task manager at the JobManager.
	 *
	 * @param instanceConnectionInfo the information the job manager requires to connect to the instance's task manager
	 * @param hardwareDescription a hardware description with details on the instance's compute resources.
	 * @throws IOException
	 *
	 * @return whether the task manager was successfully registered
	 */
	RegisterTaskManagerResult registerTaskManager(InstanceConnectionInfo instanceConnectionInfo,
						HardwareDescription hardwareDescription, IntegerRecord numberOfSlots)
			throws IOException;

	/**
	 * Reports an update of a task's execution state to the job manager.
	 * 
	 * @param taskExecutionState
	 *        the new task execution state
	 * @throws IOException
	 *         thrown if an error occurs during this remote procedure call
	 */
	void updateTaskExecutionState(TaskExecutionState taskExecutionState) throws IOException;
}
