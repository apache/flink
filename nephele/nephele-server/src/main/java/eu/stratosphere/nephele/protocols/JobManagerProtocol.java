/***********************************************************************************************************************
 *
 * Copyright (C) 2010 by the Stratosphere project (http://stratosphere.eu)
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

package eu.stratosphere.nephele.protocols;

import java.io.IOException;

import eu.stratosphere.nephele.instance.HardwareDescription;
import eu.stratosphere.nephele.instance.InstanceConnectionInfo;
import eu.stratosphere.nephele.protocols.VersionedProtocol;
import eu.stratosphere.nephele.taskmanager.TaskExecutionState;

/**
 * The job manager protocol is implemented by the job manager and offers functionality
 * to task managers which allows them to register themselves, send heart beat messages
 * or to report the results of a task execution.
 * 
 * @author warneke
 */
public interface JobManagerProtocol extends VersionedProtocol {

	/**
	 * Sends a heart beat to the job manager.
	 * 
	 * @param instanceConnectionInfo
	 *        the information the job manager requires to connect to the instance's task manager
	 * @param hardwareDescription
	 *        a hardware description with details on the instance's compute resources.
	 * @throws IOException
	 *         thrown if an error occurs during this remote procedure call
	 */
	void sendHeartbeat(InstanceConnectionInfo instanceConnectionInfo, HardwareDescription hardwareDescription)
			throws IOException;

	/**
	 * Reports the result of a task execution to the job manager.
	 * 
	 * @param executionResult
	 * @throws IOException
	 *         thrown if an error occurs during this remote procedure call
	 */
	void updateTaskExecutionState(TaskExecutionState executionResult) throws IOException;
}
