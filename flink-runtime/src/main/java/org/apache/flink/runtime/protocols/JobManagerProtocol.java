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

package org.apache.flink.runtime.protocols;

import java.io.IOException;

import org.apache.flink.runtime.instance.HardwareDescription;
import org.apache.flink.runtime.instance.InstanceConnectionInfo;
import org.apache.flink.runtime.instance.InstanceID;
import org.apache.flink.runtime.taskmanager.TaskExecutionState;

/**
 * The job manager protocol is implemented by the job manager and offers functionality
 * to task managers which allows them to register themselves, send heart beat messages
 * or to report the results of a task execution.
 */
public interface JobManagerProtocol extends ServiceDiscoveryProtocol {

	/**
	 * Sends a heart beat to the job manager.
	 * 
	 * @param taskManagerId The ID identifying the task manager.
	 * @throws IOException Thrown if an error occurs during this remote procedure call.
	 */
	boolean sendHeartbeat(InstanceID taskManagerId) throws IOException;

	/**
	 * Registers a task manager at the JobManager.
	 *
	 * @param instanceConnectionInfo the information the job manager requires to connect to the instance's task manager
	 * @param hardwareDescription a hardware description with details on the instance's compute resources.
	 * @param numberOfSlots The number of task slots that the TaskManager provides.
	 *
	 * @return The ID under which the TaskManager is registered. Null, if the JobManager does not register the TaskManager.
	 */
	InstanceID registerTaskManager(InstanceConnectionInfo instanceConnectionInfo, HardwareDescription hardwareDescription, int numberOfSlots) throws IOException;

	/**
	 * Reports an update of a task's execution state to the job manager. This method returns true, if the state was
	 * correctly registered. It it returns false, the calling task manager should cancel its execution of the task.
	 * 
	 * @param taskExecutionState The new task execution state.
	 * @return True if everything is all right, false if the caller should cancel the task execution.
	 * 
	 * @throws IOException Thrown, if an error occurs during this remote procedure call
	 */
	boolean updateTaskExecutionState(TaskExecutionState taskExecutionState) throws IOException;
}
