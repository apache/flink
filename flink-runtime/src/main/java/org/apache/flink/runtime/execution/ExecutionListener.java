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


package org.apache.flink.runtime.execution;

import org.apache.flink.runtime.executiongraph.ExecutionVertexID;
import org.apache.flink.runtime.jobgraph.JobID;

/**
 * This interface must be implemented by classes which should be able to receive notifications about
 * changes of a task's execution state.
 * 
 */
public interface ExecutionListener {

	/**
	 * Returns the priority of the execution listener object. If multiple execution listener objects are registered for
	 * a given vertex, the priority determines in which order they will be called. Priorities are expressed as
	 * non-negative integer values. The lower the integer value, the higher the call priority.
	 * 
	 * @return the priority of this execution listener
	 */
	int getPriority();

	/**
	 * Called when the execution state of the associated task has changed. It is important to point out that multiple
	 * execution listeners can be invoked as a reaction to a state change, according to their priority. As a result, the
	 * value of <code>newExecutionState</code> may be out-dated by the time a particular execution listener is called.
	 * To determine the most recent state of the respective task, it is recommended to store a reference on the
	 * execution that represents it and then call <code>getExecutionState()</code> on the vertex within this method.
	 * 
	 * @param jobID
	 *        the ID of the job the task belongs to
	 * @param vertexID
	 *        the ID of the task whose state has changed
	 * @param newExecutionState
	 *        the execution state the task has just switched to
	 * @param optionalMessage
	 *        an optional message providing further information on the state change
	 */
	void executionStateChanged(JobID jobID, ExecutionVertexID vertexID, ExecutionState newExecutionState,
			String optionalMessage);

	/**
	 * Called when the user task has started a new thread.
	 * 
	 * @param jobID
	 *        the ID of the job the task belongs to
	 * @param vertexID
	 *        the ID of the task that started of new thread
	 * @param userThread
	 *        the user thread which has been started
	 */
	void userThreadStarted(JobID jobID, ExecutionVertexID vertexID, Thread userThread);

	/**
	 * Called when a thread spawn by a user task has finished.
	 * 
	 * @param jobID
	 *        the ID of the job the task belongs to
	 * @param vertexID
	 *        the ID of the task whose thread has finished
	 * @param userThread
	 *        the user thread which has finished
	 */
	void userThreadFinished(JobID jobID, ExecutionVertexID vertexID, Thread userThread);
}
