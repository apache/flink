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

package eu.stratosphere.nephele.execution;

/**
 * This interface must be implemented by classes which should be able to receive notifications about
 * changes of a task's execution state.
 * 
 * @author warneke
 */
public interface ExecutionNotifiable {

	/**
	 * Called when the execution state of the associated task has changed.
	 * 
	 * @param ee
	 *        the execution environment of the task
	 * @param newExecutionState
	 *        the execution state the task has just switched to
	 * @param optionalMessage
	 *        an optional message providing further information on the state change
	 */
	void executionStateChanged(Environment ee, ExecutionState newExecutionState, String optionalMessage);

	/**
	 * Called when the user task has started a new thread.
	 * 
	 * @param ee
	 *        the execution environment the newly started thread belongs to
	 * @param userThread
	 *        the user thread which has been started
	 */
	void userThreadStarted(Environment ee, Thread userThread);

	/**
	 * Called when a thread spawn by a user task has finished.
	 * 
	 * @param ee
	 *        the execution environment the finished thread belongs to
	 * @param userThread
	 *        the user thread which has finished
	 */
	void userThreadFinished(Environment ee, Thread userThread);
}
