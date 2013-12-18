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

package eu.stratosphere.nephele.execution;

public interface ExecutionObserver {

	/**
	 * Called when the execution state of the associated task has changed.
	 * 
	 * @param newExecutionState
	 *        the execution state the task has just switched to
	 * @param optionalMessage
	 *        an optional message providing further information on the state change
	 */
	void executionStateChanged(ExecutionState newExecutionState, String optionalMessage);

	/**
	 * Called when the user task has started a new thread.
	 * 
	 * @param userThread
	 *        the user thread which has been started
	 */
	void userThreadStarted(Thread userThread);

	/**
	 * Called when a thread spawn by a user task has finished.
	 * 
	 * @param userThread
	 *        the user thread which has finished
	 */
	void userThreadFinished(Thread userThread);

	/**
	 * Returns whether the task has been canceled.
	 * 
	 * @return <code>true</code> if the task has been canceled, <code>false</code> otherwise
	 */
	boolean isCanceled();
}
