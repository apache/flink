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
 * This enumerations includes all possible states during a task's lifetime.
 * 
 * @author warneke
 */
public enum ExecutionState {

	/**
	 * The task has been created, but is not yet submitted to a scheduler.
	 */
	CREATED,

	/**
	 * The task has been accepted by the scheduler, the resource for the task has been requested
	 */
	SCHEDULED,

	/**
	 * The task has been assigned a resource to run, but is not yet read to by deployed.
	 */
	ASSIGNED,

	/**
	 * The task has been announced ready to run by the scheduler, but is not yet running.
	 */
	READY,

	/**
	 * The task is currently deployed to the assigned to task manager.
	 */
	STARTING,

	/**
	 * The task is currently running.
	 */
	RUNNING,

	/**
	 * The task has already finished, but not all of its results have been consumed yet.
	 */
	FINISHING,

	/**
	 * The task finished, all of its results have been consumed.
	 */
	FINISHED,

	/**
	 * The task has been requested to be canceled, but is not yet terminated.
	 */
	CANCELING,

	/**
	 * The task has been canceled due to a user request or the error of a connected task.
	 */
	CANCELED,

	/**
	 * The task has been aborted due to a failure during execution.
	 */
	FAILED
}
