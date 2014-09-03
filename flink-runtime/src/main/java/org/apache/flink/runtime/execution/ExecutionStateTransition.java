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

import static org.apache.flink.runtime.execution.ExecutionState.CANCELED;
import static org.apache.flink.runtime.execution.ExecutionState.CANCELING;
import static org.apache.flink.runtime.execution.ExecutionState.FAILED;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

/**
 * This class is a utility class to check the consistency of Nephele's execution state model.
 * 
 */
public final class ExecutionStateTransition {

	/**
	 * The log object used for debugging.
	 */
	private static final Log LOG = LogFactory.getLog(ExecutionStateTransition.class);

	/**
	 * Private constructor to prevent instantiation of object.
	 */
	private ExecutionStateTransition() {
	}

	/**
	 * Checks the transition of the execution state and outputs an error in case of an unexpected state transition.
	 * 
	 * @param jobManager
	 *        <code>true</code> to indicate the method is called by the job manager,
	 *        <code>false/<code> to indicate it is called by a task manager
	 * @param taskName
	 *        the name of the task whose execution has changed
	 * @param oldState
	 *        the old execution state
	 * @param newState
	 *        the new execution state
	 */
	public static void checkTransition(boolean jobManager, String taskName, ExecutionState oldState, ExecutionState newState) {

		LOG.info((jobManager ? "JM: " : "TM: ") + "ExecutionState set from " + oldState + " to " + newState + " for task " + taskName);

		boolean unexpectedStateChange = true;

		// This is the regular life cycle of a task
		if (oldState == ExecutionState.CREATED && newState == ExecutionState.SCHEDULED) {
			unexpectedStateChange = false;
		}
		else if (oldState == ExecutionState.SCHEDULED && newState == ExecutionState.ASSIGNED) {
			unexpectedStateChange = false;
		}
		else if (oldState == ExecutionState.ASSIGNED && newState == ExecutionState.READY) {
			unexpectedStateChange = false;
		}
		else if (oldState == ExecutionState.READY && newState == ExecutionState.STARTING) {
			unexpectedStateChange = false;
		}
		else if (oldState == ExecutionState.STARTING && newState == ExecutionState.RUNNING) {
			unexpectedStateChange = false;
		}
		else if (oldState == ExecutionState.RUNNING && newState == ExecutionState.FINISHING) {
			unexpectedStateChange = false;
		}
		else if (oldState == ExecutionState.FINISHING && newState == ExecutionState.FINISHED) {
			unexpectedStateChange = false;
		}

		// A vertex might skip the SCHEDULED state if its resource has been allocated in a previous stage.
		else if (oldState == ExecutionState.CREATED && newState == ExecutionState.ASSIGNED) {
			unexpectedStateChange = false;
		}

		// This transition can appear if a task in a stage which is not yet executed gets canceled.
		else if (oldState == ExecutionState.SCHEDULED && newState == ExecutionState.CANCELING) {
			unexpectedStateChange = false;
		}

		// This transition can appear if a task in a stage which is not yet executed gets canceled.
		else if (oldState == ExecutionState.ASSIGNED && newState == ExecutionState.CANCELING) {
			unexpectedStateChange = false;
		}

		// This transition can appear if a task is canceled that is not yet running on the task manager.
		else if (oldState == ExecutionState.READY && newState == ExecutionState.CANCELING) {
			unexpectedStateChange = false;
		}

		// -------------- error cases --------------
		else if (newState == FAILED || newState == CANCELED || newState == CANCELING) {
			// any state may fail or cancel itself
			unexpectedStateChange = false;
		}

		if (unexpectedStateChange) {
			LOG.error("Unexpected state change: " + oldState + " -> " + newState);
		}
	}
}
