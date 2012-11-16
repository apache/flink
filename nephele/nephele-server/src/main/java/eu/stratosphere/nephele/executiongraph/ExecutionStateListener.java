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

package eu.stratosphere.nephele.executiongraph;

import eu.stratosphere.nephele.execution.ExecutionState;
import eu.stratosphere.nephele.jobgraph.JobID;

/**
 * This interface must be implemented by classes which should be able to receive notifications about
 * changes of a task's execution state.
 * 
 * @author warneke
 */
public interface ExecutionStateListener extends PriorityListener {

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
}
