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

package eu.stratosphere.nephele.executiongraph;

import eu.stratosphere.nephele.execution.ExecutionState;

/**
 * Classes implementing the {@link GroupExecutionListener} interface can register for notifications about changes
 * to a group vertex's execution state.
 * 
 * @author warneke
 */
public interface GroupExecutionListener {

	/**
	 * Called when the execution state of the given {@link ExecutionGroupVertex} has changed.
	 * 
	 * @param groupVertex
	 *        the groupVertex whose execution state has changed
	 * @param newExecutionState
	 *        the new execution state of the group vertex
	 */
	void groupExecutionStateChanged(ExecutionGroupVertex groupVertex, ExecutionState newExecutionState,
			String optionalMessage);
}
