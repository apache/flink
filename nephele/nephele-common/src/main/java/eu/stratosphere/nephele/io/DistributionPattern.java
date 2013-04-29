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

package eu.stratosphere.nephele.io;

/**
 * A distribution pattern determines which subtasks of a producing Nephele task a wired to which
 * subtasks of a consuming subtask.
 * 
 * @author warneke
 */

public enum DistributionPattern {

	/**
	 * Each subtask of the producing Nepheke task is wired to each subtask of the consuming Nephele task.
	 */
	BIPARTITE,

	/**
	 * The i-th subtask of the producing Nephele task is wired to the i-th subtask of the consuming Nephele task.
	 */
	POINTWISE
}